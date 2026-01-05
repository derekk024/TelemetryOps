#include <httplib.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;

struct Thresholds {
    double latency_p95_ms = 200.0;
    double drop_rate = 0.05;
    double min_link_quality = 0.7;
    int window_s = 600;
};

static json eval_alerts(const json& metrics, const Thresholds& t) {
    json alerts = json::array();

    if (!metrics.contains("ok") || !metrics["ok"].is_boolean() || !metrics["ok"].get<bool>()) {
        alerts.push_back({{"severity","HIGH"},{"type","AGGREGATOR_ERROR"},{"message","metrics not ok"}});
        return alerts;
    }

    int count = metrics.value("count", 0);
    if (count == 0) {
        return alerts;
    }

    double p95 = metrics.value("latency_p95_ms", 0.0);
    double dr  = metrics.value("drop_rate", 0.0);
    double lq  = metrics.value("avg_link_quality", 0.0);

    if (p95 > t.latency_p95_ms) {
        alerts.push_back({{"severity","MED"},{"type","LATENCY_P95"},{"value",p95},{"threshold",t.latency_p95_ms}});
    }
    if (dr > t.drop_rate) {
        alerts.push_back({{"severity","HIGH"},{"type","DROP_RATE"},{"value",dr},{"threshold",t.drop_rate}});
    }
    if (lq < t.min_link_quality) {
        alerts.push_back({{"severity","MED"},{"type","LINK_QUALITY"},{"value",lq},{"threshold",t.min_link_quality}});
    }
    return alerts;
}

static std::atomic<long long> g_health{0}, g_ready{0}, g_config{0}, g_alerts{0}, g_prom{0}, g_watched{0};

static std::mutex g_alert_mu;
static std::unordered_map<std::string, long long> g_alert_type_counts;

static std::mutex g_state_mu;
static std::unordered_map<std::string, json> g_last_metrics_by_sat;
static std::unordered_map<std::string, json> g_last_alerts_by_sat;

static std::atomic<long long> g_poll_cycles{0};
static std::atomic<long long> g_poll_failures{0};

static std::string prom_metrics() {
    std::ostringstream out;
    out << "# TYPE http_requests_total counter\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/health\"} " << g_health.load() << "\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/ready\"} " << g_ready.load() << "\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/config\"} " << g_config.load() << "\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/alerts\"} " << g_alerts.load() << "\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/prom\"} " << g_prom.load() << "\n";
    out << "http_requests_total{service=\"controlplane\",route=\"/watched\"} " << g_watched.load() << "\n";

    out << "# TYPE alerts_total counter\n";
    {
        std::lock_guard<std::mutex> lock(g_alert_mu);
        for (auto& kv : g_alert_type_counts) {
            out << "alerts_total{type=\"" << kv.first << "\"} " << kv.second << "\n";
        }
    }

    out << "# TYPE poll_cycles_total counter\n";
    out << "poll_cycles_total " << g_poll_cycles.load() << "\n";
    out << "# TYPE poll_failures_total counter\n";
    out << "poll_failures_total " << g_poll_failures.load() << "\n";
    return out.str();
}

static std::int64_t now_ms() {
    return (std::int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8083;
    std::string aggregator_host = (argc > 2) ? argv[2] : std::string("localhost");
    int aggregator_port = (argc > 3) ? std::atoi(argv[3]) : 8082;

    Thresholds thresholds;
    std::mutex thresholds_mu;

    std::vector<std::string> watched = {"SAT-001","SAT-002","SAT-003","SAT-004","SAT-005"};
    std::mutex watched_mu;

    httplib::Client agg(aggregator_host, aggregator_port);
    agg.set_connection_timeout(2);
    agg.set_read_timeout(2);
    agg.set_write_timeout(2);

    std::atomic<bool> stop{false};

    std::thread poller([&](){
        while (!stop.load()) {
            g_poll_cycles++;

            Thresholds t;
            {
                std::lock_guard<std::mutex> lock(thresholds_mu);
                t = thresholds;
            }

            std::vector<std::string> sats;
            {
                std::lock_guard<std::mutex> lock(watched_mu);
                sats = watched;
            }

            for (const auto& sat_id : sats) {
                std::string path = "/metrics?sat_id=" + sat_id + "&window_s=" + std::to_string(t.window_s);

                auto r = agg.Get(path.c_str());
                if (!r || r->status != 200) {
                    g_poll_failures++;
                    continue;
                }

                json metrics;
                try {
                    metrics = json::parse(r->body);
                } catch (...) {
                    g_poll_failures++;
                    continue;
                }

                json alerts = eval_alerts(metrics, t);

                {
                    std::lock_guard<std::mutex> lock(g_state_mu);
                    g_last_metrics_by_sat[sat_id] = metrics;
                    g_last_alerts_by_sat[sat_id] = alerts;
                }

                {
                    std::lock_guard<std::mutex> lock(g_alert_mu);
                    for (auto& a : alerts) {
                        if (a.contains("type") && a["type"].is_string()) {
                            g_alert_type_counts[a["type"].get<std::string>()]++;
                        }
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    });

    httplib::Server svr;

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        g_health++;
        res.set_content(R"({"ok":true})", "application/json");
    });

    svr.Get("/ready", [&agg](const httplib::Request&, httplib::Response& res) {
        g_ready++;
        auto r = agg.Get("/health");
        if (!r || r->status != 200) {
            res.status = 503;
            res.set_content(R"({"ok":false,"error":"aggregator unreachable"})", "application/json");
            return;
        }
        res.set_content(R"({"ok":true})", "application/json");
    });

    svr.Get("/prom", [](const httplib::Request&, httplib::Response& res) {
        g_prom++;
        res.set_content(prom_metrics(), "text/plain; version=0.0.4");
    });

    svr.Post("/config", [&thresholds_mu, &thresholds](const httplib::Request& req, httplib::Response& res) {
        g_config++;
        try {
            auto j = json::parse(req.body);
            std::lock_guard<std::mutex> lock(thresholds_mu);

            if (j.contains("latency_p95_ms")) thresholds.latency_p95_ms = j["latency_p95_ms"].get<double>();
            if (j.contains("drop_rate")) thresholds.drop_rate = j["drop_rate"].get<double>();
            if (j.contains("min_link_quality")) thresholds.min_link_quality = j["min_link_quality"].get<double>();
            if (j.contains("window_s")) thresholds.window_s = j["window_s"].get<int>();

            res.set_content(json{{"ok",true},{"thresholds",{
                {"latency_p95_ms",thresholds.latency_p95_ms},
                {"drop_rate",thresholds.drop_rate},
                {"min_link_quality",thresholds.min_link_quality},
                {"window_s",thresholds.window_s}
            }}}.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(json{{"ok",false},{"error",std::string("invalid json: ")+e.what()}}.dump(), "application/json");
        }
    });

    svr.Post("/watched", [&watched_mu, &watched](const httplib::Request& req, httplib::Response& res) {
        g_watched++;
        try {
            auto j = json::parse(req.body);
            if (!j.contains("sats") || !j["sats"].is_array()) {
                res.status = 400;
                res.set_content(R"({"ok":false,"error":"expected {\"sats\":[...]}"} )", "application/json");
                return;
            }
            std::vector<std::string> next;
            for (auto& x : j["sats"]) {
                if (x.is_string()) next.push_back(x.get<std::string>());
            }
            if (next.empty()) {
                res.status = 400;
                res.set_content(R"({"ok":false,"error":"sats must be non-empty"} )", "application/json");
                return;
            }
            {
                std::lock_guard<std::mutex> lock(watched_mu);
                watched = std::move(next);
            }
            res.set_content(R"({"ok":true})", "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(json{{"ok",false},{"error",std::string("invalid json: ")+e.what()}}.dump(), "application/json");
        }
    });

    svr.Get("/watched", [&watched_mu, &watched](const httplib::Request&, httplib::Response& res) {
        g_watched++;
        std::vector<std::string> sats;
        {
            std::lock_guard<std::mutex> lock(watched_mu);
            sats = watched;
        }
        res.set_content(json{{"ok",true},{"sats",sats}}.dump(), "application/json");
    });

    svr.Get("/alerts", [&thresholds_mu, &thresholds](const httplib::Request& req, httplib::Response& res) {
        g_alerts++;
        if (!req.has_param("sat_id")) {
            res.status = 400;
            res.set_content(R"({"ok":false,"error":"missing sat_id"})", "application/json");
            return;
        }
        std::string sat_id = req.get_param_value("sat_id");

        Thresholds t;
        {
            std::lock_guard<std::mutex> lock(thresholds_mu);
            t = thresholds;
        }

        json metrics = json{{"ok",false},{"error","no data yet"}};
        json alerts = json::array();
        {
            std::lock_guard<std::mutex> lock(g_state_mu);
            if (g_last_metrics_by_sat.count(sat_id)) metrics = g_last_metrics_by_sat[sat_id];
            if (g_last_alerts_by_sat.count(sat_id)) alerts = g_last_alerts_by_sat[sat_id];
        }

        json out = {
            {"ok", true},
            {"sat_id", sat_id},
            {"metrics", metrics},
            {"alerts", alerts},
            {"thresholds", {
                {"latency_p95_ms",t.latency_p95_ms},
                {"drop_rate",t.drop_rate},
                {"min_link_quality",t.min_link_quality},
                {"window_s",t.window_s}
            }},
            {"poll", {{"cycles", g_poll_cycles.load()}, {"failures", g_poll_failures.load()}, {"now_ms", now_ms()}}}
        };

        res.set_content(out.dump(), "application/json");
    });

    spdlog::info("controlplane listening on {} -> aggregator {}:{}", port, aggregator_host, aggregator_port);
    svr.listen("0.0.0.0", port);

    stop.store(true);
    if (poller.joinable()) poller.join();
    return 0;
}
