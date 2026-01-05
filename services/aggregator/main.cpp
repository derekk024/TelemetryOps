#include <httplib.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sqlite3.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using json = nlohmann::json;

struct SqliteRO {
    sqlite3* db = nullptr;

    explicit SqliteRO(const std::string& path) {
        if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
            std::string msg = db ? sqlite3_errmsg(db) : "unknown";
            if (db) sqlite3_close(db);
            db = nullptr;
            throw std::runtime_error("sqlite open (readonly) failed: " + msg);
        }

        sqlite3_busy_timeout(db, 5000);
        exec("PRAGMA busy_timeout=5000;");
    }

    ~SqliteRO() { if (db) sqlite3_close(db); }

    void exec(const std::string& sql) {
        char* err = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error("sqlite exec failed: " + msg);
        }
    }

    struct Row {
        double latency_ms;
        int dropped;
        int sent;
        double link_quality;
    };

    std::vector<Row> select_rows(const std::string& sat_id, std::int64_t min_ts_ms) {
        const char* sql =
            "SELECT latency_ms, dropped_packets, sent_packets, link_quality "
            "FROM telemetry WHERE sat_id = ? AND ts_ms >= ?;";

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("sqlite prepare failed: ") + sqlite3_errmsg(db));
        }

        sqlite3_bind_text(stmt, 1, sat_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, min_ts_ms);

        std::vector<Row> out;
        while (true) {
            int rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                Row r;
                r.latency_ms = sqlite3_column_double(stmt, 0);
                r.dropped = sqlite3_column_int(stmt, 1);
                r.sent = sqlite3_column_int(stmt, 2);
                r.link_quality = sqlite3_column_double(stmt, 3);
                out.push_back(r);
            } else if (rc == SQLITE_DONE) {
                break;
            } else {
                sqlite3_finalize(stmt);
                throw std::runtime_error(std::string("sqlite step failed: ") + sqlite3_errmsg(db));
            }
        }
        sqlite3_finalize(stmt);
        return out;
    }
};

static double percentile(std::vector<double> v, double p) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    double idx = (p / 100.0) * (v.size() - 1);
    std::size_t i = static_cast<std::size_t>(idx);
    double frac = idx - static_cast<double>(i);
    if (i + 1 < v.size()) return v[i] * (1.0 - frac) + v[i + 1] * frac;
    return v[i];
}

static std::atomic<long long> g_health{0}, g_ready{0}, g_prom{0}, g_query{0};

static std::string prom_metrics() {
    std::ostringstream out;
    out << "# TYPE http_requests_total counter\n";
    out << "http_requests_total{service=\"aggregator\",route=\"/health\"} " << g_health.load() << "\n";
    out << "http_requests_total{service=\"aggregator\",route=\"/ready\"} " << g_ready.load() << "\n";
    out << "http_requests_total{service=\"aggregator\",route=\"/prom\"} " << g_prom.load() << "\n";
    out << "http_requests_total{service=\"aggregator\",route=\"/metrics\"} " << g_query.load() << "\n";
    return out.str();
}

int main(int argc, char** argv) {
    try {
        int port = (argc > 1) ? std::atoi(argv[1]) : 8082;
        std::string db_path = (argc > 2) ? argv[2] : std::string("data/telemetry.db");

        SqliteRO db(db_path);
        httplib::Server svr;

        svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
            g_health++;
            res.set_content(R"({"ok":true})", "application/json");
        });

        svr.Get("/ready", [](const httplib::Request&, httplib::Response& res) {
            g_ready++;
            res.set_content(R"({"ok":true})", "application/json");
        });

        svr.Get("/prom", [](const httplib::Request&, httplib::Response& res) {
            g_prom++;
            res.set_content(prom_metrics(), "text/plain; version=0.0.4");
        });

        svr.Get("/metrics", [&db](const httplib::Request& req, httplib::Response& res) {
            g_query++;
            if (!req.has_param("sat_id")) {
                res.status = 400;
                res.set_content(R"({"ok":false,"error":"missing sat_id"})", "application/json");
                return;
            }
            std::string sat_id = req.get_param_value("sat_id");
            int window_s = 600;
            if (req.has_param("window_s")) window_s = std::max(1, std::atoi(req.get_param_value("window_s").c_str()));

            std::int64_t now_ms = static_cast<std::int64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count()
            );
            std::int64_t min_ts = now_ms - static_cast<std::int64_t>(window_s) * 1000;

            try {
                auto rows = db.select_rows(sat_id, min_ts);

                long long sum_dropped = 0;
                long long sum_sent = 0;
                double sum_lq = 0.0;
                std::vector<double> lat;
                lat.reserve(rows.size());

                for (auto& r : rows) {
                    sum_dropped += r.dropped;
                    sum_sent += r.sent;
                    sum_lq += r.link_quality;
                    lat.push_back(r.latency_ms);
                }

                double drop_rate = (sum_sent > 0) ? (double)sum_dropped / (double)sum_sent : 0.0;
                double avg_lq = (!rows.empty()) ? sum_lq / (double)rows.size() : 0.0;

                json out = {
                    {"ok", true},
                    {"sat_id", sat_id},
                    {"window_s", window_s},
                    {"count", (int)rows.size()},
                    {"drop_rate", drop_rate},
                    {"latency_p50_ms", percentile(lat, 50.0)},
                    {"latency_p95_ms", percentile(lat, 95.0)},
                    {"avg_link_quality", avg_lq}
                };

                res.set_content(out.dump(), "application/json");
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(json{{"ok",false},{"error",e.what()}}.dump(), "application/json");
            }
        });

        spdlog::info("aggregator listening on {} db={}", port, db_path);
        svr.listen("0.0.0.0", port);
        return 0;
    } catch (const std::exception& e) {
        spdlog::error("aggregator fatal: {}", e.what());
        return 1;
    }
}