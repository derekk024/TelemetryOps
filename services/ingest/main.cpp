#include <httplib.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>

#include <atomic>
#include <sstream>

using json = nlohmann::json;

struct Sqlite {
    sqlite3* db = nullptr;

    explicit Sqlite(const std::string& path) {
        if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
            std::string msg = db ? sqlite3_errmsg(db) : "unknown";
            if (db) sqlite3_close(db);
            db = nullptr;
            throw std::runtime_error("sqlite open failed: " + msg);
        }

        sqlite3_busy_timeout(db, 5000);

        exec("PRAGMA busy_timeout=5000;");
        exec("PRAGMA journal_mode=WAL;");
        exec(R"sql(
            CREATE TABLE IF NOT EXISTS telemetry (
                event_id TEXT PRIMARY KEY,
                sat_id TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                latency_ms REAL NOT NULL,
                dropped_packets INTEGER NOT NULL,
                sent_packets INTEGER NOT NULL,
                link_quality REAL NOT NULL
            );
        )sql");
        exec("CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(ts_ms);");
        exec("CREATE INDEX IF NOT EXISTS idx_telemetry_sat ON telemetry(sat_id);");
    }

    ~Sqlite() { if (db) sqlite3_close(db); }

    void exec(const std::string& sql) {
        char* err = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error("sqlite exec failed: " + msg);
        }
    }

    bool insert_event(const json& j) {
        const char* sql =
            "INSERT OR IGNORE INTO telemetry(event_id,sat_id,ts_ms,latency_ms,dropped_packets,sent_packets,link_quality) "
            "VALUES(?,?,?,?,?,?,?);";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("sqlite prepare failed: ") + sqlite3_errmsg(db));
        }

        sqlite3_bind_text(stmt, 1, j["event_id"].get<std::string>().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, j["sat_id"].get<std::string>().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, j["ts_ms"].get<std::int64_t>());
        sqlite3_bind_double(stmt, 4, j["latency_ms"].get<double>());
        sqlite3_bind_int(stmt, 5, j["dropped_packets"].get<int>());
        sqlite3_bind_int(stmt, 6, j["sent_packets"].get<int>());
        sqlite3_bind_double(stmt, 7, j["link_quality"].get<double>());

        int rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) throw std::runtime_error(std::string("sqlite step failed: ") + sqlite3_errmsg(db));

        return sqlite3_changes(db) > 0;
    }
};

static bool validate_event(const json& j, std::string& err) {
    const char* req[] = {"event_id","sat_id","ts_ms","latency_ms","dropped_packets","sent_packets","link_quality"};
    for (auto k : req) if (!j.contains(k)) { err = std::string("missing field: ") + k; return false; }

    if (!j["event_id"].is_string() || j["event_id"].get<std::string>().empty()) { err = "event_id invalid"; return false; }
    if (!j["sat_id"].is_string()   || j["sat_id"].get<std::string>().empty())   { err = "sat_id invalid"; return false; }
    if (!j["ts_ms"].is_number_integer()) { err = "ts_ms must be int64"; return false; }
    if (!j["latency_ms"].is_number())    { err = "latency_ms must be number"; return false; }
    if (!j["dropped_packets"].is_number_integer()) { err = "dropped_packets must be int"; return false; }
    if (!j["sent_packets"].is_number_integer())    { err = "sent_packets must be int"; return false; }

    int sent = j["sent_packets"].get<int>();
    int dropped = j["dropped_packets"].get<int>();
    if (sent <= 0) { err = "sent_packets must be > 0"; return false; }
    if (dropped < 0 || dropped > sent) { err = "dropped_packets must be in [0,sent_packets]"; return false; }

    double lq = j["link_quality"].get<double>();
    if (lq < 0.0 || lq > 1.0) { err = "link_quality out of range [0,1]"; return false; }

    return true;
}

static std::atomic<long long> g_inserted{0};
static std::atomic<long long> g_duplicates{0};
static std::atomic<long long> g_health{0}, g_ready{0}, g_telemetry{0}, g_metrics{0};

static std::string prometheus_metrics() {
    std::ostringstream out;
    out << "# TYPE telemetry_inserted_total counter\n";
    out << "telemetry_inserted_total " << g_inserted.load() << "\n";
    out << "# TYPE telemetry_duplicates_total counter\n";
    out << "telemetry_duplicates_total " << g_duplicates.load() << "\n";
    out << "# TYPE http_requests_total counter\n";
    out << "http_requests_total{service=\"ingest\",route=\"/health\"} " << g_health.load() << "\n";
    out << "http_requests_total{service=\"ingest\",route=\"/ready\"} " << g_ready.load() << "\n";
    out << "http_requests_total{service=\"ingest\",route=\"/telemetry\"} " << g_telemetry.load() << "\n";
    out << "http_requests_total{service=\"ingest\",route=\"/metrics\"} " << g_metrics.load() << "\n";
    return out.str();
}

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8081;
    std::string db_path = (argc > 2) ? argv[2] : std::string("data/telemetry.db");

    Sqlite db(db_path);
    httplib::Server svr;

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        g_health++;
        res.set_content(R"({"ok":true})", "application/json");
    });

    svr.Get("/ready", [&db](const httplib::Request&, httplib::Response& res) {
        (void)db;
        g_ready++;
        res.set_content(R"({"ok":true})", "application/json");
    });

    svr.Get("/metrics", [](const httplib::Request&, httplib::Response& res) {
        g_metrics++;
        res.set_content(prometheus_metrics(), "text/plain; version=0.0.4");
    });

    svr.Post("/telemetry", [&db](const httplib::Request& req, httplib::Response& res) {
        g_telemetry++;
        try {
            auto j = json::parse(req.body);
            std::string err;
            if (!validate_event(j, err)) {
                res.status = 400;
                res.set_content(json{{"ok",false},{"error",err}}.dump(), "application/json");
                return;
            }

            bool inserted = db.insert_event(j);
            if (inserted) g_inserted++; else g_duplicates++;

            spdlog::info("accepted event_id={} sat_id={} inserted={}",
                         j["event_id"].get<std::string>(),
                         j["sat_id"].get<std::string>(),
                         inserted);

            res.status = 202;
            res.set_content(json{{"ok",true},{"inserted",inserted}}.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(json{{"ok",false},{"error",std::string("error: ")+e.what()}}.dump(), "application/json");
        }
    });

    spdlog::info("ingest listening on {} db={}", port, db_path);
    svr.listen("0.0.0.0", port);
    return 0;
}