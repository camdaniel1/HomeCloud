// dashboard-api/src/main.cpp
//
// Minimal HTTP server exposing read endpoints backed by the durable ingest
// journal on the shared RAID volume. A short-TTL in-memory cache keeps
// dashboard queries sub-second even while ingestion is writing 50k+
// rows/day, by absorbing repeated identical queries between polling
// intervals instead of round-tripping to SQL Server every time.
//
// Endpoints:
//   GET /health
//   GET /api/readings/latest?device_id=...&limit=50
//   GET /api/readings/summary?device_id=...&sensor_type=temperature&window_minutes=60

#include "../../common/protocol.hpp"
#include "../../ingestion-service/src/database.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <iomanip>

namespace {

struct CacheEntry {
    std::string body;
    std::chrono::steady_clock::time_point expires_at;
};

class ResponseCache {
public:
    explicit ResponseCache(int ttl_ms) : ttl_ms_(ttl_ms) {}

    bool get(const std::string &key, std::string &out) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = cache_.find(key);
        if (it == cache_.end()) return false;
        if (std::chrono::steady_clock::now() > it->second.expires_at) {
            cache_.erase(it);
            return false;
        }
        out = it->second.body;
        return true;
    }

    void put(const std::string &key, const std::string &body) {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_[key] = {body, std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms_)};
    }

private:
    int ttl_ms_;
    std::mutex mutex_;
    std::map<std::string, CacheEntry> cache_;
};

// Reads the durable RAID-backed journal written before ingest is acknowledged.
// This keeps the API available during SQL Server outages as well as locally.
struct ReadingRow {
    std::string device_id;
    std::string sensor_type;
    double value = 0;
    int64_t epoch_millis = 0;
};

std::string json_escape(const std::string &value) {
    std::ostringstream out;
    for (unsigned char c : value) {
        switch (c) {
            case '\"': out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\b': out << "\\b"; break;
            case '\f': out << "\\f"; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default:
                if (c < 0x20) out << "\\u" << std::hex << std::setw(4)
                                  << std::setfill('0') << static_cast<int>(c) << std::dec;
                else out << static_cast<char>(c);
        }
    }
    return out.str();
}

bool parse_row(const std::string &line, ReadingRow &result) {
    std::istringstream row(line);
    std::string value, timestamp, sequence;
    if (!std::getline(row, result.device_id, '\t') ||
        !std::getline(row, result.sensor_type, '\t') ||
        !std::getline(row, value, '\t') ||
        !std::getline(row, timestamp, '\t') ||
        !std::getline(row, sequence, '\t')) return false;
    try {
        size_t used = 0;
        result.value = std::stod(value, &used);
        if (used != value.size() || !std::isfinite(result.value)) return false;
        result.epoch_millis = std::stoll(timestamp, &used);
        return used == timestamp.size();
    } catch (...) {
        return false;
    }
}

std::vector<ReadingRow> read_rows(const std::string &data_dir) {
    std::ifstream in(data_dir + "/readings.tsv");
    std::vector<ReadingRow> rows;
    std::string line;
    while (std::getline(in, line)) {
        ReadingRow row;
        if (parse_row(line, row)) rows.push_back(std::move(row));
    }
    return rows;
}

std::string query_latest(const std::string &data_dir, const std::string &device_filter, int limit) {
    auto rows = read_rows(data_dir);
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const ReadingRow &row) {
        return !device_filter.empty() && row.device_id != device_filter;
    }), rows.end());
    std::ostringstream json;
    json << "{\"readings\":[";
    size_t start = rows.size() > static_cast<size_t>(limit) ? rows.size() - limit : 0;
    for (size_t i = start; i < rows.size(); i++) {
        if (i > start) json << ",";
        json << "{\"device_id\":\"" << json_escape(rows[i].device_id) << "\","
             << "\"sensor_type\":\"" << json_escape(rows[i].sensor_type) << "\","
             << "\"value\":" << rows[i].value << ","
             << "\"epoch_millis\":" << rows[i].epoch_millis << "}";
    }
    json << "]}";
    return json.str();
}

std::string query_summary(const std::string &data_dir, const std::string &device_filter,
                          const std::string &type_filter, int window_minutes) {
    auto rows = read_rows(data_dir);
    int64_t cutoff = iot::now_millis() - static_cast<int64_t>(window_minutes) * 60000;
    size_t count = 0;
    double sum = 0, minimum = 0, maximum = 0;
    for (const auto &row : rows) {
        if ((!device_filter.empty() && row.device_id != device_filter) ||
            (!type_filter.empty() && row.sensor_type != type_filter) || row.epoch_millis < cutoff) continue;
        if (count++ == 0) minimum = maximum = row.value;
        else { minimum = std::min(minimum, row.value); maximum = std::max(maximum, row.value); }
        sum += row.value;
    }
    std::ostringstream json;
    json << "{\"count\":" << count << ",\"window_minutes\":" << window_minutes;
    if (count) json << ",\"average\":" << (sum / count) << ",\"minimum\":" << minimum
                    << ",\"maximum\":" << maximum;
    else json << ",\"average\":null,\"minimum\":null,\"maximum\":null";
    json << "}";
    return json.str();
}

std::string http_response(const std::string &body, const std::string &status = "200 OK") {
    std::ostringstream resp;
    resp << "HTTP/1.1 " << status << "\r\n"
         << "Content-Type: application/json\r\n"
         << "Content-Length: " << body.size() << "\r\n"
         << "Connection: close\r\n\r\n"
         << body;
    return resp.str();
}

std::map<std::string, std::string> parse_query(const std::string &path) {
    std::map<std::string, std::string> params;
    auto qpos = path.find('?');
    if (qpos == std::string::npos) return params;
    std::istringstream qs(path.substr(qpos + 1));
    std::string kv;
    while (std::getline(qs, kv, '&')) {
        auto eq = kv.find('=');
        if (eq != std::string::npos) params[kv.substr(0, eq)] = kv.substr(eq + 1);
    }
    return params;
}

int bounded_int(const std::map<std::string, std::string> &params, const std::string &name,
                int fallback, int minimum, int maximum) {
    auto it = params.find(name);
    if (it == params.end()) return fallback;
    try {
        size_t used = 0;
        long value = std::stol(it->second, &used);
        if (used != it->second.size() || value < minimum || value > maximum) return fallback;
        return static_cast<int>(value);
    } catch (...) { return fallback; }
}

bool send_all(int fd, const std::string &data) {
    size_t offset = 0;
    while (offset < data.size()) {
        ssize_t sent = ::send(fd, data.data() + offset, data.size() - offset, 0);
        if (sent < 0 && errno == EINTR) continue;
        if (sent <= 0) return false;
        offset += static_cast<size_t>(sent);
    }
    return true;
}

} // namespace

int main() {
    // Unbuffer stdout so log lines appear immediately under `docker logs` /
    // `kubectl logs` instead of sitting in a full buffer until it fills.
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    int port = 8080;
    if (const char *v = std::getenv("LISTEN_PORT")) port = std::atoi(v);
    if (port < 1 || port > 65535) {
        std::cerr << "[dashboard-api] invalid LISTEN_PORT\n";
        return 1;
    }
    std::string raid_dir = "/data/ingest";
    if (const char *v = std::getenv("RAID_MOUNT_PATH")) raid_dir = v;
    int cache_ttl_ms = 2000; // short TTL keeps dashboards fresh but absorbs polling bursts
    if (const char *v = std::getenv("CACHE_TTL_MS")) cache_ttl_ms = std::atoi(v);
    if (cache_ttl_ms < 0) cache_ttl_ms = 2000;

    ResponseCache cache(cache_ttl_ms);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "[dashboard-api] socket creation failed\n";
        return 1;
    }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (::bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0 ||
        ::listen(server_fd, 128) < 0) {
        std::cerr << "[dashboard-api] failed to listen on port " << port << "\n";
        ::close(server_fd);
        return 1;
    }
    std::cout << "[dashboard-api] listening on :" << port << " cache_ttl_ms=" << cache_ttl_ms << "\n";

    while (true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &len);
        if (client_fd < 0) continue;

        char buf[4096] = {0};
        ::recv(client_fd, buf, sizeof(buf) - 1, 0);
        std::istringstream request(buf);
        std::string method, path, version;
        request >> method >> path >> version;

        std::string body;
        std::string status = "200 OK";

        if (method != "GET") {
            status = "405 Method Not Allowed";
            body = "{\"error\":\"method not allowed\"}";
        } else if (path == "/health") {
            body = "{\"status\":\"ok\"}";
        } else if (path.rfind("/api/readings/latest", 0) == 0) {
            auto params = parse_query(path);
            std::string device_id = params.count("device_id") ? params["device_id"] : "";
            int limit = bounded_int(params, "limit", 50, 1, 1000);

            std::string cache_key = "latest:" + device_id + ":" + std::to_string(limit);
            if (!cache.get(cache_key, body)) {
                body = query_latest(raid_dir, device_id, limit);
                cache.put(cache_key, body);
            }
        } else if (path.rfind("/api/readings/summary", 0) == 0) {
            auto params = parse_query(path);
            std::string device_id = params.count("device_id") ? params["device_id"] : "";
            std::string sensor_type = params.count("sensor_type") ? params["sensor_type"] : "";
            int window = bounded_int(params, "window_minutes", 60, 1, 525600);
            std::string cache_key = "summary:" + device_id + ":" + sensor_type + ":" + std::to_string(window);
            if (!cache.get(cache_key, body)) {
                body = query_summary(raid_dir, device_id, sensor_type, window);
                cache.put(cache_key, body);
            }
        } else {
            status = "404 Not Found";
            body = "{\"error\":\"not found\"}";
        }

        std::string resp = http_response(body, status);
        send_all(client_fd, resp);
        ::close(client_fd);
    }
    return 0;
}
