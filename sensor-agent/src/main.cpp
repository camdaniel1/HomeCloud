// sensor-agent/src/main.cpp
//
// Runs on the Raspberry Pi. Reads sensor values (real GPIO/I2C driver can be
// swapped in behind SensorSource; a simple simulator is provided for
// dev/test), batches them, and ships them to the ingestion-service over TCP.
//
// Fault tolerance: if the ingestion-service or network is unreachable, readings
// are appended to a local write-ahead log on disk (/var/lib/sensor-agent/wal)
// and re-sent on the next successful connection, so a Pi reboot or a transient
// Kubernetes ingress outage never loses a reading.

#include "../../common/protocol.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <filesystem>
#include <cerrno>
#include <fcntl.h>

namespace {

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running = false; }

struct Config {
    std::string device_id   = "rpi-sensor-01";
    std::string server_host = "ingestion-service";
    int         server_port = 9000;
    int         interval_ms = 1000;   // ~1 reading/sec -> ~86,400/day per device
    std::string wal_path    = "/var/lib/sensor-agent/wal.log";
    int         batch_size  = 20;
};

Config load_config_from_env() {
    Config c;
    if (const char *v = std::getenv("DEVICE_ID"))    c.device_id   = v;
    if (const char *v = std::getenv("SERVER_HOST"))   c.server_host = v;
    if (const char *v = std::getenv("SERVER_PORT"))   c.server_port = std::atoi(v);
    if (const char *v = std::getenv("INTERVAL_MS"))   c.interval_ms = std::atoi(v);
    if (const char *v = std::getenv("WAL_PATH"))      c.wal_path    = v;
    if (const char *v = std::getenv("BATCH_SIZE"))    c.batch_size  = std::atoi(v);
    if (c.server_port < 1 || c.server_port > 65535) c.server_port = 9000;
    if (c.interval_ms < 1) c.interval_ms = 1000;
    if (c.batch_size < 1 || c.batch_size > 10000) c.batch_size = 20;
    return c;
}

// ---- Simulated sensor source (swap for real GPIO/I2C reads on hardware) ----
class SensorSource {
public:
    SensorSource() : rng_(std::random_device{}()) {}

    iot::SensorReading read(const std::string &device_id, uint32_t seq) {
        iot::SensorReading r;
        r.set_device_id(device_id);
        r.sensor_type = static_cast<uint8_t>(seq % 5); // cycle through sensor types
        r.value = value_for(static_cast<iot::SensorType>(r.sensor_type));
        r.epoch_millis = iot::now_millis();
        r.sequence_no = seq;
        iot::sign(r);
        return r;
    }

private:
    double value_for(iot::SensorType t) {
        std::uniform_real_distribution<double> temp(18.0, 27.0);
        std::uniform_real_distribution<double> hum(30.0, 70.0);
        std::uniform_real_distribution<double> pres(990.0, 1025.0);
        std::uniform_real_distribution<double> vib(0.0, 2.5);
        std::uniform_real_distribution<double> light(0.0, 1000.0);
        switch (t) {
            case iot::SensorType::TEMPERATURE: return temp(rng_);
            case iot::SensorType::HUMIDITY:    return hum(rng_);
            case iot::SensorType::PRESSURE:    return pres(rng_);
            case iot::SensorType::VIBRATION:   return vib(rng_);
            case iot::SensorType::LIGHT:       return light(rng_);
        }
        return 0.0;
    }
    std::mt19937 rng_;
};

// ---- Write-ahead log for fault tolerance during network/server outages ----
class WriteAheadLog {
public:
    explicit WriteAheadLog(std::string path) : path_(std::move(path)) {
        std::error_code ec;
        auto parent = std::filesystem::path(path_).parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
    }

    bool append(const iot::SensorReading &r) {
        int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) return false;
        const char *data = reinterpret_cast<const char *>(&r);
        size_t offset = 0;
        bool ok = true;
        while (offset < sizeof(r)) {
            ssize_t written = ::write(fd, data + offset, sizeof(r) - offset);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) { ok = false; break; }
            offset += static_cast<size_t>(written);
        }
        if (ok) ok = (::fsync(fd) == 0);
        if (::close(fd) != 0) ok = false;
        return ok;
    }

    // Load without truncating. The caller clears only after the server ACKs,
    // so a crash or power loss during replay cannot erase unsent readings.
    std::vector<iot::SensorReading> load() const {
        std::vector<iot::SensorReading> out;
        std::ifstream in(path_, std::ios::binary);
        if (!in) return out;
        iot::SensorReading r;
        while (in.read(reinterpret_cast<char *>(&r), sizeof(r))) {
            if (iot::verify(r)) out.push_back(r);
        }
        return out;
    }

    bool clear() {
        std::ofstream trunc(path_, std::ios::binary | std::ios::trunc);
        return trunc.good();
    }

    size_t pending_count() const {
        std::ifstream in(path_, std::ios::binary | std::ios::ate);
        if (!in) return 0;
        return static_cast<size_t>(in.tellg()) / sizeof(iot::SensorReading);
    }

private:
    std::string path_;
};

// ---- TCP client with retry/backoff ----
class IngestionClient {
public:
    IngestionClient(std::string host, int port) : host_(std::move(host)), port_(port) {}

    // Sends a batch; returns true if the whole batch was accepted.
    bool send_batch(const std::vector<iot::SensorReading> &batch) {
        if (batch.empty() || batch.size() > 10000) return false;
        int fd = connect_socket();
        if (fd < 0) return false;

        uint32_t count = htonl(static_cast<uint32_t>(batch.size()));
        bool ok = send_all(fd, &count, sizeof(count));
        for (const auto &r : batch) {
            if (ok && !send_all(fd, &r, sizeof(r))) { ok = false; break; }
        }
        // Wait for a 1-byte ACK ('K') so we know the server durably queued the batch.
        if (ok) {
            char ack = 0;
            ssize_t n = ::recv(fd, &ack, 1, 0);
            ok = (n == 1 && ack == 'K');
        }
        ::close(fd);
        return ok;
    }

private:
    static bool send_all(int fd, const void *data, size_t size) {
        const char *cursor = static_cast<const char *>(data);
        while (size > 0) {
            ssize_t sent = ::send(fd, cursor, size, 0);
            if (sent <= 0) return false;
            cursor += sent;
            size -= static_cast<size_t>(sent);
        }
        return true;
    }

    int connect_socket() {
        struct addrinfo hints{}, *res = nullptr;
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        std::string port_str = std::to_string(port_);
        if (getaddrinfo(host_.c_str(), port_str.c_str(), &hints, &res) != 0) return -1;

        int fd = -1;
        for (auto *p = res; p != nullptr; p = p->ai_next) {
            fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (fd < 0) continue;
            struct timeval tv{2, 0};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
            ::close(fd);
            fd = -1;
        }
        freeaddrinfo(res);
        return fd;
    }

    std::string host_;
    int port_;
};

} // namespace

int main() {
    // Unbuffer stdout so log lines appear immediately under `docker logs` /
    // `kubectl logs` instead of sitting in a full buffer until it fills.
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    Config cfg = load_config_from_env();
    SensorSource source;
    WriteAheadLog wal(cfg.wal_path);
    IngestionClient client(cfg.server_host, cfg.server_port);

    std::cout << "[sensor-agent] device=" << cfg.device_id
              << " target=" << cfg.server_host << ":" << cfg.server_port
              << " interval_ms=" << cfg.interval_ms << "\n";

    uint32_t seq = 0;
    std::vector<iot::SensorReading> batch;
    int backoff_ms = 500;
    const int max_backoff_ms = 30000;

    while (g_running) {
        // 1. Always replay anything stuck in the WAL from a prior outage first.
        auto backlog = wal.load();
        if (!backlog.empty()) {
            std::cout << "[sensor-agent] replaying " << backlog.size()
                      << " buffered readings from WAL\n";
            if (!client.send_batch(backlog)) {
                // The WAL remains untouched and will be retried.
            } else {
                if (!wal.clear()) {
                    std::cerr << "[sensor-agent] failed to clear acknowledged WAL\n";
                }
                backoff_ms = 500; // success resets backoff
            }
        }

        // 2. Collect a fresh batch.
        batch.clear();
        for (int i = 0; i < cfg.batch_size && g_running; i++) {
            batch.push_back(source.read(cfg.device_id, seq++));
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg.interval_ms));
        }
        if (batch.empty()) continue;

        // 3. Try to send; on failure, persist to WAL and back off exponentially
        //    so a downed ingestion-service pod doesn't get hammered.
        if (client.send_batch(batch)) {
            backoff_ms = 500;
        } else {
            std::cerr << "[sensor-agent] send failed, buffering "
                      << batch.size() << " readings to WAL (" << cfg.wal_path << ")\n";
            for (auto &r : batch) {
                if (!wal.append(r)) {
                    std::cerr << "[sensor-agent] failed to persist reading to WAL\n";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);
        }
    }

    std::cout << "[sensor-agent] shutting down, " << wal.pending_count()
              << " readings still queued in WAL\n";
    return 0;
}
