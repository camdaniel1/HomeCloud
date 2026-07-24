// ingestion-service/src/main.cpp
//
// Runs as a Kubernetes Deployment. Accepts TCP connections from one or more
// sensor-agent processes, reads batches of SensorReading structs, durably
// writes them to the RAID-backed PersistentVolume + SQL Server, and ACKs
// with a single 'K' byte. If persistence fails, no ACK is sent so the
// sensor-agent retries (at-least-once delivery).
//
// A single background writer retries SQL Server writes. Client connections
// are handled one at a time, which keeps the control flow easy to follow.

#include "../../common/protocol.hpp"
#include "database.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <cerrno>

namespace {

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running = false; }

// Thread-safe queue of batches awaiting a durable DB write, so a slow/flaky
// SQL Server never blocks a client connection from being ACKed to the
// RAID-backed on-disk buffer first (write-then-ack, ack-then-db-async).
class RetryQueue {
public:
    void push(std::vector<iot::SensorReading> batch) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(std::move(batch));
        cv_.notify_one();
    }

    bool pop(std::vector<iot::SensorReading> &out) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(500),
                     [&] { return !queue_.empty() || !g_running; });
        if (queue_.empty()) return false;
        out = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::vector<iot::SensorReading>> queue_;
};

void db_writer_thread(RetryQueue &queue, iot::Database &db) {
    while (g_running || queue.size() > 0) {
        std::vector<iot::SensorReading> batch;
        if (!queue.pop(batch)) continue;

        int attempts = 0;
        while (!db.insert_batch(batch)) {
            if (!g_running) break;
            attempts++;
            std::cerr << "[ingestion-service] DB write failed (attempt " << attempts
                      << "), retrying in " << std::min(attempts * 2, 30) << "s\n";
            std::this_thread::sleep_for(std::chrono::seconds(std::min(attempts * 2, 30)));
        }
    }
}

bool recv_all(int fd, void *data, size_t size) {
    char *cursor = static_cast<char *>(data);
    while (size > 0) {
        ssize_t received = ::recv(fd, cursor, size, 0);
        if (received == 0) return false;
        if (received < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        cursor += received;
        size -= static_cast<size_t>(received);
    }
    return true;
}

void handle_client(int client_fd, iot::Database &raid_buffer, RetryQueue &retry_queue) {
    std::vector<iot::SensorReading> batch;
    uint32_t network_count = 0;
    if (recv_all(client_fd, &network_count, sizeof(network_count))) {
        uint32_t count = ntohl(network_count);
        if (count == 0 || count > 10000) {
            std::cerr << "[ingestion-service] rejected invalid batch size " << count << "\n";
        } else {
            batch.reserve(count);
            bool valid = true;
            for (uint32_t i = 0; i < count; ++i) {
                iot::SensorReading r;
                if (!recv_all(client_fd, &r, sizeof(r)) || !iot::verify(r)) {
                    valid = false;
                    break;
                }
                batch.push_back(r);
            }
            if (!valid) {
                std::cerr << "[ingestion-service] rejected incomplete or corrupt batch\n";
            } else {
            // 1. Durable write to RAID-backed local buffer first (fast, local disk).
            bool ok = raid_buffer.insert_batch(batch);
            // 2. ACK only after the durable local write succeeds.
            if (ok) {
                char ack = 'K';
                ::send(client_fd, &ack, 1, 0);
                // 3. Hand off to async SQL Server writer (decoupled from the ACK
                //    path so DB latency never affects ingest throughput).
                retry_queue.push(batch);
            } else {
                std::cerr << "[ingestion-service] local durable write failed, NOT acking batch\n";
            }
            }
        }
    }
    ::close(client_fd);
}

} // namespace

int main() {
    // Unbuffer stdout so log lines appear immediately under `docker logs` /
    // `kubectl logs` instead of sitting in a full buffer until it fills.
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int port = 9000;
    if (const char *v = std::getenv("LISTEN_PORT")) port = std::atoi(v);
    if (port < 1 || port > 65535) {
        std::cerr << "[ingestion-service] invalid LISTEN_PORT\n";
        return 1;
    }

    std::string raid_dir = "/data/ingest";
    if (const char *v = std::getenv("RAID_MOUNT_PATH")) raid_dir = v;

    iot::FileDatabase raid_buffer(raid_dir); // durable local write, RAID-backed volume

#ifdef USE_ODBC
    auto env_or = [](const char *name, const char *fallback) {
        const char *value = std::getenv(name);
        return std::string(value ? value : fallback);
    };
    std::string conn_str = "DRIVER={ODBC Driver 18 for SQL Server};"
                            "SERVER=" + env_or("SQL_SERVER_HOST", "sql-server") + ";"
                            "DATABASE=" + env_or("SQL_SERVER_DB", "IoTSensors") + ";"
                            "UID=" + env_or("SQL_SERVER_USER", "sa") + ";"
                            "PWD=" + env_or("SQL_SERVER_PASSWORD", "") + ";"
                            "Encrypt=yes;TrustServerCertificate=yes;";
    iot::SqlServerDatabase db(conn_str);
#else
    // Dev/test fallback: also just writes to the RAID-backed volume, under a
    // separate "table" file, so the pipeline is fully runnable without a
    // live SQL Server instance.
    iot::FileDatabase db(raid_dir + "/sql_mock");
#endif

    RetryQueue retry_queue;
    std::thread writer(db_writer_thread, std::ref(retry_queue), std::ref(db));

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "[ingestion-service] socket creation failed\n";
        g_running = false;
        writer.join();
        return 1;
    }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (::bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[ingestion-service] bind failed on port " << port << "\n";
        ::close(server_fd);
        g_running = false;
        writer.join();
        return 1;
    }
    if (::listen(server_fd, 128) < 0) {
        std::cerr << "[ingestion-service] listen failed\n";
        ::close(server_fd);
        g_running = false;
        writer.join();
        return 1;
    }
    std::cout << "[ingestion-service] listening on :" << port
              << " raid_mount=" << raid_dir << "\n";

    while (g_running) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        struct timeval tv{1, 0};
        setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &len);
        // Sequential handling is sufficient for the target traffic and makes
        // this service much easier to debug than a thread-per-client design.
        if (client_fd >= 0) handle_client(client_fd, raid_buffer, retry_queue);
    }

    ::close(server_fd);
    writer.join();
    return 0;
}
