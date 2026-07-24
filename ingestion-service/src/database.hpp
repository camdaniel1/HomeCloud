// ingestion-service/src/database.hpp
//
// Thin persistence layer. Two backends:
//  - ODBC backend (compiled with -DUSE_ODBC) talks to SQL Server via
//    unixODBC + the Microsoft ODBC Driver for SQL Server. This is the
//    production path used in the Kubernetes deployment.
//  - File backend (default) appends newline-delimited rows to a file on the
//    RAID-backed PersistentVolume. Useful for local dev/testing without a
//    SQL Server instance, and doubles as the pipeline's write path if the
//    database is briefly unavailable (batches are retried, never dropped).
//
// Both backends write through the same RAID-backed volume mount
// (/data/ingest by default) before acknowledging the sensor-agent, so a
// single disk failure in the underlying RAID array never loses an
// already-ACKed batch.
#pragma once

#include "../../common/protocol.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#ifdef USE_ODBC
#include <sql.h>
#include <sqlext.h>
#endif

namespace iot {

class Database {
public:
    virtual ~Database() = default;
    virtual bool insert_batch(const std::vector<SensorReading> &batch) = 0;
};

// ------------------------- File-backed mock/dev backend -------------------------
class FileDatabase : public Database {
public:
    explicit FileDatabase(std::string dir) : dir_(std::move(dir)) {
        // Must create the directory first -- std::ofstream silently fails
        // (no exception by default) if the parent directory doesn't exist,
        // which previously caused every write to this backend to be a no-op.
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        if (ec) {
            std::cerr << "[database] failed to create directory " << dir_
                      << ": " << ec.message() << "\n";
        }
        std::ofstream touch(dir_ + "/readings.tsv", std::ios::app);
        if (!touch) {
            std::cerr << "[database] warning: could not open "
                      << dir_ + "/readings.tsv" << " for writing\n";
        }
    }

    bool insert_batch(const std::vector<SensorReading> &batch) override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream encoded;
        for (const auto &r : batch) {
            encoded << r.device_id << '\t'
                    << sensor_type_name(static_cast<SensorType>(r.sensor_type)) << '\t'
                    << r.value << '\t'
                    << r.epoch_millis << '\t'
                    << r.sequence_no << '\n';
        }
        std::string data = encoded.str();
        std::string path = dir_ + "/readings.tsv";
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) return false;
        bool ok = (::flock(fd, LOCK_EX) == 0);
        size_t offset = 0;
        while (ok && offset < data.size()) {
            ssize_t written = ::write(fd, data.data() + offset, data.size() - offset);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) { ok = false; break; }
            offset += static_cast<size_t>(written);
        }
        if (ok) ok = (::fsync(fd) == 0);
        ::flock(fd, LOCK_UN);
        if (::close(fd) != 0) ok = false;
        return ok;
    }

private:
    std::string dir_;
    std::mutex mutex_;
};

#ifdef USE_ODBC
// ------------------------- SQL Server via ODBC -------------------------
class SqlServerDatabase : public Database {
public:
    explicit SqlServerDatabase(std::string conn_str) : conn_str_(std::move(conn_str)) {
        connect();
    }

    ~SqlServerDatabase() override { disconnect(); }

    bool insert_batch(const std::vector<SensorReading> &batch) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ && !connect()) return false;

        SQLHSTMT stmt;
        if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, dbc_, &stmt))) return false;
        if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc_, SQL_ATTR_AUTOCOMMIT,
                                             (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0))) {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            return false;
        }
        const char *sql =
            "INSERT INTO dbo.SensorReadings "
            "(DeviceId, SensorType, Value, ReadingTimeUtc, SequenceNo) "
            "VALUES (?, ?, ?, DATEADD(millisecond, ?, DATEADD(day, ?, '19700101')), ?)";

        bool all_ok = SQL_SUCCEEDED(SQLPrepare(stmt, (SQLCHAR *)sql, SQL_NTS));
        for (const auto &r : batch) {
            if (!all_ok) break;
            std::string device_id(r.device_id);
            std::string sensor_type = sensor_type_name(static_cast<SensorType>(r.sensor_type));
            SQLLEN device_length = SQL_NTS;
            SQLLEN type_length = SQL_NTS;
            SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 32, 0,
                              (SQLPOINTER)device_id.c_str(), device_id.size() + 1, &device_length);
            SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 16, 0,
                              (SQLPOINTER)sensor_type.c_str(), sensor_type.size() + 1, &type_length);
            SQLDOUBLE value = r.value;
            SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &value, 0, nullptr);
            SQLINTEGER days = static_cast<SQLINTEGER>(r.epoch_millis / 86400000);
            SQLINTEGER millis_in_day = static_cast<SQLINTEGER>(r.epoch_millis % 86400000);
            SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0,
                             &millis_in_day, 0, nullptr);
            SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0,
                             &days, 0, nullptr);
            SQLUINTEGER seq = r.sequence_no;
            SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_ULONG, SQL_INTEGER, 0, 0, &seq, 0, nullptr);

            all_ok = SQL_SUCCEEDED(SQLExecute(stmt));
            SQLFreeStmt(stmt, SQL_CLOSE);
            SQLFreeStmt(stmt, SQL_RESET_PARAMS);
        }
        SQLRETURN transaction = SQLEndTran(SQL_HANDLE_DBC, dbc_,
                                            all_ok ? SQL_COMMIT : SQL_ROLLBACK);
        if (!SQL_SUCCEEDED(transaction)) all_ok = false;
        SQLSetConnectAttr(dbc_, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        if (!all_ok) disconnect();
        return all_ok;
    }

private:
    bool connect() {
        disconnect();
        if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_)) ||
            !SQL_SUCCEEDED(SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0)) ||
            !SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_))) {
            disconnect();
            return false;
        }
        SQLCHAR outstr[1024];
        SQLSMALLINT outstrlen;
        SQLRETURN ret = SQLDriverConnect(
            dbc_, nullptr, (SQLCHAR *)conn_str_.c_str(), SQL_NTS,
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        connected_ = SQL_SUCCEEDED(ret);
        if (!connected_) {
            std::cerr << "[database] failed to connect to SQL Server\n";
            disconnect();
        }
        return connected_;
    }

    void disconnect() {
        if (dbc_) { SQLDisconnect(dbc_); SQLFreeHandle(SQL_HANDLE_DBC, dbc_); }
        if (env_) SQLFreeHandle(SQL_HANDLE_ENV, env_);
        dbc_ = nullptr;
        env_ = nullptr;
        connected_ = false;
    }
    std::string conn_str_;
    SQLHENV env_ = nullptr;
    SQLHDBC dbc_ = nullptr;
    bool connected_ = false;
    std::mutex mutex_;
};
#endif // USE_ODBC

} // namespace iot
