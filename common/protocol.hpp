// common/protocol.hpp
// Wire format shared between sensor-agent and ingestion-service.
// Kept as a fixed-size POD struct so it can be sent raw over TCP with no
// serialization library dependency.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <chrono>
#include <array>

namespace iot {

constexpr uint32_t PROTOCOL_MAGIC = 0x53454E52; // "SENR"
constexpr uint16_t PROTOCOL_VERSION = 1;

enum class SensorType : uint8_t {
    TEMPERATURE = 0,
    HUMIDITY    = 1,
    PRESSURE    = 2,
    VIBRATION   = 3,
    LIGHT       = 4
};

#pragma pack(push, 1)
struct SensorReading {
    uint32_t magic        = PROTOCOL_MAGIC;
    uint16_t version      = PROTOCOL_VERSION;
    char     device_id[32]{};      // e.g. "rpi-greenhouse-01"
    uint8_t  sensor_type   = 0;
    double   value         = 0.0;
    int64_t  epoch_millis  = 0;    // reading timestamp
    uint32_t sequence_no    = 0;   // monotonically increasing per-device counter
    uint32_t crc32          = 0;   // simple integrity check, computed last

    void set_device_id(const std::string &id) {
        std::memset(device_id, 0, sizeof(device_id));
        std::strncpy(device_id, id.c_str(), sizeof(device_id) - 1);
    }
};
#pragma pack(pop)

static_assert(sizeof(SensorReading) == 4 + 2 + 32 + 1 + 8 + 8 + 4 + 4,
              "SensorReading must stay a tightly packed POD for wire compat");

// Small, dependency-free CRC32 (IEEE 802.3 polynomial).
inline uint32_t crc32_compute(const void *data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
            values[i] = c;
        }
        return values;
    }();
    const auto *bytes = static_cast<const unsigned char *>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++)
        crc = table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFF;
}

inline void sign(SensorReading &r) {
    r.crc32 = 0;
    r.crc32 = crc32_compute(&r, sizeof(SensorReading) - sizeof(r.crc32));
}

inline bool verify(const SensorReading &r) {
    SensorReading copy = r;
    uint32_t stored = copy.crc32;
    copy.crc32 = 0;
    return crc32_compute(&copy, sizeof(SensorReading) - sizeof(copy.crc32)) == stored;
}

inline int64_t now_millis() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline const char* sensor_type_name(SensorType t) {
    switch (t) {
        case SensorType::TEMPERATURE: return "temperature";
        case SensorType::HUMIDITY:    return "humidity";
        case SensorType::PRESSURE:    return "pressure";
        case SensorType::VIBRATION:   return "vibration";
        case SensorType::LIGHT:       return "light";
        default: return "unknown";
    }
}

} // namespace iot
