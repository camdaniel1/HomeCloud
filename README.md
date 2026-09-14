# IoT Sensor Pipeline

A small C++ project that sends simulated sensor readings through a simple data
pipeline. You can run and debug the complete project locally before trying
Docker, SQL Server, RAID, or Kubernetes.

## How it works

```text
sensor-agent  -->  ingestion-service  -->  readings.tsv
                                              |
                                              v
                                        dashboard-api
```

The project contains three programs:

1. `sensor-agent` creates temperature, humidity, pressure, vibration, and light
   readings and sends them over TCP.
2. `ingestion-service` checks each reading and saves it to `readings.tsv`.
3. `dashboard-api` reads that file and returns the data as JSON over HTTP.

If ingestion is unavailable, the agent saves readings in a local write-ahead
log (WAL). It sends those readings again when ingestion comes back online.

## Project layout

| Path | Purpose |
|---|---|
| `sensor-agent/src/main.cpp` | Creates and sends sensor readings |
| `ingestion-service/src/main.cpp` | Receives batches and coordinates storage |
| `ingestion-service/src/database.hpp` | File storage and optional SQL Server storage |
| `dashboard-api/src/main.cpp` | Provides the HTTP API |
| `common/protocol.hpp` | Shared reading format and CRC validation |
| `sql/schema.sql` | Optional SQL Server schema |
| `docker/` | Dockerfiles for all three programs |
| `k8s/` | Optional Kubernetes configuration |
| `scripts/` | Load testing and RAID helper scripts |

## Requirements

For the basic local version, you need:

- Linux or WSL
- A C++17 compiler such as GCC
- CMake 3.13 or newer
- `curl` for testing the API

SQL Server, Docker, Kubernetes, and physical sensors are not required for local
development.

## Build locally

From the repository root:

```bash
mkdir build
cd build
cmake ..
cmake --build . -j
```

CMake creates a debug build by default. Compiler optimization is disabled and
debug symbols are included.

The build produces:

```text
build/sensor-agent
build/ingestion-service
build/dashboard-api
```

## Run locally

Open three terminals in the repository root.

Start ingestion:

```bash
RAID_MOUNT_PATH=/tmp/iot-data ./build/ingestion-service
```

Start the dashboard API:

```bash
RAID_MOUNT_PATH=/tmp/iot-data ./build/dashboard-api
```

Start one simulated sensor:

```bash
SERVER_HOST=127.0.0.1 \
SERVER_PORT=9000 \
DEVICE_ID=test-sensor-01 \
INTERVAL_MS=1000 \
BATCH_SIZE=5 \
WAL_PATH=/tmp/iot-sensor-wal.log \
./build/sensor-agent
```

The agent waits for a complete batch before sending it. With these settings,
new data appears about every five seconds.

## Test the API

```bash
# Health check
curl http://localhost:8080/health

# Latest ten readings
curl "http://localhost:8080/api/readings/latest?limit=10"

# Readings from one device
curl "http://localhost:8080/api/readings/latest?device_id=test-sensor-01&limit=10"

# Summary for the last hour
curl "http://localhost:8080/api/readings/summary?device_id=test-sensor-01&window_minutes=60"

# Temperature summary for the last hour
curl "http://localhost:8080/api/readings/summary?sensor_type=temperature&window_minutes=60"
```

## Test failure recovery

While the sensor agent is running:

1. Stop `ingestion-service` with `Ctrl+C`.
2. Wait long enough for the agent to create another batch.
3. Check that `/tmp/iot-sensor-wal.log` was created.
4. Restart `ingestion-service`.
5. Watch the agent report that it is replaying buffered readings.
6. Query the dashboard and confirm that the readings appear.

This is the easiest way to understand and debug the fault-tolerance behavior.

## Configuration

### Sensor agent

| Variable | Default | Meaning |
|---|---:|---|
| `DEVICE_ID` | `rpi-sensor-01` | Name stored with each reading |
| `SERVER_HOST` | `ingestion-service` | Ingestion server hostname |
| `SERVER_PORT` | `9000` | Ingestion TCP port |
| `INTERVAL_MS` | `1000` | Time between generated readings |
| `BATCH_SIZE` | `20` | Readings sent in one batch |
| `WAL_PATH` | `/var/lib/sensor-agent/wal.log` | Local retry file |

### Ingestion service

| Variable | Default | Meaning |
|---|---:|---|
| `LISTEN_PORT` | `9000` | TCP port used by sensor agents |
| `RAID_MOUNT_PATH` | `/data/ingest` | Directory containing `readings.tsv` |

### Dashboard API

| Variable | Default | Meaning |
|---|---:|---|
| `LISTEN_PORT` | `8080` | HTTP API port |
| `RAID_MOUNT_PATH` | `/data/ingest` | Directory containing `readings.tsv` |
| `CACHE_TTL_MS` | `2000` | How long API responses remain cached |

## Debugging tips

- Start with `BATCH_SIZE=1` so each reading is sent immediately.
- Use a larger `INTERVAL_MS`, such as `5000`, when stepping through code.
- Inspect saved readings with `tail -f /tmp/iot-data/readings.tsv`.
- Give each test agent a different `DEVICE_ID`.
- Debug one program while the other two run normally.

The main path is intentionally direct. Ingestion handles one client at a time,
writes the batch, acknowledges it, and places it on one SQL writer queue. SQL
code is excluded from normal local builds.

## Run a load test

Start all three programs, then run:

```bash
./scripts/load-test.sh 10 1000 localhost:8080
```

This starts ten simulated agents, waits for data, measures five API requests,
and stops the test agents when it finishes.

## Optional: SQL Server

Create the database:

```bash
sqlcmd -S <server> -U sa -P '<password>' -i sql/schema.sql
```

Build ingestion with ODBC support:

```bash
mkdir build-odbc
cd build-odbc
cmake -DUSE_ODBC=ON ..
cmake --build . -j
```

This requires unixODBC development headers and Microsoft ODBC Driver 18.
Configure it with `SQL_SERVER_HOST`, `SQL_SERVER_DB`, `SQL_SERVER_USER`, and
`SQL_SERVER_PASSWORD`.

## Optional: Docker

Build the images from the repository root:

```bash
docker build -f docker/Dockerfile.sensor-agent -t iot/sensor-agent:latest .
docker build -f docker/Dockerfile.ingestion-service -t iot/ingestion-service:latest .
docker build -f docker/Dockerfile.dashboard-api -t iot/dashboard-api:latest .
```

The sensor image builds for the machine running Docker. Build it on a Raspberry
Pi when you need a Raspberry Pi image.

## Optional: Kubernetes and RAID

The `k8s` directory contains deployments for a larger environment. Before
applying them, replace the placeholder image names, SQL password, storage size,
and storage-node hostname.

```bash
kubectl apply -f k8s/00-namespace-config.yaml
kubectl apply -f k8s/01-storage.yaml
kubectl apply -f k8s/02-ingestion-service.yaml
kubectl apply -f k8s/03-dashboard-api.yaml
```

Apply `k8s/04-sensor-agent-daemonset.yaml` only when Raspberry Pi devices are
Kubernetes nodes. RAID setup and failure simulation are described in
[`docs/RAID.md`](docs/RAID.md). These operations modify real disks, so use them
only on a dedicated test or deployment machine.
