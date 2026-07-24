# IoT Sensor Pipeline with Fault Tolerance

An end-to-end pipeline that carries sensor readings from a Raspberry Pi device,
through a Kubernetes-hosted ingestion layer, into a centralized SQL Server
database, with a query API for dashboards. Built in C++.

```
Raspberry Pi (sensor-agent)
      │  TCP, batched, WAL-buffered on failure
      ▼
Kubernetes: ingestion-service (Deployment, 3+ replicas)
      │  writes to RAID-backed PVC first, then async to SQL Server
      ▼                                   ▼
RAID-backed PersistentVolume         SQL Server (dbo.SensorReadings)
      ▲
      │  journal queries, cached
dashboard-api (Deployment, 2+ replicas)
```

## What's here

| Path | What it is |
|---|---|
| `common/protocol.hpp` | Shared wire-format struct (`SensorReading`) + CRC32 used by all three services |
| `sensor-agent/` | Runs on the Raspberry Pi. Reads sensor values, batches, sends over TCP, buffers to a local write-ahead log (WAL) on failure |
| `ingestion-service/` | Runs in Kubernetes. Accepts TCP batches, writes durably to a RAID-backed volume, then asynchronously to SQL Server |
| `dashboard-api/` | Runs in Kubernetes. HTTP query API with a short-TTL in-memory cache for sub-second dashboard responses |
| `sql/schema.sql` | SQL Server schema + indexes tuned for downstream analytics queries |
| `docker/` | Straightforward Docker builds for the three services |
| `k8s/` | Kubernetes manifests: namespace, RAID-backed StorageClass/PV/PVC, Deployments, Services, HPA, DaemonSet |
| `scripts/setup-raid.sh` | Builds the fault-tolerant mdadm RAID array backing the ingest PVC |
| `scripts/simulate-drive-failure.sh` | Fails/re-adds a disk to validate zero-downtime tolerance |
| `scripts/load-test.sh` | Generates 50k+ readings/day of traffic and times dashboard queries |

## 1. Build

Requires a C++17 compiler and CMake 3.13+.

```bash
mkdir build && cd build
cmake ..
make -j
```

This produces three binaries: `sensor-agent`, `ingestion-service`,
`dashboard-api`. By default `ingestion-service`/`dashboard-api` use a
file-backed mock database so you can run the whole pipeline locally without a
SQL Server instance. To build against real SQL Server via ODBC:

```bash
# requires unixODBC dev headers + Microsoft ODBC Driver 18 for SQL Server installed
cmake -DUSE_ODBC=ON ..
make -j
```

For learning and debugging, start with the default file-backed build. The
programs deliberately keep a simple control flow:

1. `sensor-agent` creates a batch and sends it, or saves it in its WAL.
2. `ingestion-service` handles one connection at a time, writes the journal,
   acknowledges it, and gives the batch to one SQL writer thread.
3. `dashboard-api` reads that journal and returns JSON.

The ODBC implementation is optional and is compiled only when `USE_ODBC=ON`.

## 2. Run locally (no Kubernetes, no SQL Server)

```bash
# terminal 1
RAID_MOUNT_PATH=/tmp/ingest ./build/ingestion-service

# terminal 2
RAID_MOUNT_PATH=/tmp/ingest ./build/dashboard-api

# terminal 3
SERVER_HOST=127.0.0.1 SERVER_PORT=9000 DEVICE_ID=test-sensor-01 ./build/sensor-agent

# terminal 4 — query it
curl "http://localhost:8080/api/readings/latest?limit=10"
```

Kill `ingestion-service` mid-run, watch `sensor-agent` start logging WAL
buffering, then restart it — the buffered readings replay automatically.

## 3. Set up the SQL Server database

```bash
sqlcmd -S <your-sql-server-host> -U sa -P '<password>' -i sql/schema.sql
```

This creates `IoTSensors.dbo.SensorReadings` with indexes on
`(DeviceId, ReadingTimeUtc)` and `(SensorType, ReadingTimeUtc)`, which is what
keeps downstream analytics queries as index seeks (not table scans) as the
table grows past 50,000 rows/day. The dashboard reads the RAID-backed durable
journal, so it remains available during a SQL Server outage.

## 4. Set up the fault-tolerant RAID storage

On the Kubernetes node that will back the ingest `PersistentVolume`:

```bash
sudo DEVICES="/dev/sdb /dev/sdc /dev/sdd /dev/sde" ./scripts/setup-raid.sh
```

Defaults to **RAID 10** (striped mirrors — tolerates a disk failure per
mirror pair, good write throughput for continuous ingest). Set
`RAID_LEVEL=6` for RAID 6 instead (tolerates any 2 simultaneous failures).
Mounts the array at `/mnt/raid-array`, matching the `hostPath`/`local`
volume referenced in `k8s/01-storage.yaml`.

Validate fault tolerance (repeat against different member disks, 10+ times,
ideally while `scripts/load-test.sh` is generating traffic):

```bash
sudo ./scripts/simulate-drive-failure.sh /dev/md0 /dev/sdc
```

This fails a disk, confirms the array stays active/degraded (no downtime),
writes a probe file to confirm the mount is still writable, removes the
failed disk, then re-adds it (simulating a hot-swap) and starts the rebuild.
See `docs/RAID.md` for more detail on the tradeoffs and how to interpret
`mdadm --detail` output.

## 5. Build and push the container images

```bash
# Run this on the Raspberry Pi to build its native image
docker build -f docker/Dockerfile.sensor-agent \
  -t <registry>/iot/sensor-agent:latest .

# ingestion-service (SQL Server via ODBC)
docker build -f docker/Dockerfile.ingestion-service \
  -t <registry>/iot/ingestion-service:latest .

# dashboard-api
docker build -f docker/Dockerfile.dashboard-api \
  -t <registry>/iot/dashboard-api:latest .

# Push after each local build has been tested
docker push <registry>/iot/sensor-agent:latest
docker push <registry>/iot/ingestion-service:latest
docker push <registry>/iot/dashboard-api:latest
```

The images use normal Debian-based build and runtime stages. The binaries keep
debug symbols and the server images include ordinary troubleshooting tools,
which makes failures easier to reproduce and inspect inside a container.

## 6. Deploy to Kubernetes

```bash
kubectl apply -f k8s/00-namespace-config.yaml
kubectl apply -f k8s/01-storage.yaml
kubectl apply -f k8s/02-ingestion-service.yaml
kubectl apply -f k8s/03-dashboard-api.yaml

# If your Pis are cluster-joined arm64 nodes (e.g. k3s):
kubectl apply -f k8s/04-sensor-agent-daemonset.yaml
```

Before applying, edit:
- `k8s/00-namespace-config.yaml` — real SQL Server credentials (use a proper
  secret store in production, this is a placeholder)
- `k8s/01-storage.yaml` — the `nodeAffinity` hostname(s) for your
  RAID-equipped storage node(s)
- `k8s/02-ingestion-service.yaml` / `03-dashboard-api.yaml` — your image
  registry paths

### Standalone Raspberry Pi deployment (not cluster-joined)

If the Pi isn't a Kubernetes node, run the agent directly on-device instead
of the DaemonSet:

```bash
docker run -d --restart unless-stopped \
  -e DEVICE_ID=rpi-greenhouse-01 \
  -e SERVER_HOST=<ingestion-service external/NodePort address> \
  -e SERVER_PORT=9000 \
  -v /var/lib/sensor-agent:/var/lib/sensor-agent \
  <registry>/iot/sensor-agent:latest
```

Expose `ingestion-service` externally (`type: LoadBalancer` or `NodePort`) if
the Pi is outside the cluster network.

## 7. Validate the throughput / latency targets

```bash
./scripts/load-test.sh 10 1000 localhost:8080
```

Runs 10 simulated sensor-agents at 1 reading/sec each (≈864,000 readings/day,
well above the 50,000/day target), then times 5 sequential
`GET /api/readings/latest` calls against `dashboard-api` to confirm sub-second
response times.

## API reference (dashboard-api)

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness/readiness check |
| `GET /api/readings/latest?device_id=<id>&limit=<n>` | Most recent `n` readings, optionally filtered by device |
| `GET /api/readings/summary?device_id=<id>&sensor_type=<type>&window_minutes=<n>` | Count, average, minimum, and maximum over a recent time window |

Responses are read from the durable ingest journal and served from a 2-second in-memory cache by default
(`CACHE_TTL_MS`), so repeated dashboard polling does not repeatedly scan the journal
while still refreshing fast enough for a live view.

## How each requirement is met

**Throughput / latency** — `sensor-agent` batches readings and ships them
over a persistent TCP connection; `ingestion-service` ACKs after a local
durable write and hands off to SQL Server asynchronously, so ingest
throughput isn't gated by database latency. `dashboard-api` serves reads from
the RAID-backed journal behind a short-TTL cache. SQL Server has covering
indexes (`sql/schema.sql`) for downstream analytics over accumulated history.

**Fault tolerance / zero downtime** — Three layers: (1) `sensor-agent`'s WAL
survives Pi reboots and network/ingestion outages; (2) `ingestion-service`
runs as a 3-replica Deployment behind a Service, and only ACKs after a
successful write to the RAID-backed volume; (3) the RAID array itself
(RAID 10 or RAID 6, `scripts/setup-raid.sh`) tolerates single-disk failure
with the array staying active/degraded, validated repeatedly with
`scripts/simulate-drive-failure.sh`.

**Debugging** — CMake defaults to a debug build. Container binaries are also
built with debug symbols and without compiler optimization, and the server
images use a regular Debian shell with basic process and network tools.
