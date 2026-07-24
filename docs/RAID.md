# RAID design notes

## Why RAID 10 by default

The ingest volume takes continuous small writes from `ingestion-service`
(every batch flush) as well as periodic reads from `dashboard-api`. RAID 10
(mirrored pairs, striped across pairs) gives:

- Tolerance of at least one disk failure per mirror pair (and often more,
  depending on which disks fail) with no rebuild-induced write penalty.
- Better random write throughput than parity-based RAID levels, which matters
  because every ingested batch is a write.

Trade-off: 50% usable capacity vs. raw disk. If capacity matters more than
write throughput for your deployment, use RAID 6 (`RAID_LEVEL=6`):
tolerates any 2 simultaneous disk failures, ~75%+ usable capacity at 4+
disks, but parity calculation adds write overhead and rebuilds are slower
and I/O-heavier.

| | RAID 10 | RAID 6 |
|---|---|---|
| Min disks | 4 | 4 |
| Tolerates | ≥1 failure/mirror pair | any 2 failures |
| Usable capacity | 50% | (n-2)/n |
| Write performance | High | Lower (parity calc) |
| Rebuild time | Fast | Slower |

## Validating zero-downtime tolerance

`scripts/simulate-drive-failure.sh` automates the standard mdadm
fail/remove/re-add cycle:

1. `mdadm --manage /dev/md0 --fail /dev/sdX` — marks a member disk faulted.
2. Confirms `mdadm --detail` reports the array as `active, degraded` (not
   `failed`/`inactive`) — degraded means still serving reads/writes.
3. Writes a timestamped probe line to the mounted filesystem to confirm it's
   still writable while degraded.
4. `mdadm --manage /dev/md0 --remove /dev/sdX` then `--add /dev/sdX`
   simulates hot-swapping a replacement disk and triggers a rebuild.

Run this at least 10 times, cycling through different member disks, ideally
while `scripts/load-test.sh` is generating ingest traffic, to confirm no
write is ever rejected or lost during a failure/rebuild cycle. Watch
`/proc/mdstat` during rebuilds to confirm sync completes and the array
returns to `active` (not degraded).

## Monitoring

```bash
cat /proc/mdstat                # live sync/degraded state
mdadm --detail /dev/md0         # full array + per-disk status
mdadm --monitor --scan --daemonise  # optional: email/syslog alerts on events
```

In production, wire `mdadm --monitor` or a node-level exporter (e.g.
`node_exporter`'s `mdadm` collector) into your cluster's alerting so a
degraded array pages someone before a second disk fails.
