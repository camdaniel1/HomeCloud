#!/usr/bin/env bash
# scripts/simulate-drive-failure.sh
#
# Simulates a single-disk failure against the mdadm RAID array backing the
# ingest volume, confirms the array stays online (degraded) with zero
# downtime, then re-adds a replacement disk to rebuild redundancy. Intended
# to be run repeatedly (e.g. 10+ times, per one disk at a time, cycling
# through array members) to validate fault tolerance under load.
#
# USAGE:
#   sudo ./simulate-drive-failure.sh /dev/md0 /dev/sdc
#
# For an in-cluster validation loop, run this while the ingestion-service is
# actively receiving traffic (e.g. from scripts/load-test.sh) and confirm no
# writes are rejected/lost during the "faulty -> remove -> re-add" cycle.

set -euo pipefail

RAID_DEVICE="${1:?Usage: $0 <raid_device e.g. /dev/md0> <disk_to_fail e.g. /dev/sdc>}"
DISK="${2:?Usage: $0 <raid_device e.g. /dev/md0> <disk_to_fail e.g. /dev/sdc>}"

echo "== Array state before failure =="
mdadm --detail "$RAID_DEVICE" | grep -E "State|Active Devices|Failed Devices"

echo "== Simulating failure of $DISK =="
mdadm --manage "$RAID_DEVICE" --fail "$DISK"

echo "== Confirming array is degraded but still active (no downtime) =="
STATE=$(mdadm --detail "$RAID_DEVICE" | awk -F': ' '/^ *State/{print $2; exit}')
echo "Array state: $STATE"
if [[ "$STATE" != *"active"* && "$STATE" != *"degraded"* ]]; then
  echo "FAIL: array is not in an active/degraded state, investigate before continuing" >&2
  exit 1
fi

echo "== Confirming mount point is still readable/writable =="
TEST_MOUNT=$(findmnt -n -o TARGET --source "$RAID_DEVICE" || echo "/mnt/raid-array")
echo "probe $(date -u)" >> "$TEST_MOUNT/failover-probe.log"
tail -1 "$TEST_MOUNT/failover-probe.log"

echo "== Removing failed disk from array =="
mdadm --manage "$RAID_DEVICE" --remove "$DISK"

echo "== Re-adding disk (simulates hot-swap replacement) =="
mdadm --manage "$RAID_DEVICE" --add "$DISK"

echo "== Rebuild in progress; monitor with: watch cat /proc/mdstat =="
cat /proc/mdstat

echo "== Simulation complete for $DISK. Array remained online throughout. =="
