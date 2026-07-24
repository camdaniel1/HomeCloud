#!/usr/bin/env bash
# scripts/setup-raid.sh
#
# Builds a fault-tolerant mdadm RAID array on a Kubernetes storage node to
# back the ingest PersistentVolume, and mounts it where k8s/01-storage.yaml
# expects it (/mnt/raid-array/ingest).
#
# Defaults to RAID 10 (striped mirrors): tolerates at least one disk failure
# per mirror pair with no downtime, and better write throughput than RAID 6,
# which matters for a continuous ingest workload. Pass RAID_LEVEL=6 for
# RAID 6 instead (tolerates any 2 disk failures, less usable capacity
# overhead loss at 4+ disks).
#
# USAGE:
#   sudo DEVICES="/dev/sdb /dev/sdc /dev/sdd /dev/sde" ./setup-raid.sh
#
# Run on the Kubernetes node referenced in k8s/01-storage.yaml's
# nodeAffinity (k8s-storage-node-1).

set -euo pipefail

RAID_LEVEL="${RAID_LEVEL:-10}"
RAID_DEVICE="${RAID_DEVICE:-/dev/md0}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/raid-array}"
DEVICES="${DEVICES:?Set DEVICES to a space-separated list of at least 4 block devices, e.g. '/dev/sdb /dev/sdc /dev/sdd /dev/sde'}"

echo "== Installing mdadm =="
if command -v apt-get >/dev/null; then
  apt-get update && apt-get install -y mdadm xfsprogs
elif command -v yum >/dev/null; then
  yum install -y mdadm xfsprogs
fi

NUM_DEVICES=$(wc -w <<< "$DEVICES")
if [ "$RAID_LEVEL" = "10" ] && [ "$NUM_DEVICES" -lt 4 ]; then
  echo "RAID 10 needs at least 4 devices, got $NUM_DEVICES" >&2
  exit 1
fi
if [ "$RAID_LEVEL" = "6" ] && [ "$NUM_DEVICES" -lt 4 ]; then
  echo "RAID 6 needs at least 4 devices, got $NUM_DEVICES" >&2
  exit 1
fi

echo "== Creating RAID $RAID_LEVEL array $RAID_DEVICE from: $DEVICES =="
mdadm --create --verbose "$RAID_DEVICE" \
  --level="$RAID_LEVEL" \
  --raid-devices="$NUM_DEVICES" \
  $DEVICES

echo "== Waiting for initial sync (this can take a while on real disks) =="
mdadm --wait "$RAID_DEVICE" || true
cat /proc/mdstat

echo "== Persisting array config so it reassembles on reboot =="
mkdir -p /etc/mdadm
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
if command -v update-initramfs >/dev/null; then
  update-initramfs -u
fi

echo "== Formatting and mounting =="
mkfs.xfs -f "$RAID_DEVICE"
mkdir -p "$MOUNT_POINT/ingest"
chown 10001:10001 "$MOUNT_POINT/ingest"
chmod 0770 "$MOUNT_POINT/ingest"
mount "$RAID_DEVICE" "$MOUNT_POINT"
mkdir -p "$MOUNT_POINT/ingest"

UUID=$(blkid -s UUID -o value "$RAID_DEVICE")
if ! grep -q "$UUID" /etc/fstab; then
  echo "UUID=$UUID $MOUNT_POINT xfs defaults 0 0" >> /etc/fstab
fi

echo "== Done. RAID $RAID_LEVEL array mounted at $MOUNT_POINT =="
echo "   mdadm --detail $RAID_DEVICE   # check array/disk health"
echo "   cat /proc/mdstat              # check sync/degraded state"
