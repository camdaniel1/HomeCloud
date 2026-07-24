#!/usr/bin/env bash
# scripts/load-test.sh
#
# Spins up N sensor-agent instances at an interval that collectively produces
# 50,000+ readings/day, then times dashboard-api query latency to validate
# the sub-second response requirement.
#
# USAGE:
#   ./load-test.sh [num_agents] [interval_ms] [dashboard_host:port]
#
# 50,000 readings/day / 86,400 sec/day ~= 0.58 readings/sec needed overall.
# Default below (10 agents @ 1 reading/sec each = 10/sec = 864,000/day)
# comfortably exceeds the 50k/day target so you can see steady-state
# behavior, not just the bare minimum.

set -euo pipefail

NUM_AGENTS="${1:-10}"
INTERVAL_MS="${2:-1000}"
DASHBOARD="${3:-localhost:8080}"

if ! [[ "$NUM_AGENTS" =~ ^[1-9][0-9]*$ && "$INTERVAL_MS" =~ ^[1-9][0-9]*$ ]]; then
  echo "num_agents and interval_ms must be positive integers" >&2
  exit 1
fi

pids=()
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "Starting $NUM_AGENTS sensor-agent processes @ ${INTERVAL_MS}ms interval"
echo "(projected: $(( NUM_AGENTS * 86400000 / INTERVAL_MS )) readings/day)"

for i in $(seq 1 "$NUM_AGENTS"); do
  DEVICE_ID="load-test-device-$i" \
  SERVER_HOST="${SERVER_HOST:-127.0.0.1}" \
  SERVER_PORT="${SERVER_PORT:-9000}" \
  INTERVAL_MS="$INTERVAL_MS" \
  WAL_PATH="/tmp/loadtest-wal-$i.log" \
  ./build/sensor-agent &
  pids+=($!)
done

echo "Agents running (pids: ${pids[*]}). Sleeping 30s to build up data..."
sleep 30

echo "== Timing dashboard query response =="
for i in 1 2 3 4 5; do
  START=$(date +%s%N)
  curl -s "http://$DASHBOARD/api/readings/latest?limit=50" > /dev/null
  END=$(date +%s%N)
  echo "Query $i: $(( (END - START) / 1000000 )) ms"
done

echo "Stopping load test agents..."
cleanup
pids=()
echo "Done."
