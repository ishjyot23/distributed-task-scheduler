#!/bin/sh
set -eu

export COORDINATOR_ADDRESS="${COORDINATOR_ADDRESS:-127.0.0.1:50051}"
export TASK_DB_PATH="${TASK_DB_PATH:-/app/data/tasks.db}"
export WORKER_COUNT="${WORKER_COUNT:-3}"
export WORKER_CAPACITY="${WORKER_CAPACITY:-2}"
export PORT="${PORT:-10000}"
mkdir -p "$(dirname "$TASK_DB_PATH")"

pids=""
cleanup() {
  echo "Stopping dashboard, workers and coordinator..."
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  wait || true
}
trap cleanup INT TERM EXIT

python coordinator.py --host 0.0.0.0 --port 50051 &
COORD_PID=$!
pids="$pids $COORD_PID"

# Wait for the gRPC scheduler to come up before starting worker registrations.
python - <<'PY'
import os, time, grpc
address = os.environ.get("COORDINATOR_ADDRESS", "127.0.0.1:50051")
for attempt in range(60):
    try:
        channel = grpc.insecure_channel(address)
        grpc.channel_ready_future(channel).result(timeout=1)
        channel.close()
        print("Coordinator is ready.", flush=True)
        break
    except Exception:
        time.sleep(1)
else:
    raise SystemExit("Coordinator did not become ready within 60 seconds")
PY

count="$WORKER_COUNT"
capacity="$WORKER_CAPACITY"
case "$count" in *[!0-9]*|"") echo "WORKER_COUNT must be an integer from 1 to 6"; exit 2 ;; esac
case "$capacity" in *[!0-9]*|"") echo "WORKER_CAPACITY must be an integer from 1 to 4"; exit 2 ;; esac
if [ "$count" -lt 1 ] || [ "$count" -gt 6 ]; then echo "WORKER_COUNT must be from 1 to 6"; exit 2; fi
if [ "$capacity" -lt 1 ] || [ "$capacity" -gt 4 ]; then echo "WORKER_CAPACITY must be from 1 to 4"; exit 2; fi
i=1
while [ "$i" -le "$count" ]; do
  port=$((50060 + i))
  WORKER_ADVERTISE_HOST="127.0.0.1" python worker.py --id "worker-$i" --port "$port" --capacity "$capacity" &
  pids="$pids $!"
  i=$((i + 1))
done

streamlit run app.py --server.address 0.0.0.0 --server.port "$PORT" &
UI_PID=$!
pids="$pids $UI_PID"
wait "$UI_PID"
