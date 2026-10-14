# Distributed Task Scheduler with Dynamic Load Balancing (gRPC)

This is the next version of the Streamlit prototype. It keeps the dashboard but replaces the in-process worker pools with **separate worker processes and real gRPC calls**.

## Architecture in this version

```text
Browser
  |
  v
Streamlit dashboard (app.py) --gRPC--> Central scheduler (coordinator.py)
                                             |
                                      Pending task queue
                                      least-loaded selection
                                             |
                   +-------------------------+-------------------------+
                   |                         |                         |
                gRPC                      gRPC                      gRPC
                   v                         v                         v
             worker-1                   worker-2                   worker-3
           process/port               process/port               process/port
                   \                         |                        /
                    +-------- results/status/attempts ---------------+
                                             |
                                             v
                                      SQLite task history
```

The cloud Docker container starts the dashboard, coordinator, and each worker as separate operating-system processes. They communicate through gRPC on separate ports. This is a real RPC-based multi-process cluster and demonstrates dynamic load balancing. **It is still one host/container** in this starter deployment, so host failure takes down all components. For a multi-machine deployment, the same services can be split across private cloud services, with external persistent storage.

## Easiest cloud deployment — GitHub + Render

You do not need Python, Docker, VS Code, or any project dependencies installed on your computer. The cloud provider builds and runs the code.

1. Download and extract the ZIP archive.
2. Create a GitHub repository, e.g. `distributed-task-scheduler-grpc`.
3. Upload the **contents** of this project into the repository root. Ensure `Dockerfile`, `render.yaml`, `app.py`, `coordinator.py`, `worker.py`, `task.proto`, `requirements.txt`, `start.sh`, and `.streamlit/config.toml` are present. Do not upload the `data/` directory or any SQLite database.
4. Open Render and choose **New + → Blueprint** (Render reads `render.yaml` from the repository). Connect the GitHub repository and approve the Blueprint.
5. Confirm the service named `distributed-task-scheduler-grpc` and create/deploy it. Render builds the Docker image, generates Python gRPC code from `task.proto`, starts the coordinator, starts three workers, and starts Streamlit.
6. Open the public `onrender.com` URL for the web service. In the log stream, look for `gRPC service listening`, each `gRPC worker listening`, and `Registered worker-1` / `worker-2` / `worker-3`.

Deployment links:
- GitHub repository creation: https://github.com/new
- Render dashboard: https://dashboard.render.com/
- Render Docker deployment documentation: https://render.com/docs/docker
- Render Blueprint reference: https://render.com/docs/blueprint-spec

`render.yaml` uses the Free plan as a starting point where it is available. Free instances have limited compute, may sleep, and local files are not durable across deployments/restarts. Upgrade the service or configure supported persistent storage and a managed database if reliable task history is required. Pricing and platform limits can change.

## What to do in the dashboard to prove load balancing

### Experiment 1 — Small, fast tasks

1. Confirm worker-1, worker-2 and worker-3 all show Online.
2. Choose `Uppercase text`.
3. Enter `ishjyot`.
4. Submit 6 or 12 identical tasks.
5. Inspect **Dynamic load-balancing evidence** and **Assignment path** in task history. The expected output is `ISHJYOT`; task assignment should use more than one worker and, with all three workers online and idle, should be spread across the three workers.

The key change that fixes the earlier result is **batch enqueueing**: all submitted tasks are saved before the dispatcher starts filling worker slots. The scheduler dispatches without waiting for a previous task to complete. A rotating tie-break prevents worker-1 winning every equal-load tie.

### Experiment 2 — Visible concurrent execution (recommended for viva)

1. Select `Simulated workload (sleep)` and keep input `3`.
2. Submit **12 tasks** with three workers and capacity two.
3. Watch the active load bars and task table for around six seconds. Each worker has two simultaneous slots, so six jobs can run at once; the remaining jobs wait for a free slot.
4. Capture the load-balancing chart and completed task table after all tasks finish.

Actual time depends on cloud CPU and scheduling overhead; record what you observe rather than inventing performance figures.

### Experiment 3 — Worker exclusion and recovery

1. Click **Take out of rotation** on worker-2.
2. Submit 6 sleep tasks.
3. Verify new assignments are sent only to worker-1 and worker-3 while worker-2 is disabled.
4. Click **Re-enable worker** and submit another batch.
5. To test crash recovery, stop a worker process only in a controlled test environment. An RPC failure triggers up to three total attempts, then the dashboard records failure if it cannot recover. Stopping a task's worker may cause duplicate execution if the worker finished but its response was lost; the implementation guarantees neither exactly-once execution nor full host-level fault tolerance.

## Settings

The Render Blueprint sets these environment variables:

- `WORKER_COUNT=3`: number of real worker processes started by `start.sh` (1–6 is a sensible demo range).
- `WORKER_CAPACITY=2`: concurrency slots in each worker.
- `TASK_DB_PATH=/app/data/tasks.db`: local SQLite database path; not durable across all redeployments on ephemeral storage.
- `MAX_TASK_ATTEMPTS=3`: total attempts before an RPC-related task failure is final.
- `HEARTBEAT_TIMEOUT_SECONDS=12`: time without worker heartbeats before marking a worker offline.

Change `WORKER_COUNT` or `WORKER_CAPACITY` in the Render service environment, then redeploy. Do not use a dashboard slider to claim worker processes exist; the dashboard intentionally displays workers that actually registered with the scheduler.

## Source files

- `app.py` — Streamlit client; uses gRPC for task submission, status queries and worker controls.
- `coordinator.py` — scheduler service, task queue, least-loaded selection, heartbeats, RPC retry and SQLite persistence.
- `worker.py` — independent worker process exposing `WorkerService.Execute` over gRPC.
- `task.proto` — Protocol Buffers contract for scheduler and workers.
- `Dockerfile` — installs cloud dependencies and generates the Python gRPC service modules at image build time.
- `start.sh` — starts the coordinator, N worker processes and Streamlit; cloud platform exposes only the dashboard's HTTP port.
- `render.yaml` — Render deployment Blueprint.
- `tests/smoke_test.py` — automated local gRPC smoke test to verify workers register and tasks distribute.

## Optional local run (only if you want to test outside the cloud)

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. task.proto
```

Then use separate terminals:

```bash
# Terminal 1
python coordinator.py --host 0.0.0.0 --port 50051

# Terminals 2, 3 and 4
python worker.py --id worker-1 --port 50061 --capacity 2
python worker.py --id worker-2 --port 50062 --capacity 2
python worker.py --id worker-3 --port 50063 --capacity 2

# Terminal 5
streamlit run app.py
```

To run the integration smoke test, generate the gRPC files first and then run:

```bash
python tests/smoke_test.py
```

## What is honestly implemented vs. future work

Implemented: real gRPC request/response calls, separately running worker processes, worker registration and heartbeats, batch task queueing, load-aware scheduling, rotating tie-breaks, bounded RPC-failure retries, online/offline scheduling controls, task execution/status history, SQLite persistence during the running instance, and CSV export.

Not implemented: physically separate machines in this default one-container deployment, exactly-once semantics, replicated scheduler/leader election, replicated database, authenticated/encrypted service-to-service gRPC, durable data across ephemeral-host restarts, or production-grade sandboxing. These are valid next extensions, not claims to make about this version.
