# Architecture, Algorithm and Viva Notes

## Scheduling algorithm

For each queued task:
1. List workers whose heartbeat is recent, which are enabled, and whose active task count is below capacity.
2. Rank eligible workers by `active_tasks / capacity` (least utilization), then by active task count, lifetime assignment count, and rotating round-robin distance.
3. Reserve a slot and record the worker assignment before issuing the remote gRPC call.
4. Dispatch without blocking the scheduling loop on the task result. This permits all available worker slots to fill concurrently.
5. On success, store result, duration, worker ID and completion status. On a computation error, store Failed. On an RPC failure, release the slot and requeue if attempts remain.

Batch submission inserts all jobs before waking the dispatcher. The old prototype called `submit()` and `_dispatch()` for every tiny task synchronously. By the time the next fast uppercase task arrived, worker-1 might already be idle and the tie-break key always favored the lowest worker ID. The RPC version queues the whole batch and uses a rotating tie-break.

## Important terms

- **Client:** submits work and reads results; here it is the Streamlit dashboard.
- **Scheduler/coordinator:** tracks workers and jobs, and makes placement decisions.
- **Worker:** separate Python process that executes the RPC request.
- **gRPC:** remote procedure call framework; generated from Protocol Buffers in `task.proto`.
- **Heartbeat:** periodic message showing that a worker is still responding.
- **Dynamic load balancing:** assignments are recalculated from current worker load/capacity, not permanently assigned by task number.
- **Concurrency capacity:** maximum simultaneous task RPCs accepted by one worker.
- **Retry / at-least-once:** an RPC failure can cause a task to run again. If the first worker completed work but its reply was lost, a second execution is possible.
- **SQLite persistence:** task records remain across application reruns while the database file remains available. Ephemeral cloud filesystem does not guarantee persistence through redeploys/restarts.
- **Single point of failure:** all processes run in one container in this easy-deployment edition; losing the container loses the running cluster.

## Sample viva explanation

“I built a distributed task scheduler with a Streamlit client, a central gRPC scheduler and three independent worker processes. The dashboard submits a batch through the `SubmitBatch` RPC. The coordinator stores the batch in SQLite and picks the worker with the lowest active-load-to-capacity ratio. Each worker is a separate gRPC server, and its `Execute` RPC returns the result. The coordinator tracks heartbeats, updates task records and retries RPC failures within a configured limit. The current cloud setup runs those independent processes in one container, so it demonstrates RPC and scheduling across processes but is not yet a multi-machine or highly available cluster.”

## Recommended measured experiment

Run 12 sleep tasks where each task sleeps 3 seconds:
- Configuration A: `WORKER_COUNT=1`, `WORKER_CAPACITY=2`.
- Configuration B: `WORKER_COUNT=3`, `WORKER_CAPACITY=2`.
- Repeat each configuration three times.
- Record batch completion time and per-worker assignment counts.

The theoretical waves are six tasks per wave in configuration B, versus two tasks per wave in configuration A. Actual times will be higher due to overhead and depend on resources. Record measured values; do not present theoretical durations as observed results.
