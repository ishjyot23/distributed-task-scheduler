from __future__ import annotations

import os
import time
from collections import Counter
from datetime import datetime

import grpc
import pandas as pd
import streamlit as st
from google.protobuf import empty_pb2

import task_pb2
import task_pb2_grpc

COORDINATOR_ADDRESS = os.environ.get("COORDINATOR_ADDRESS", "127.0.0.1:50051")
TASK_TYPES = ["uppercase", "sum", "fibonacci", "sleep", "word_count", "prime_count"]
TASK_LABELS = {
    "uppercase": "Uppercase text",
    "sum": "Sum numbers",
    "fibonacci": "Fibonacci",
    "sleep": "Simulated workload (sleep)",
    "word_count": "Word / character count",
    "prime_count": "Count primes",
}
TASK_DEFAULTS = {
    "uppercase": "distributed systems",
    "sum": "10,20,30",
    "fibonacci": "30",
    "sleep": "3",
    "word_count": "Distributed systems share work across multiple workers",
    "prime_count": "10000",
}
TASK_HELP = {
    "uppercase": "Converts text to uppercase.",
    "sum": "Enter comma-separated numbers, e.g. 10,20,30.",
    "fibonacci": "Computes Fibonacci(n), where n is 0–5000.",
    "sleep": "Best for visibly demonstrating concurrent execution and dynamic load balancing.",
    "word_count": "Counts whitespace-separated words and characters.",
    "prime_count": "Counts prime numbers up to a limit between 2 and 200,000.",
}


@st.cache_resource
def get_rpc_client():
    channel = grpc.insecure_channel(COORDINATOR_ADDRESS)
    return channel, task_pb2_grpc.SchedulerServiceStub(channel)


def call_rpc(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except grpc.RpcError as exc:
        st.error(
            f"Could not contact the scheduler at `{COORDINATOR_ADDRESS}`. "
            f"Check that the coordinator process is running. RPC status: {exc.code().name}."
        )
        st.caption("If this is a cloud deployment, inspect the service logs for coordinator/worker startup messages.")
        st.stop()


def record_to_dict(record):
    data = {field: getattr(record, field) for field in (
        "id", "task_type", "input_value", "status", "worker_id", "result", "error",
        "created_at", "started_at", "finished_at", "attempts", "duration",
    )}
    data["assignment_history"] = list(record.assignment_history)
    return data


def snapshot_to_data(snapshot):
    task_items = [record_to_dict(row) for row in snapshot.tasks]
    worker_items = []
    for row in snapshot.workers:
        worker_items.append({
            "worker_id": row.worker_id,
            "status": row.status,
            "address": row.address,
            "capacity": row.capacity,
            "active": row.active,
            "completed": row.completed,
            "failed": row.failed,
            "assignments_total": row.assignments_total,
            "last_seen": row.last_seen,
            "enabled": row.enabled,
        })
    return task_items, worker_items


st.set_page_config(page_title="Distributed Task Scheduler", page_icon="⚙️", layout="wide")
st.markdown("""
<style>
.block-container {padding-top: 1.3rem; max-width: 1500px;}
.hero {padding: 1.25rem 1.5rem; border: 1px solid rgba(128,128,128,.25); border-radius: 14px; margin-bottom: 1rem;}
.small-muted {color: #7b8490; font-size: .9rem;}
div[data-testid="stMetric"] {background: rgba(125,125,125,.05); padding: .65rem .8rem; border-radius: 10px;}
</style>
""", unsafe_allow_html=True)

st.markdown(
    '<div class="hero"><h1>Distributed Task Scheduler</h1>'
    '<p>Submit jobs, observe real gRPC dispatch to independent worker processes, and monitor dynamic load balancing.</p>'
    '<span class="small-muted">Streamlit client · gRPC coordinator · pending queue · worker registry · concurrent RPC execution · SQLite task history</span></div>',
    unsafe_allow_html=True,
)

channel, stub = get_rpc_client()
snapshot = call_rpc(stub.GetSnapshot, empty_pb2.Empty(), timeout=4)
tasks, workers = snapshot_to_data(snapshot)

with st.sidebar:
    st.title("⚙️ Control panel")
    st.caption("Actual processes detected by the scheduler")
    online_count = sum(worker["status"] == "Online" for worker in workers)
    st.metric("Registered workers online", f"{online_count} / {len(workers)}")
    if workers:
        st.write("**Worker capacities**")
        for worker in workers:
            st.caption(f"{worker['worker_id']}: {worker['capacity']} concurrent task(s)")
    st.info("Worker count/capacity are process settings, not pretend UI sliders. Change WORKER_COUNT or WORKER_CAPACITY in the cloud service settings and redeploy to change the real cluster.")
    st.divider()
    st.markdown("**Task types**")
    st.caption("Uppercase · Sum · Fibonacci · Sleep · Word count · Prime count")
    st.divider()
    auto_refresh = st.toggle("Auto-refresh dashboard", value=True)
    refresh_seconds = st.selectbox("Refresh interval", [1, 2, 3, 5, 10], index=2)

st.subheader("Submit a task batch")
with st.container(border=True):
    left, right = st.columns([1, 2])
    with left:
        task_type = st.selectbox("Task type", TASK_TYPES, index=3, format_func=lambda value: TASK_LABELS[value])
        batch_count = st.number_input("Number of identical tasks", min_value=1, max_value=100, value=12, step=1)
    with right:
        input_value = st.text_input(
            "Task input", value=TASK_DEFAULTS[task_type], key=f"input_{task_type}",
            help="For Sum, separate numbers with commas. For Sleep, use 0–30 seconds.",
        )
        st.caption(TASK_HELP[task_type])
    if st.button("▶ Submit task batch", type="primary", use_container_width=True):
        response = call_rpc(
            stub.SubmitBatch,
            task_pb2.BatchTaskRequest(task_type=task_type, input_value=input_value, count=int(batch_count)),
            timeout=8,
        )
        st.session_state["latest_batch_ids"] = list(response.task_ids)
        st.success(f"Queued {len(response.task_ids)} task(s): " + ", ".join(response.task_ids[:8]) + ("…" if len(response.task_ids) > 8 else ""))
        st.rerun()

# Re-snapshot after a task action so the page does not depend on the next auto-refresh.
snapshot = call_rpc(stub.GetSnapshot, empty_pb2.Empty(), timeout=4)
tasks, workers = snapshot_to_data(snapshot)

st.subheader("Cluster overview")
counts = Counter(task["status"] for task in tasks)
metric_cols = st.columns(5)
for col, label, value in zip(
    metric_cols,
    ["Queued", "Running", "Completed", "Failed", "Interrupted"],
    [counts["Queued"], counts["Running"], counts["Completed"], counts["Failed"], counts["Interrupted"]],
):
    col.metric(label, value)

if not workers:
    st.warning("No workers have registered yet. Check the cloud logs for worker start-up and registration messages.")

st.subheader("Worker cluster")
if workers:
    worker_cols = st.columns(min(3, len(workers)))
    for index, worker in enumerate(workers):
        with worker_cols[index % len(worker_cols)]:
            online = worker["status"] == "Online"
            disabled = worker["status"] == "Disabled"
            icon = "🟢" if online else ("🟡" if disabled else "⚪")
            with st.container(border=True):
                st.markdown(f"### {icon} {worker['worker_id']}")
                st.caption(f"{worker['status']} · {worker['address']}")
                capacity = max(1, worker["capacity"])
                st.progress(min(1.0, worker["active"] / capacity), text=f"Active load: {worker['active']} / {capacity} slots")
                a, b = st.columns(2)
                a.metric("Assignments", worker["assignments_total"])
                b.metric("Completed", worker["completed"])
                st.metric("Failed on this worker", worker["failed"])
                button_label = "Take out of rotation" if worker["enabled"] else "Re-enable worker"
                if st.button(button_label, key=f"toggle_{worker['worker_id']}", use_container_width=True):
                    response = call_rpc(
                        stub.SetWorkerEnabled,
                        task_pb2.WorkerControlRequest(worker_id=worker["worker_id"], enabled=not worker["enabled"]),
                        timeout=4,
                    )
                    if response.ok:
                        st.toast(response.message)
                    else:
                        st.error(response.message)
                    st.rerun()

st.subheader("Dynamic load-balancing evidence")
st.caption("The scheduler assigns queued tasks to the worker with the lowest normalized active load; a rotating tie-break prevents equal-load ties always favouring worker-1.")
latest_ids = set(st.session_state.get("latest_batch_ids", []))
latest_tasks = [task for task in tasks if task["id"] in latest_ids]
if latest_tasks:
    distribution = Counter(
        assigned_worker
        for task in latest_tasks
        for assigned_worker in task.get("assignment_history", [])
    )
    if workers:
        chart_data = pd.DataFrame({
            "Worker": [worker["worker_id"] for worker in workers],
            "Assignment attempts in latest batch": [distribution.get(worker["worker_id"], 0) for worker in workers],
        }).set_index("Worker")
        st.bar_chart(chart_data)
    st.write(f"**Last batch:** {len(latest_tasks)} tasks · {sum(distribution.values())} total assignment attempt(s)")
    if distribution:
        st.write(" · ".join(f"**{worker}:** {count}" for worker, count in sorted(distribution.items())))
    else:
        st.info("Tasks are queued but have not been assigned yet. Wait a moment for worker registration or a free slot.")
else:
    if workers:
        overall = pd.DataFrame({
            "Worker": [worker["worker_id"] for worker in workers],
            "Assignment attempts": [worker["assignments_total"] for worker in workers],
        }).set_index("Worker")
        st.bar_chart(overall)
    st.info("Submit a batch to record and chart the actual worker assignments. For an obvious live demonstration, select Sleep = 3 seconds and submit 12 tasks.")

st.subheader("Task history")
if tasks:
    df = pd.DataFrame(tasks)
    df["Assignment path"] = df["assignment_history"].map(lambda items: " → ".join(items) if items else "—")
    df["Submitted"] = df["created_at"].map(lambda value: datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S"))
    df["Duration"] = df["duration"].map(lambda value: f"{value:.3f}s" if value else "—")
    view = df[["id", "task_type", "input_value", "status", "worker_id", "Assignment path", "attempts", "Duration", "result", "error", "Submitted"]].copy()
    view = view.rename(columns={
        "id": "Task ID", "task_type": "Type", "input_value": "Input", "status": "Status",
        "worker_id": "Final worker", "attempts": "Attempts", "result": "Result", "error": "Error",
    })
    st.dataframe(view, use_container_width=True, hide_index=True, column_config={
        "Result": st.column_config.TextColumn(width="large"),
        "Error": st.column_config.TextColumn(width="medium"),
        "Assignment path": st.column_config.TextColumn(width="medium"),
        "Input": st.column_config.TextColumn(width="medium"),
    })
    retryable = [task for task in tasks if task["status"] in ("Failed", "Interrupted")]
    if retryable:
        task_ids = [task["id"] for task in retryable]
        chosen = st.selectbox(
            "Retry a failed/interrupted task", options=task_ids,
            format_func=lambda task_id: f"{task_id} · {next(task['task_type'] for task in retryable if task['id'] == task_id)}",
        )
        if st.button("↻ Retry selected task"):
            response = call_rpc(stub.RetryTask, task_pb2.RetryRequest(task_id=chosen), timeout=4)
            (st.success if response.ok else st.error)(response.message)
            st.rerun()
    export_df = df.drop(columns=["assignment_history"], errors="ignore")
    st.download_button(
        "Download task history (CSV)", data=export_df.to_csv(index=False).encode("utf-8"),
        file_name="task_history.csv", mime="text/csv",
    )
else:
    st.info("No tasks yet. Submit a task batch above to see task routing, results and execution timing.")

with st.expander("How the actual gRPC implementation works"):
    st.markdown("""
    1. **Client:** this Streamlit page calls the central scheduler through gRPC.
    2. **Queue:** `SubmitBatch` records every task before returning; the scheduler dispatcher drains the queue as worker capacity becomes available.
    3. **Dynamic load balancing:** the coordinator compares each worker's active/capacity ratio, active count, historical assignments and a rotating tie-break. That avoids all equal-load tasks being assigned to worker-1.
    4. **Independent workers:** every `worker.py` instance is a separate OS process exposing a gRPC `Execute` service. The coordinator makes a remote procedure call to the selected worker.
    5. **Heartbeat and recovery:** workers heartbeat to the coordinator. An RPC failure causes a bounded retry on an available worker. This is at-least-once recovery; exactly-once execution is not guaranteed.
    6. **Persistence:** SQLite stores task status, output, attempts and assignment history. The local container database may be lost on host redeploy/restart unless persistent storage is configured.

    **Deployment honesty:** this Docker version runs the dashboard, coordinator and each worker as separate processes in one hosted container. It genuinely uses gRPC between those processes, but it is not yet a multi-machine cluster and the container remains a single point of failure. The services can be split onto separate hosts later by setting `COORDINATOR_ADDRESS` and `WORKER_ADVERTISE_HOST` to private-network service addresses and using durable external storage.
    """)

st.caption("Distributed Task Scheduler · gRPC worker processes · Educational demo, not production-hardened")
if auto_refresh:
    time.sleep(int(refresh_seconds))
    st.rerun()
