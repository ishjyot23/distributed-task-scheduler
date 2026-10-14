from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import grpc
from google.protobuf import empty_pb2

import task_pb2
import task_pb2_grpc

DB_PATH = Path(os.environ.get("TASK_DB_PATH", "data/tasks.db"))
HEARTBEAT_TIMEOUT_SECONDS = float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "12"))
MAX_ATTEMPTS = int(os.environ.get("MAX_TASK_ATTEMPTS", "3"))
RPC_TASK_TIMEOUT_SECONDS = float(os.environ.get("RPC_TASK_TIMEOUT_SECONDS", "45"))
VALID_TASK_TYPES = {"uppercase", "sum", "fibonacci", "sleep", "word_count", "prime_count"}


def _safe_json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
        return parsed if isinstance(parsed, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


class SchedulerService(task_pb2_grpc.SchedulerServiceServicer):
    """Central scheduler. Workers connect to it using gRPC registration/heartbeats.

    Work is dispatched over a separate gRPC channel to each worker process.
    A short scheduling loop fills available slots without waiting for previous
    tasks to complete, so batch submissions can occupy multiple worker nodes.
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.tasks: dict[str, dict[str, Any]] = {}
        self.workers: dict[str, dict[str, Any]] = {}
        self.round_robin_cursor = 0
        self.running = True
        self.rpc_pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="task-rpc")
        self._init_db()
        self._load_tasks()
        self.dispatcher = threading.Thread(target=self._dispatch_loop, name="scheduler-loop", daemon=True)
        self.dispatcher.start()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=15)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    input_value TEXT NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    result TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    duration REAL,
                    assignment_history TEXT NOT NULL DEFAULT '[]'
                )"""
            )
            # Allows a database created by the earlier single-process demo to be
            # opened without losing its task rows.
            columns = {row[1] for row in con.execute("PRAGMA table_info(tasks)").fetchall()}
            if "assignment_history" not in columns:
                con.execute("ALTER TABLE tasks ADD COLUMN assignment_history TEXT NOT NULL DEFAULT '[]'")
            con.commit()

    def _load_tasks(self) -> None:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT 1000").fetchall()
        for row in rows:
            item = dict(row)
            item["assignment_history"] = _safe_json_list(item.get("assignment_history"))
            # Re-queue tasks left Running at a coordinator restart. This is
            # at-least-once recovery: if the worker finished before the restart,
            # the same job may execute again because no distributed transaction exists.
            if item["status"] == "Running":
                item["status"] = "Queued"
                item["worker_id"] = None
                item["started_at"] = None
                item["error"] = "Coordinator restarted during execution; re-queued (duplicate execution is possible)."
                self._persist_task(item)
            self.tasks[item["id"]] = item

    def _persist_task(self, task: dict[str, Any]) -> None:
        with self._connect() as con:
            con.execute(
                """INSERT INTO tasks
                (id, task_type, input_value, status, worker_id, result, error,
                 created_at, started_at, finished_at, attempts, duration, assignment_history)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  task_type=excluded.task_type,
                  input_value=excluded.input_value,
                  status=excluded.status,
                  worker_id=excluded.worker_id,
                  result=excluded.result,
                  error=excluded.error,
                  started_at=excluded.started_at,
                  finished_at=excluded.finished_at,
                  attempts=excluded.attempts,
                  duration=excluded.duration,
                  assignment_history=excluded.assignment_history""",
                (
                    task["id"], task["task_type"], task["input_value"], task["status"],
                    task.get("worker_id"), task.get("result"), task.get("error"),
                    task["created_at"], task.get("started_at"), task.get("finished_at"),
                    task.get("attempts", 0), task.get("duration"),
                    json.dumps(task.get("assignment_history", [])),
                ),
            )
            con.commit()

    def RegisterWorker(self, request: task_pb2.RegisterWorkerRequest, context: grpc.ServicerContext) -> task_pb2.ActionResponse:
        if not request.worker_id or not request.address or request.capacity < 1:
            return task_pb2.ActionResponse(ok=False, message="Worker ID, address and positive capacity are required.")
        with self.condition:
            existing = self.workers.get(request.worker_id, {})
            histories = [
                wid for task in self.tasks.values()
                for wid in task.get("assignment_history", [])
                if wid == request.worker_id
            ]
            completed = sum(
                1 for task in self.tasks.values()
                if task.get("worker_id") == request.worker_id and task.get("status") == "Completed"
            )
            failed = sum(
                1 for task in self.tasks.values()
                if task.get("worker_id") == request.worker_id and task.get("status") == "Failed"
            )
            self.workers[request.worker_id] = {
                "worker_id": request.worker_id,
                "address": request.address,
                "capacity": int(request.capacity),
                "active": int(existing.get("active", 0)),
                "completed": completed,
                "failed": failed,
                "assignments_total": len(histories),
                "last_seen": time.time(),
                "enabled": bool(existing.get("enabled", True)),
                "reachable": True,
            }
            self.condition.notify_all()
        print(f"[scheduler] Registered {request.worker_id} at {request.address} (capacity={request.capacity})", flush=True)
        return task_pb2.ActionResponse(ok=True, message=f"Registered {request.worker_id}.")

    def Heartbeat(self, request: task_pb2.HeartbeatRequest, context: grpc.ServicerContext) -> task_pb2.ActionResponse:
        with self.condition:
            worker = self.workers.get(request.worker_id)
            if not worker:
                return task_pb2.ActionResponse(ok=False, message="Worker is not registered yet.")
            worker["last_seen"] = time.time()
            worker["reachable"] = True
            self.condition.notify_all()
        return task_pb2.ActionResponse(ok=True, message="Heartbeat received.")

    def SubmitBatch(self, request: task_pb2.BatchTaskRequest, context: grpc.ServicerContext) -> task_pb2.BatchTaskResponse:
        if request.task_type not in VALID_TASK_TYPES:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Unsupported task type: {request.task_type}")
        if request.count < 1 or request.count > 100:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Batch count must be between 1 and 100.")
        now = time.time()
        new_tasks = []
        with self.condition:
            # All jobs enter the queue in one locked transaction. The dispatcher
            # can then fill available slots across all workers before tiny tasks
            # have time to finish and bias assignments toward worker-1.
            for _ in range(request.count):
                task_id = uuid.uuid4().hex[:12]
                item = {
                    "id": task_id,
                    "task_type": request.task_type,
                    "input_value": request.input_value,
                    "status": "Queued",
                    "worker_id": None,
                    "result": None,
                    "error": None,
                    "created_at": now,
                    "started_at": None,
                    "finished_at": None,
                    "attempts": 0,
                    "duration": None,
                    "assignment_history": [],
                }
                self.tasks[task_id] = item
                self._persist_task(item)
                new_tasks.append(task_id)
            self.condition.notify_all()
        print(f"[scheduler] Queued {len(new_tasks)} {request.task_type} task(s)", flush=True)
        return task_pb2.BatchTaskResponse(task_ids=new_tasks)

    def SetWorkerEnabled(self, request: task_pb2.WorkerControlRequest, context: grpc.ServicerContext) -> task_pb2.ActionResponse:
        with self.condition:
            worker = self.workers.get(request.worker_id)
            if not worker:
                return task_pb2.ActionResponse(ok=False, message="Worker is not registered.")
            worker["enabled"] = bool(request.enabled)
            self.condition.notify_all()
        state = "enabled for scheduling" if request.enabled else "disabled for new assignments"
        print(f"[scheduler] {request.worker_id} {state}", flush=True)
        return task_pb2.ActionResponse(ok=True, message=f"{request.worker_id} is {state}.")

    def RetryTask(self, request: task_pb2.RetryRequest, context: grpc.ServicerContext) -> task_pb2.ActionResponse:
        with self.condition:
            task = self.tasks.get(request.task_id)
            if not task:
                return task_pb2.ActionResponse(ok=False, message="Task was not found.")
            if task["status"] not in ("Failed", "Interrupted"):
                return task_pb2.ActionResponse(ok=False, message="Only failed or interrupted tasks can be retried manually.")
            task.update(
                status="Queued", worker_id=None, result=None, error=None,
                created_at=time.time(), started_at=None, finished_at=None,
                attempts=0, duration=None,
            )
            self._persist_task(task)
            self.condition.notify_all()
        return task_pb2.ActionResponse(ok=True, message=f"Task {request.task_id} queued for retry.")

    def GetSnapshot(self, request: empty_pb2.Empty, context: grpc.ServicerContext) -> task_pb2.SnapshotResponse:
        with self.condition:
            self._refresh_liveness_locked()
            ordered_tasks = sorted(self.tasks.values(), key=lambda item: item["created_at"], reverse=True)
            task_rows = []
            for item in ordered_tasks[:1000]:
                task_rows.append(task_pb2.TaskRecord(
                    id=str(item["id"]), task_type=str(item["task_type"]), input_value=str(item["input_value"]),
                    status=str(item["status"]), worker_id=str(item.get("worker_id") or ""),
                    result=str(item.get("result") or ""), error=str(item.get("error") or ""),
                    created_at=float(item.get("created_at") or 0), started_at=float(item.get("started_at") or 0),
                    finished_at=float(item.get("finished_at") or 0), attempts=int(item.get("attempts") or 0),
                    duration=float(item.get("duration") or 0),
                    assignment_history=item.get("assignment_history", []),
                ))
            worker_rows = []
            for wid, item in sorted(self.workers.items()):
                status = "Online" if item["reachable"] and item["enabled"] else ("Disabled" if item["reachable"] else "Offline")
                worker_rows.append(task_pb2.WorkerRecord(
                    worker_id=wid, status=status, address=item["address"], capacity=item["capacity"],
                    active=item["active"], completed=item["completed"], failed=item["failed"],
                    assignments_total=item["assignments_total"], last_seen=item["last_seen"], enabled=item["enabled"],
                ))
            counts = {name: sum(1 for t in self.tasks.values() if t["status"] == name)
                      for name in ("Queued", "Running", "Completed", "Failed", "Interrupted")}
            return task_pb2.SnapshotResponse(
                tasks=task_rows, workers=worker_rows,
                scheduler_status="Online" if self.running else "Stopping",
                queued=counts["Queued"], running=counts["Running"], completed=counts["Completed"],
                failed=counts["Failed"], interrupted=counts["Interrupted"],
            )

    def _refresh_liveness_locked(self) -> None:
        now = time.time()
        for worker in self.workers.values():
            if now - worker["last_seen"] > HEARTBEAT_TIMEOUT_SECONDS:
                worker["reachable"] = False

    def _is_eligible(self, worker: dict[str, Any], now: float) -> bool:
        if now - worker["last_seen"] > HEARTBEAT_TIMEOUT_SECONDS:
            worker["reachable"] = False
        return bool(worker["reachable"] and worker["enabled"] and worker["active"] < worker["capacity"])

    def _dispatch_loop(self) -> None:
        while self.running:
            with self.condition:
                self._refresh_liveness_locked()
                queued = sorted(
                    (task for task in self.tasks.values() if task["status"] == "Queued"),
                    key=lambda task: (task["created_at"], task["id"]),
                )
                for task in queued:
                    now = time.time()
                    eligible = [w for w in self.workers.values() if self._is_eligible(w, now)]
                    if not eligible:
                        break
                    worker = self._select_worker_locked(eligible)
                    self._assign_locked(task, worker)
                self.condition.wait(timeout=0.08)

    def _select_worker_locked(self, eligible: list[dict[str, Any]]) -> dict[str, Any]:
        # Least utilization first; use historical assignment count and a rotating
        # tie-break to prevent equal-load tasks always choosing worker-1.
        all_ids = sorted(self.workers)
        n = max(1, len(all_ids))
        position = {wid: index for index, wid in enumerate(all_ids)}
        def key(worker: dict[str, Any]) -> tuple[float, int, int, int]:
            wid = worker["worker_id"]
            distance = (position.get(wid, 0) - self.round_robin_cursor) % n
            return (
                worker["active"] / max(worker["capacity"], 1),
                worker["active"],
                worker["assignments_total"],
                distance,
            )
        chosen = min(eligible, key=key)
        self.round_robin_cursor = (position.get(chosen["worker_id"], 0) + 1) % n
        return chosen

    def _assign_locked(self, task: dict[str, Any], worker: dict[str, Any]) -> None:
        wid = worker["worker_id"]
        task.update(status="Running", worker_id=wid, started_at=time.time(), finished_at=None, error=None)
        task["attempts"] = int(task.get("attempts", 0)) + 1
        task.setdefault("assignment_history", []).append(wid)
        worker["active"] += 1
        worker["assignments_total"] += 1
        self._persist_task(task)
        request = task_pb2.ExecuteRequest(task_id=task["id"], task_type=task["task_type"], input_value=task["input_value"])
        future = self.rpc_pool.submit(self._execute_remote, worker["address"], request)
        future.add_done_callback(lambda completed, task_id=task["id"], worker_id=wid: self._on_task_done(task_id, worker_id, completed))
        print(f"[scheduler] Assigned task {task['id']} -> {wid}; active={worker['active']}/{worker['capacity']}", flush=True)

    @staticmethod
    def _execute_remote(address: str, request: task_pb2.ExecuteRequest) -> task_pb2.ExecuteResponse:
        with grpc.insecure_channel(address) as channel:
            stub = task_pb2_grpc.WorkerServiceStub(channel)
            return stub.Execute(request, timeout=RPC_TASK_TIMEOUT_SECONDS)

    def _on_task_done(self, task_id: str, worker_id: str, future: Any) -> None:
        rpc_error: str | None = None
        response: task_pb2.ExecuteResponse | None = None
        try:
            response = future.result()
        except Exception as exc:  # network failure, deadline exceeded, worker process crashed, etc.
            rpc_error = f"Worker RPC failed: {type(exc).__name__}: {exc}"

        with self.condition:
            worker = self.workers.get(worker_id)
            if worker:
                worker["active"] = max(0, worker["active"] - 1)
            task = self.tasks.get(task_id)
            if not task or task["status"] != "Running" or task.get("worker_id") != worker_id:
                self.condition.notify_all()
                return
            finished = time.time()
            started = float(task.get("started_at") or finished)
            task["finished_at"] = finished
            task["duration"] = round(max(0.0, finished - started), 4)

            if rpc_error is not None:
                if worker:
                    worker["reachable"] = False
                    worker["last_seen"] = min(worker["last_seen"], finished - HEARTBEAT_TIMEOUT_SECONDS - 1)
                task["error"] = rpc_error[:1000]
                task["result"] = None
                task["worker_id"] = None
                if int(task.get("attempts", 0)) < MAX_ATTEMPTS:
                    task["status"] = "Queued"
                    print(f"[scheduler] Re-queued task {task_id} after worker RPC failure (attempt {task['attempts']})", flush=True)
                else:
                    task["status"] = "Failed"
                    if worker:
                        worker["failed"] += 1
            elif response is not None and response.success:
                task.update(status="Completed", result=response.result, error=None, worker_id=worker_id)
                if worker:
                    worker["completed"] += 1
                print(f"[scheduler] Completed task {task_id} on {worker_id} in {response.duration:.3f}s", flush=True)
            else:
                task.update(status="Failed", result=None, error=(response.error if response else "Unknown worker error"), worker_id=worker_id)
                if worker:
                    worker["failed"] += 1
                print(f"[scheduler] Task {task_id} failed on {worker_id}: {task.get('error')}", flush=True)
            self._persist_task(task)
            self.condition.notify_all()

    def stop(self) -> None:
        self.running = False
        with self.condition:
            self.condition.notify_all()
        self.rpc_pool.shutdown(wait=False, cancel_futures=True)


def serve(host: str, port: int) -> None:
    service = SchedulerService()
    server = grpc.server(ThreadPoolExecutor(max_workers=64), options=[("grpc.so_reuseport", 0)])
    task_pb2_grpc.add_SchedulerServiceServicer_to_server(service, server)
    bind_address = f"{host}:{port}"
    bound_port = server.add_insecure_port(bind_address)
    if not bound_port:
        raise RuntimeError(f"Could not bind gRPC scheduler at {bind_address}")
    server.start()
    print(f"[scheduler] gRPC service listening on {bind_address}; database={service.db_path}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        service.stop()
        server.stop(grace=2).wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed task scheduler gRPC coordinator")
    parser.add_argument("--host", default=os.environ.get("SCHEDULER_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SCHEDULER_PORT", "50051")))
    args = parser.parse_args()
    serve(args.host, args.port)
