"""Integration smoke test for the local gRPC scheduler and two worker processes.

Run from the project root after `python -m grpc_tools.protoc ...`:
    python tests/smoke_test.py
"""
from __future__ import annotations
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import grpc
from google.protobuf import empty_pb2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
PYTHON = sys.executable


def wait_ready(address: str, timeout: float = 20.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=0.5)
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"Service at {address} did not become ready")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="dts-smoke-") as temp:
        env = os.environ.copy()
        env["TASK_DB_PATH"] = str(Path(temp) / "tasks.db")
        env["COORDINATOR_ADDRESS"] = "127.0.0.1:55051"
        coordinator = subprocess.Popen(
            [PYTHON, "coordinator.py", "--host", "127.0.0.1", "--port", "55051"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        processes: list[subprocess.Popen] = []
        try:
            wait_ready("127.0.0.1:55051")
            for index, port in enumerate((55061, 55062, 55063), start=1):
                worker_env = env.copy()
                worker_env["WORKER_ADVERTISE_HOST"] = "127.0.0.1"
                processes.append(subprocess.Popen(
                    [PYTHON, "worker.py", "--id", f"worker-{index}", "--port", str(port), "--capacity", "2"],
                    cwd=ROOT, env=worker_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                ))
            time.sleep(2.5)
            with grpc.insecure_channel("127.0.0.1:55051") as channel:
                stub = __import__("task_pb2_grpc").SchedulerServiceStub(channel)
                snapshot = stub.GetSnapshot(empty_pb2.Empty(), timeout=3)
                online = [worker for worker in snapshot.workers if worker.status == "Online"]
                assert len(online) == 3, f"Expected 3 online workers, got {[w.status for w in snapshot.workers]}"
                batch = stub.SubmitBatch(
                    __import__("task_pb2").BatchTaskRequest(task_type="uppercase", input_value="ishjyot", count=12),
                    timeout=4,
                )
                assert len(batch.task_ids) == 12
                deadline = time.time() + 15
                records = {}
                while time.time() < deadline:
                    snap = stub.GetSnapshot(empty_pb2.Empty(), timeout=3)
                    records = {task.id: task for task in snap.tasks if task.id in set(batch.task_ids)}
                    if len(records) == 12 and all(task.status == "Completed" for task in records.values()):
                        break
                    time.sleep(0.2)
                assert len(records) == 12, f"Expected 12 records, got {len(records)}"
                assert all(task.status == "Completed" for task in records.values()), "Not all tasks completed"
                assigned = {wid for task in records.values() for wid in task.assignment_history}
                assert len(assigned) >= 2, f"Load-balancing check expected 2+ workers, got {assigned}"
                per_worker = {worker.worker_id: 0 for worker in snap.workers}
                for task in records.values():
                    for wid in task.assignment_history:
                        per_worker[wid] = per_worker.get(wid, 0) + 1
                print("PASS: 12 tasks completed over gRPC.")
                print("Assignment attempts:", per_worker)
                print("Worker IDs used:", sorted(assigned))

                # Six 1-second tasks should run concurrently in one wave with 3x2 slots.
                import task_pb2
                start = time.time()
                sleep_batch = stub.SubmitBatch(
                    task_pb2.BatchTaskRequest(task_type="sleep", input_value="1", count=6), timeout=4
                )
                sleep_ids = set(sleep_batch.task_ids)
                deadline = time.time() + 12
                sleep_records = {}
                while time.time() < deadline:
                    snap = stub.GetSnapshot(empty_pb2.Empty(), timeout=3)
                    sleep_records = {task.id: task for task in snap.tasks if task.id in sleep_ids}
                    if len(sleep_records) == 6 and all(task.status == "Completed" for task in sleep_records.values()):
                        break
                    time.sleep(0.1)
                elapsed = time.time() - start
                assert len(sleep_records) == 6 and all(t.status == "Completed" for t in sleep_records.values())
                assert elapsed < 4.0, f"Concurrency smoke test took {elapsed:.2f}s; expected one ~1s wave plus overhead"
                print(f"PASS: 6 one-second tasks completed concurrently in {elapsed:.2f}s.")

                # Verify worker exclusion: disabled nodes must receive no new work.
                import task_pb2
                control = stub.SetWorkerEnabled(
                    task_pb2.WorkerControlRequest(worker_id="worker-2", enabled=False), timeout=3
                )
                assert control.ok, control.message
                second = stub.SubmitBatch(
                    task_pb2.BatchTaskRequest(task_type="uppercase", input_value="excluded-worker-check", count=6),
                    timeout=4,
                )
                second_ids = set(second.task_ids)
                deadline = time.time() + 15
                second_records = {}
                while time.time() < deadline:
                    snap = stub.GetSnapshot(empty_pb2.Empty(), timeout=3)
                    second_records = {task.id: task for task in snap.tasks if task.id in second_ids}
                    if len(second_records) == 6 and all(task.status == "Completed" for task in second_records.values()):
                        break
                    time.sleep(0.2)
                assert len(second_records) == 6 and all(t.status == "Completed" for t in second_records.values())
                assert all("worker-2" not in t.assignment_history for t in second_records.values()),                     "Disabled worker received a new task"
                assert stub.SetWorkerEnabled(task_pb2.WorkerControlRequest(worker_id="worker-2", enabled=True), timeout=3).ok
                print("PASS: disabled worker was excluded from all six tasks.")
                return 0
        finally:
            for process in processes:
                process.terminate()
            coordinator.terminate()
            for process in processes + [coordinator]:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
