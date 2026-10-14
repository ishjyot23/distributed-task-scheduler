from __future__ import annotations

import argparse
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import grpc

import task_pb2
import task_pb2_grpc

COORDINATOR_ADDRESS = os.environ.get("COORDINATOR_ADDRESS", "127.0.0.1:50051")
HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "2"))


def run_task(task_type: str, raw: str) -> Any:
    if task_type == "uppercase":
        return raw.upper()
    if task_type == "sum":
        numbers = [float(part.strip()) for part in raw.split(",") if part.strip()]
        if not numbers:
            raise ValueError("Enter comma-separated numbers, for example 10,20,30.")
        total = sum(numbers)
        return int(total) if total.is_integer() else total
    if task_type == "fibonacci":
        n = int(raw)
        if not 0 <= n <= 5000:
            raise ValueError("Choose Fibonacci n between 0 and 5000.")
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a
    if task_type == "sleep":
        seconds = float(raw)
        if not 0 <= seconds <= 30:
            raise ValueError("Sleep duration must be between 0 and 30 seconds.")
        time.sleep(seconds)
        return f"Finished simulated workload after {seconds:g} seconds"
    if task_type == "word_count":
        return {"words": len(raw.split()), "characters": len(raw)}
    if task_type == "prime_count":
        n = int(raw)
        if not 2 <= n <= 200_000:
            raise ValueError("Choose an upper limit between 2 and 200000.")
        sieve = bytearray(b"\x01") * (n + 1)
        sieve[:2] = b"\x00\x00"
        for p in range(2, math.isqrt(n) + 1):
            if sieve[p]:
                start = p * p
                sieve[start:n + 1:p] = b"\x00" * (((n - start) // p) + 1)
        return sum(sieve)
    raise ValueError(f"Unknown task type: {task_type}")


class WorkerService(task_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.lock = threading.Lock()
        self.active = 0
        self.completed = 0
        self.failed = 0

    def Execute(self, request: task_pb2.ExecuteRequest, context: grpc.ServicerContext) -> task_pb2.ExecuteResponse:
        started = time.time()
        with self.lock:
            self.active += 1
        success = False
        result_text = ""
        error = ""
        try:
            result = run_task(request.task_type, request.input_value)
            result_text = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
            success, error = True, ""
        except Exception as exc:
            result_text, success, error = "", False, f"{type(exc).__name__}: {exc}"
        finally:
            duration = round(time.time() - started, 4)
            with self.lock:
                self.active = max(0, self.active - 1)
                if success:
                    self.completed += 1
                else:
                    self.failed += 1
        print(
            f"[{self.worker_id}] task={request.task_id} type={request.task_type} "
            f"success={success} duration={duration:.3f}s",
            flush=True,
        )
        return task_pb2.ExecuteResponse(
            task_id=request.task_id,
            worker_id=self.worker_id,
            success=success,
            result=result_text,
            error=error,
            duration=duration,
        )


def heartbeat_loop(worker_id: str, address: str, capacity: int, stop_event: threading.Event) -> None:
    with grpc.insecure_channel(COORDINATOR_ADDRESS) as channel:
        stub = task_pb2_grpc.SchedulerServiceStub(channel)
        registered = False
        while not stop_event.is_set():
            try:
                if not registered:
                    response = stub.RegisterWorker(
                        task_pb2.RegisterWorkerRequest(worker_id=worker_id, address=address, capacity=capacity),
                        timeout=3,
                    )
                    registered = response.ok
                    if registered:
                        print(f"[{worker_id}] registered with coordinator {COORDINATOR_ADDRESS}", flush=True)
                    else:
                        print(f"[{worker_id}] registration rejected: {response.message}", flush=True)
                else:
                    response = stub.Heartbeat(task_pb2.HeartbeatRequest(worker_id=worker_id), timeout=3)
                    registered = response.ok
            except grpc.RpcError as exc:
                registered = False
                print(f"[{worker_id}] coordinator unavailable; will retry ({exc.code().name})", flush=True)
            stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)


def serve(worker_id: str, host: str, port: int, capacity: int) -> None:
    if capacity < 1:
        raise ValueError("Worker capacity must be positive.")
    bind_address = f"{host}:{port}"
    service = WorkerService(worker_id)
    server = grpc.server(ThreadPoolExecutor(max_workers=capacity, thread_name_prefix=worker_id))
    task_pb2_grpc.add_WorkerServiceServicer_to_server(service, server)
    if not server.add_insecure_port(bind_address):
        raise RuntimeError(f"Could not bind {worker_id} to {bind_address}")
    server.start()

    advertise_host = os.environ.get("WORKER_ADVERTISE_HOST", "127.0.0.1")
    advertise_address = f"{advertise_host}:{port}"
    stop_event = threading.Event()
    heartbeat = threading.Thread(
        target=heartbeat_loop,
        args=(worker_id, advertise_address, capacity, stop_event),
        name=f"{worker_id}-heartbeat",
        daemon=True,
    )
    heartbeat.start()
    print(f"[{worker_id}] gRPC worker listening on {bind_address}; capacity={capacity}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.stop(grace=2).wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed task scheduler worker service")
    parser.add_argument("--id", required=True, help="Unique worker identifier, e.g. worker-1")
    parser.add_argument("--host", default="0.0.0.0", help="Address to bind worker gRPC server")
    parser.add_argument("--port", type=int, required=True, help="Worker gRPC port")
    parser.add_argument("--capacity", type=int, default=2, help="Maximum simultaneous RPC jobs")
    args = parser.parse_args()
    serve(args.id, args.host, args.port, args.capacity)
