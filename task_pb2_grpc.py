# gRPC stubs compatible with task.proto. Docker can replace this file with the
# canonical output of grpc_tools.protoc when building the image.
import grpc
import task_pb2 as task__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


class SchedulerServiceStub:
    def __init__(self, channel):
        self.SubmitBatch = channel.unary_unary(
            "/dts.SchedulerService/SubmitBatch",
            request_serializer=task__pb2.BatchTaskRequest.SerializeToString,
            response_deserializer=task__pb2.BatchTaskResponse.FromString,
        )
        self.GetSnapshot = channel.unary_unary(
            "/dts.SchedulerService/GetSnapshot",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=task__pb2.SnapshotResponse.FromString,
        )
        self.SetWorkerEnabled = channel.unary_unary(
            "/dts.SchedulerService/SetWorkerEnabled",
            request_serializer=task__pb2.WorkerControlRequest.SerializeToString,
            response_deserializer=task__pb2.ActionResponse.FromString,
        )
        self.RetryTask = channel.unary_unary(
            "/dts.SchedulerService/RetryTask",
            request_serializer=task__pb2.RetryRequest.SerializeToString,
            response_deserializer=task__pb2.ActionResponse.FromString,
        )
        self.RegisterWorker = channel.unary_unary(
            "/dts.SchedulerService/RegisterWorker",
            request_serializer=task__pb2.RegisterWorkerRequest.SerializeToString,
            response_deserializer=task__pb2.ActionResponse.FromString,
        )
        self.Heartbeat = channel.unary_unary(
            "/dts.SchedulerService/Heartbeat",
            request_serializer=task__pb2.HeartbeatRequest.SerializeToString,
            response_deserializer=task__pb2.ActionResponse.FromString,
        )


class SchedulerServiceServicer:
    def SubmitBatch(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")

    def GetSnapshot(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")

    def SetWorkerEnabled(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")

    def RetryTask(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")

    def RegisterWorker(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")

    def Heartbeat(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")


def add_SchedulerServiceServicer_to_server(servicer, server):
    handlers = {
        "SubmitBatch": grpc.unary_unary_rpc_method_handler(
            servicer.SubmitBatch, request_deserializer=task__pb2.BatchTaskRequest.FromString,
            response_serializer=task__pb2.BatchTaskResponse.SerializeToString,
        ),
        "GetSnapshot": grpc.unary_unary_rpc_method_handler(
            servicer.GetSnapshot, request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=task__pb2.SnapshotResponse.SerializeToString,
        ),
        "SetWorkerEnabled": grpc.unary_unary_rpc_method_handler(
            servicer.SetWorkerEnabled, request_deserializer=task__pb2.WorkerControlRequest.FromString,
            response_serializer=task__pb2.ActionResponse.SerializeToString,
        ),
        "RetryTask": grpc.unary_unary_rpc_method_handler(
            servicer.RetryTask, request_deserializer=task__pb2.RetryRequest.FromString,
            response_serializer=task__pb2.ActionResponse.SerializeToString,
        ),
        "RegisterWorker": grpc.unary_unary_rpc_method_handler(
            servicer.RegisterWorker, request_deserializer=task__pb2.RegisterWorkerRequest.FromString,
            response_serializer=task__pb2.ActionResponse.SerializeToString,
        ),
        "Heartbeat": grpc.unary_unary_rpc_method_handler(
            servicer.Heartbeat, request_deserializer=task__pb2.HeartbeatRequest.FromString,
            response_serializer=task__pb2.ActionResponse.SerializeToString,
        ),
    }
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("dts.SchedulerService", handlers),))


class WorkerServiceStub:
    def __init__(self, channel):
        self.Execute = channel.unary_unary(
            "/dts.WorkerService/Execute",
            request_serializer=task__pb2.ExecuteRequest.SerializeToString,
            response_deserializer=task__pb2.ExecuteResponse.FromString,
        )


class WorkerServiceServicer:
    def Execute(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented")


def add_WorkerServiceServicer_to_server(servicer, server):
    handlers = {
        "Execute": grpc.unary_unary_rpc_method_handler(
            servicer.Execute, request_deserializer=task__pb2.ExecuteRequest.FromString,
            response_serializer=task__pb2.ExecuteResponse.SerializeToString,
        )
    }
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("dts.WorkerService", handlers),))
