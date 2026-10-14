# Generated-compatible Protocol Buffer definitions for task.proto.
# Docker deployments regenerate this module from task.proto using grpc_tools.protoc.
# This checked-in fallback lets the sample run without a local protoc executable.
from google.protobuf import descriptor_pb2, descriptor_pool, empty_pb2  # noqa: F401
from google.protobuf.internal import builder as _builder

_fd = descriptor_pb2.FileDescriptorProto()
_fd.name = "task.proto"
_fd.package = "dts"
_fd.syntax = "proto3"
_fd.dependency.append("google/protobuf/empty.proto")

_T = descriptor_pb2.FieldDescriptorProto


def _message(name, fields):
    msg = _fd.message_type.add()
    msg.name = name
    for field_name, number, field_type, repeated in fields:
        field = msg.field.add()
        field.name = field_name
        field.number = number
        field.label = _T.LABEL_REPEATED if repeated else _T.LABEL_OPTIONAL
        field.type = field_type
        if field_type == _T.TYPE_MESSAGE and field_name == "_empty_placeholder":
            field.type_name = ".google.protobuf.Empty"
    return msg


def _f(name, number, kind, repeated=False, type_name=None):
    return (name, number, kind, repeated) if type_name is None else (name, number, kind, repeated, type_name)


def _add_message(name, fields):
    msg = _fd.message_type.add()
    msg.name = name
    for spec in fields:
        field_name, number, field_type, repeated, *rest = spec
        field = msg.field.add()
        field.name = field_name
        field.number = number
        field.label = _T.LABEL_REPEATED if repeated else _T.LABEL_OPTIONAL
        field.type = field_type
        if rest:
            field.type_name = rest[0]


_string = _T.TYPE_STRING
_int32 = _T.TYPE_INT32
_double = _T.TYPE_DOUBLE
_bool = _T.TYPE_BOOL
_message_type = _T.TYPE_MESSAGE

_add_message("BatchTaskRequest", [
    _f("task_type", 1, _string), _f("input_value", 2, _string), _f("count", 3, _int32),
])
_add_message("BatchTaskResponse", [_f("task_ids", 1, _string, True)])
_add_message("TaskRecord", [
    _f("id", 1, _string), _f("task_type", 2, _string), _f("input_value", 3, _string),
    _f("status", 4, _string), _f("worker_id", 5, _string), _f("result", 6, _string),
    _f("error", 7, _string), _f("created_at", 8, _double), _f("started_at", 9, _double),
    _f("finished_at", 10, _double), _f("attempts", 11, _int32), _f("duration", 12, _double),
    _f("assignment_history", 13, _string, True),
])
_add_message("WorkerRecord", [
    _f("worker_id", 1, _string), _f("status", 2, _string), _f("address", 3, _string),
    _f("capacity", 4, _int32), _f("active", 5, _int32), _f("completed", 6, _int32),
    _f("failed", 7, _int32), _f("assignments_total", 8, _int32), _f("last_seen", 9, _double),
    _f("enabled", 10, _bool),
])
_add_message("SnapshotResponse", [
    _f("tasks", 1, _message_type, True, ".dts.TaskRecord"),
    _f("workers", 2, _message_type, True, ".dts.WorkerRecord"),
    _f("scheduler_status", 3, _string), _f("queued", 4, _int32),
    _f("running", 5, _int32), _f("completed", 6, _int32), _f("failed", 7, _int32),
    _f("interrupted", 8, _int32),
])
_add_message("WorkerControlRequest", [_f("worker_id", 1, _string), _f("enabled", 2, _bool)])
_add_message("RetryRequest", [_f("task_id", 1, _string)])
_add_message("ActionResponse", [_f("ok", 1, _bool), _f("message", 2, _string)])
_add_message("RegisterWorkerRequest", [
    _f("worker_id", 1, _string), _f("address", 2, _string), _f("capacity", 3, _int32),
])
_add_message("HeartbeatRequest", [_f("worker_id", 1, _string), _f("completed", 2, _int32), _f("failed", 3, _int32)])
_add_message("ExecuteRequest", [_f("task_id", 1, _string), _f("task_type", 2, _string), _f("input_value", 3, _string)])
_add_message("ExecuteResponse", [
    _f("task_id", 1, _string), _f("worker_id", 2, _string), _f("success", 3, _bool),
    _f("result", 4, _string), _f("error", 5, _string), _f("duration", 6, _double),
])

_service = _fd.service.add()
_service.name = "SchedulerService"
for name, inp, out in [
    ("SubmitBatch", "BatchTaskRequest", "BatchTaskResponse"),
    ("GetSnapshot", ".google.protobuf.Empty", "SnapshotResponse"),
    ("SetWorkerEnabled", "WorkerControlRequest", "ActionResponse"),
    ("RetryTask", "RetryRequest", "ActionResponse"),
    ("RegisterWorker", "RegisterWorkerRequest", "ActionResponse"),
    ("Heartbeat", "HeartbeatRequest", "ActionResponse"),
]:
    method = _service.method.add()
    method.name = name
    method.input_type = inp if inp.startswith(".") else f".dts.{inp}"
    method.output_type = out if out.startswith(".") else f".dts.{out}"

_service = _fd.service.add()
_service.name = "WorkerService"
_method = _service.method.add()
_method.name = "Execute"
_method.input_type = ".dts.ExecuteRequest"
_method.output_type = ".dts.ExecuteResponse"

DESCRIPTOR = descriptor_pool.Default().AddSerializedFile(_fd.SerializeToString())
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, __name__, globals())
