import os
import socket
import sys
import traceback
from pathlib import Path
from typing import Protocol

import msgspec

from .wire_format import Initialized, JobCompleted, ResultType, Submit, Unhandled, WireFromWorker, to_wire

# ruff: noqa: D103, ANN001, BLE001


class HandlerProtocol[TaskSpec, Result, RunInfo](Protocol):
    TS: type[TaskSpec]
    R: type[Result]
    RAI: type[RunInfo]

    def handle(self, job: Submit[TaskSpec, Result, RunInfo]) -> ResultType[TaskSpec, Result, RunInfo]: ...


# If we never need to inject anything, but we actualy prefer the user to import instead of using such namespace
FRAMEWORK_NAMESPACE = {}


def connect_uds(path: str) -> socket.socket:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    return sock


def enc_hook(obj):
    if isinstance(obj, Path):
        return str(obj)
    raise Exception(f"Serialization of type {type(obj)} is not handled. Obj is {obj}")


ENCODER = msgspec.json.Encoder(enc_hook=enc_hook, order="sorted")


def send_message(sock: socket.socket, msg: WireFromWorker) -> None:
    sys.stderr.flush()
    sys.stdout.flush()
    encoded = ENCODER.encode(to_wire(msg)) + b"\n"
    sock.sendall(encoded)


def load_handler(path: Path) -> HandlerProtocol:
    """Load a stepfile, injecting framework symbols into its namespace."""
    source = path.read_text()
    namespace = dict(FRAMEWORK_NAMESPACE)
    namespace["__file__"] = path
    namespace["__name__"] = path.name
    exec(compile(source, path, "exec"), namespace)  # noqa: S102
    return namespace["handle"]


def main() -> None:
    uds_path = os.environ["WORKER_UDS"]
    handler_file = Path(sys.argv[1])
    handler = load_handler(handler_file)
    sock = connect_uds(uds_path)
    send_message(sock, Initialized())
    decoder = msgspec.json.Decoder(type=Submit[handler.TS, handler.R, handler.RAI])

    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        parsed_batch = decoder.decode_lines(chunk)
        if len(parsed_batch) == 1:
            job = parsed_batch[0]
        elif len(parsed_batch) == 0:
            continue  # The msgspec decoder retains the previous data
        else:
            # The runner never sends a new message before the previous one have been answered.
            raise Exception("Problem, more than one job is not possible")
        try:
            res = handler.handle(job)
        except Exception as e:
            to_send: WireFromWorker = JobCompleted(Unhandled())
            traceback.print_exception(e)
        else:
            to_send = JobCompleted(res)
        send_message(sock, to_send)
