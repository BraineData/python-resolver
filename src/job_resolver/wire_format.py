"""protocol.py — Python (worker) side of the process_worker wire protocol.

Mirrors the Rust `wire_format.rs`. Messages are newline-delimited JSON over the
Unix domain socket: the worker decodes one `WireToWorker` per line and encodes
one `WireFromWorker` per line.

Four opaque JSON payloads flow through the protocol. Three are domain types you
parameterise with your own msgspec types (or leave as `msgspec.Raw` to stay
fully dynamic): the generic parameters below.

    TaskSpec   the spec submitted for a job        (Rust Domain::TaskSpec)
    Result     a job's result value                (Rust Domain::Result)
    RunInfo    info attached to a run              (Rust Domain::RunAttachedInfo)

The fourth, `custom`, is always kept as raw JSON (`msgspec.Raw`).

Why the two directions are tagged differently
---------------------------------------------
Inbound (WireToWorker, WireJobResult): Rust *serializes* these with internal
tags (`#[serde(tag = "type")]`). Internally-tagged *serialization* of a
`RawValue` struct-variant is fine, and msgspec decodes internal tags natively,
so these are plain msgspec tagged unions — nothing special needed.

Outbound (WireFromWorker, WireWorkerResult): Rust *deserializes* these, and they
carry `Box<RawValue>`. serde's internally-tagged, adjacently-tagged AND untagged
representations all buffer the content into an intermediate value before
dispatching, and a `RawValue` cannot be reconstructed from that buffer (it needs
the original bytes). Only serde's *externally* tagged form — the default,
`{"variant": {..}}` — deserializes `RawValue`. So the worker must emit
externally-tagged JSON here. msgspec only emits internal tags, so these variants
are tagless structs and `to_wire()` adds the external key. (Note: tagless is NOT
enough on its own — an untagged serde enum would also fail on `RawValue`; the
key is required.)
"""

import enum
import os
from pathlib import Path
from typing import Any

from msgspec import Struct

from .utils import Location

RUN_DB_PATH = Path(os.environ["RUN_DB"])

# --- primitives borrowed from crate::common (adjust if these differ) -------

type RunId = int  # crate::common::RunId — assumed integer id
type SubdagIndex = int  # crate::common::SubdagIndex — assumed integer index


# --- shared payload structs (types.rs) -------------------------------------


class WorkerRunData(Struct):
    run_id: RunId
    base_location: str
    inner_cwd: str
    exit_code: int


class WorkerRunSpec(Struct):
    program: str
    args: list[str]
    base_location: str
    env: list[tuple[str, str]]
    params: Any
    priority: int = 10


# ===========================================================================
# Inbound: WireToWorker  (worker DECODES)        internally tagged on "type"
# ===========================================================================


class ResultSpec[Result](Struct):
    failed: bool
    data: Result | None  # None when Failed, we use a struct rather than an enum so that deserialization is easy


class SimpleJob[Result](Struct, tag="simple_job"):
    res: ResultSpec[Result]


class RunJob[RunInfo](Struct, tag="run_job"):
    info: RunInfo
    data: WorkerRunData

    def as_path(self, item: str | None = None) -> Path:
        if item is None:
            return Path(os.environ[self.data.base_location]) / self.data.inner_cwd
        return Path(os.environ[self.data.base_location]) / self.data.inner_cwd / self.info[item]  # pyright: ignore[reportIndexIssue]

    def as_location(self, item: str | None = None) -> Location:
        if item is None:
            return Location(self.data.base_location, Path(self.data.inner_cwd))
        return Location(self.data.base_location, Path(self.data.inner_cwd) / self.info[item])  # pyright: ignore[reportIndexIssue]


type WireJobResult[Result, RunInfo] = SimpleJob[Result] | RunJob[RunInfo]


class Submit[TaskSpec, Result, RunInfo](Struct):
    task_spec: TaskSpec
    dep_results: list[WireJobResult[Result, RunInfo]]


# ===========================================================================
# Outbound: WireFromWorker  (worker ENCODES)     externally tagged (tagless
# structs + to_wire(); see module docstring)
# ===========================================================================


class WireReason(enum.Enum):
    JOB_ISSUE = "job_issue"
    WORKER_ISSUE = "worker_issue"
    CANCELLED = "cancelled"
    UNSUPPORTED_VERSION = "unsupported_version"
    UNKNOWN = "unknown"


# --- WireWorkerResult variants (tagless; to_wire wraps them) ----------------


class Unhandled(Struct):
    pass


class Computed[Result](Struct):  # Rust WireWorkerResult::Result
    res: ResultSpec[Result]


class WireSubdagTaskSpec[TaskSpec](Struct):
    spec: TaskSpec
    deps: list[SubdagIndex]


class Subdag[TaskSpec](Struct):
    task_specs: list[WireSubdagTaskSpec[TaskSpec]]
    output_node: SubdagIndex
    tags: list[str]


class NewRun[RunInfo](Struct):
    cmd: WorkerRunSpec
    after_run_res: RunInfo


class ExistingRun[RunInfo](Struct):
    run_id: RunId
    res: RunInfo


# Type alias for hints only — do NOT hand this to a msgspec Decoder (these are
# encode-only and tagless). Decoding happens on the Rust side.
type WireWorkerResult[TaskSpec, Result, RunInfo] = Unhandled | Computed[Result] | Subdag[TaskSpec] | NewRun[RunInfo] | ExistingRun[RunInfo]


# --- WireFromWorker variants (tagless) -------------------------------------


class Initialized(Struct):
    custom: Any | None = None


type ResultType[TaskSpec, Result, RunInfo] = Computed[Result] | Subdag[TaskSpec] | NewRun[RunInfo] | ExistingRun[RunInfo]


class JobCompleted[TaskSpec, Result, RunInfo](Struct):
    result: ResultType[TaskSpec, Result, RunInfo] | Unhandled
    custom: Any | None = None


class Stopped(Struct):
    reason: WireReason
    traceback: str | None = None
    custom: Any | None = None


# Hints only — encode-only, decoded on the Rust side.
type WireFromWorker[TaskSpec, Result, RunInfo] = Initialized | JobCompleted[TaskSpec, Result, RunInfo] | Stopped


# --- external tagging for the outbound side --------------------------------

_RESULT_KEY = {
    Computed: "result",
    Subdag: "subdag",
    NewRun: "new_run",
    ExistingRun: "existing_run",
}

_FROM_WORKER_KEY = {
    Initialized: "initialized",
    Stopped: "stopped",
}


def _wire_result(r):
    if isinstance(r, Unhandled):
        return "unhandled"  # unit variant -> bare string
    return {_RESULT_KEY[type(r)]: r}  # {"result": {"res": ...}}, etc.


def to_wire(msg):
    if isinstance(msg, JobCompleted):
        body = {"result": _wire_result(msg.result), "custom": msg.custom}
        return {"job_completed": body}
    return {_FROM_WORKER_KEY[type(msg)]: msg}
