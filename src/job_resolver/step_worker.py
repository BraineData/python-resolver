from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from msgspec import Struct

from .core_worker import HandlerProtocol
from .wire_format import (
    Computed,
    ExistingRun,
    NewRun,
    ResultSpec,
    ResultType,
    RunJob,
    SimpleJob,
    Subdag,
    SubdagIndex,
    Submit,
    WireJobResult,
    WireSubdagTaskSpec,
)

# ruff: noqa: D103, ANN202, ANN001, C901, PLR0915, PLR0913, FBT001, PT018, ARG001, ANN401, PLR0912, TRY004, PLW0602, ANN003, FBT003, ANN002, FBT002


class NamedTask(Struct):
    name: str
    data: Any  # No type hints


@dataclass(slots=True)
class JobResult:
    data: Any


class Failed:
    """A result declared a failure, carrying no value (wire data -> null)."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "Failed"


@dataclass(slots=True)
class SubdagResult:
    index: SubdagIndex
    tags: list[tuple[str, str]] = field(default_factory=list)


type StepResultType[Result, RunInfo] = Result | SubdagResult | NewRun[RunInfo] | ExistingRun[RunInfo] | Failed


class StepProtocol(Protocol):
    def __call__(self, task_spec: Any, deps: list[WireJobResult[Any, Any]]) -> StepResultType[Any, Any]: ...


def no_task_maker(name: str, task_spec, deps: list[SubdagIndex]) -> SubdagIndex:
    raise Exception("No task maker defined")


CURRENT_TASK_MAKER = no_task_maker  # Implies no concurrent programming. But really, that should be fine


@dataclass(slots=True)
class StepRegistry(HandlerProtocol[NamedTask, Any, Any]):
    TS = NamedTask
    R = Any  # pyright: ignore[reportAssignmentType]
    RAI = Any  # pyright: ignore[reportAssignmentType]
    step_functions: dict[str, StepProtocol]

    def handle(self, job: Submit[NamedTask, Any, Any]) -> ResultType[NamedTask, Any, Any]:
        global CURRENT_TASK_MAKER  # noqa: PLW0603
        name = job.task_spec.name
        step_fn = self.step_functions.get(name)
        if step_fn is None:
            raise Exception("Unknown task name")
        task_specs: list[WireSubdagTaskSpec[NamedTask]] = []

        def task_maker(name: str, task_spec, deps: list[SubdagIndex]) -> SubdagIndex:
            nonlocal task_specs
            task_specs.append(WireSubdagTaskSpec(NamedTask(name, task_spec), deps))
            return len(task_specs) - 1

        CURRENT_TASK_MAKER = task_maker
        try:
            res = step_fn(job.task_spec.data, job.dep_results)
            if isinstance(res, SubdagResult):  # the result is a subdag
                res = Subdag(task_specs, res.index, res.tags)
            elif isinstance(res, Failed):
                res = Computed(ResultSpec(True, None))
            elif not isinstance(res, (NewRun, ExistingRun)):
                res = Computed(ResultSpec(failed=False, data=res))
        finally:
            CURRENT_TASK_MAKER = no_task_maker
        return res

    def register(
        self,
        f,
        name: str,
        propagate_fail: bool,
        accepts: tuple[Literal["failed", "simple", "run"], ...],
        unwrap: bool | Literal["auto"],
        as_kwargs: bool,
        no_deps: bool,
    ):
        if unwrap == "auto":
            unwrap = propagate_fail and len(accepts) == 1 and "failed" not in accepts
        if unwrap:
            assert propagate_fail and len(accepts) == 1 and "failed" not in accepts

        def new_f(task_spec: Any, deps: list[WireJobResult]) -> StepResultType[Any, Any]:
            new_deps = []
            for d in deps:
                if isinstance(d, SimpleJob):
                    if d.res.failed:
                        if propagate_fail:
                            return Failed()
                        if "failed" in accepts:
                            payload = Failed()  # We know we are wrapped
                        else:
                            raise Exception("Failed jobs are not accepted")
                    elif "simple" in accepts:
                        payload = d.res.data if unwrap else JobResult(d.res.data)
                    else:
                        raise Exception("Simple jobs are not accepted")
                elif isinstance(d, RunJob):
                    if d.data.exit_code != 0:
                        if propagate_fail:
                            return Failed()
                        if "failed" in accepts:
                            payload = Failed()  # We know we are wrapped
                        else:
                            raise Exception("Failed jobs are not accepted")
                    if "run" in accepts:
                        payload = d  # Wrapped or unwrapped is the same
                    else:
                        raise Exception("Run jobs are not accepted")
                else:
                    raise Exception("Unknown type")
                new_deps.append(payload)
            if as_kwargs:
                if task_spec is None:
                    task_spec = {}
                if no_deps:
                    if len(new_deps) > 0:
                        raise Exception("no dep task has deps...")
                    return f(**task_spec)
                return f(**task_spec, deps=new_deps)
            if no_deps:
                if len(new_deps) > 0:
                    raise Exception("no dep task has deps...")
                return f(task_spec)
            return f(task_spec, new_deps)

        self.step_functions[name] = new_f

        def task_f_kwargs(*args, deps: list[SubdagIndex] | None = None, **kwargs):
            if deps is None:
                deps = []
            if len(args) > 0:
                raise Exception("arg calls not supported")
            if no_deps and len(deps) > 0:
                raise Exception("No deps allowed")

            global CURRENT_TASK_MAKER
            return CURRENT_TASK_MAKER(name, kwargs, deps)

        def task_f(task_spec, deps: list[SubdagIndex] | None = None) -> SubdagIndex:
            if deps is None:
                deps = []
            if no_deps and len(deps) > 0:
                raise Exception("No deps allowed")
            global CURRENT_TASK_MAKER
            return CURRENT_TASK_MAKER(name, task_spec, deps)

        return task_f_kwargs if as_kwargs else task_f


handle = StepRegistry({})


@dataclass(slots=True)
class Step:
    _propagate_fail: bool = True
    _accepts: tuple[Literal["failed", "simple", "run"], ...] = ("simple", "run")
    _unwrap: bool | Literal["auto"] = "auto"
    _as_kwargs: bool = True
    _no_deps: bool = False

    def __call__[TS](self, f: Callable) -> Callable:
        return handle.register(f, f.__name__, self._propagate_fail, self._accepts, self._unwrap, self._as_kwargs, self._no_deps)

    def handles(self, *values: Literal["failed", "simple", "run"]):
        return Step(self._propagate_fail, values, self._unwrap, self._as_kwargs, self._no_deps)

    def allows_fail_deps(self):
        return Step(False, self._accepts, self._unwrap, self._as_kwargs, self._no_deps)

    def ensure_unwrapped(self):
        return Step(self._propagate_fail, self._accepts, True, self._as_kwargs, self._no_deps)

    def ensure_wrapped(self):
        return Step(self._propagate_fail, self._accepts, False, self._as_kwargs, self._no_deps)

    def no_kwargs_expand(self):
        return Step(self._propagate_fail, self._accepts, self._unwrap, False, self._no_deps)

    def no_deps(self):
        return Step(self._propagate_fail, self._accepts, self._unwrap, self._as_kwargs, True)

    def configure(
        self,
        propagate_fail: bool = True,
        accepts: tuple[Literal["failed", "simple", "run"], ...] = ("simple", "run"),
        unwrap: bool | Literal["auto"] = "auto",
        as_kwargs: bool = True,
    ):
        return Step(propagate_fail, accepts, unwrap, as_kwargs)


step = Step()
