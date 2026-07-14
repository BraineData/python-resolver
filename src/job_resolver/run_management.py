import datetime
import sqlite3
from typing import Any

import msgspec

from .core_worker import ENCODER
from .wire_format import RUN_DB_PATH, ExistingRun, NewRun, WorkerRunSpec

try:
    conn = sqlite3.connect(f"file:{RUN_DB_PATH}?mode=ro", uri=True)
except Exception as e:
    e.add_note(f"file:{RUN_DB_PATH}?mode=ro")
    raise


class SavedRunData(msgspec.Struct):
    base_location: str
    inner_cwd: str
    exit_code: int
    start_us: int
    end_us: int


def search_runs(
    w: WorkerRunSpec,
    *,
    match_cmd: bool = True,
    match_params: bool = True,
    match_env: bool = False,
    only_success: bool = True,
    run_start_interval: tuple[datetime.datetime | None, datetime.datetime | None] = (None, None),
    max_n: int | None = None,
):

    conditions = []
    values = []

    if run_start_interval[0]:
        min_as_int = int(run_start_interval[0].timestamp() * 10**6) + run_start_interval[0].microsecond
        conditions.append("CAST(run_data ->> '$.start_us' AS INT) >= ?")
        values.append(min_as_int)
    if run_start_interval[1]:
        max_as_int = int(run_start_interval[1].timestamp() * 10**6) + run_start_interval[1].microsecond
        conditions.append("CAST(run_data ->> '$.start_us' AS INT) <= ?")
        values.append(max_as_int)
    if match_cmd:
        conditions.append("cmd ->> '$.program' = ?")
        values.append(w.program)
        conditions.append("cmd -> '$.args' = json(?)")
        values.append(ENCODER.encode(w.args))

    if match_params:
        conditions.append("cmd -> '$.params' = json(?)")
        values.append(ENCODER.encode(w.params))

    if match_env:
        conditions.append("cmd -> '$.env' = json(?)")
        values.append(ENCODER.encode(w.env))

    if only_success:
        conditions.append("run_data ->> '$.exit_code' = ?")
        values.append(0)

    limit_str = f"LIMIT {max_n}" if max_n is not None else ""
    if conditions:
        query = f"""
            SELECT prid, cmd, run_data
            FROM runs
            WHERE {" AND ".join(conditions)}
            ORDER BY CAST(run_data ->> '$.start_us' AS INT) DESC
            {limit_str}
        """  # noqa: S608
    else:
        query = f"""
            SELECT prid, cmd, run_data
            FROM runs
            ORDER BY CAST(run_data ->> '$.start_us' AS INT) DESC
            {limit_str}
            """  # noqa: S608

    rows = conn.execute(query, values).fetchall()

    return [(row[0], msgspec.json.decode(row[1], type=WorkerRunSpec), msgspec.json.decode(row[2], type=SavedRunData)) for row in rows]


def try_reuse(
    spec: WorkerRunSpec,
    rai: Any,
    *,
    match_cmd: bool = True,
    match_params: bool = True,
    match_env: bool = False,
    only_success: bool = True,
    run_start_interval: tuple[datetime.datetime | None, datetime.datetime | None] = (None, None),
    no_reuse: bool = False,
):
    if not no_reuse:
        available = search_runs(spec, match_cmd=match_cmd, match_env=match_env, match_params=match_params, only_success=only_success, run_start_interval=run_start_interval)
        if available:
            return ExistingRun(available[-1][0], rai)
    return NewRun(
        spec,
        rai,
    )
