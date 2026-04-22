from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mxm.pipeline.reporting.events import ExecutionEvent, ExecutionEventType
from mxm.pipeline.reporting.models import (
    FlowRun,
    SemanticEvent,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.utils.serde import (
    json_from_sql,
    json_to_sql,
    status_from_sql,
    status_to_sql,
    ts_from_sql,
    ts_to_sql,
)
from mxm.types import TSNSScalar


def _as_str(value: Any) -> str:
    return str(value)


def _require_ts(value: str | None, *, field_name: str) -> TSNSScalar:
    ts = ts_from_sql(value)
    if ts is None:
        raise ValueError(f"Expected non-null timestamp for {field_name}.")
    return ts


@dataclass(frozen=True)
class FlowRunsStore:
    backend: SQLiteBackend

    def upsert(self, flow_run: FlowRun) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT INTO flow_runs (
                flow_run_id,
                flow_name,
                status,
                started_at,
                finished_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(flow_run_id) DO UPDATE SET
                flow_name = excluded.flow_name,
                status = excluded.status,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    flow_run.flow_run_id,
                    flow_run.flow_name,
                    status_to_sql(flow_run.status),
                    ts_to_sql(flow_run.started_at),
                    ts_to_sql(flow_run.finished_at),
                ),
            )

    def get(self, flow_run_id: str) -> FlowRun | None:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    flow_run_id,
                    flow_name,
                    status,
                    started_at,
                    finished_at
                FROM flow_runs
                WHERE flow_run_id = ?
                """,
                (flow_run_id,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_flow_run(row)

    def list_all(self) -> list[FlowRun]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    flow_run_id,
                    flow_name,
                    status,
                    started_at,
                    finished_at
                FROM flow_runs
                ORDER BY flow_name ASC, flow_run_id ASC
                """
            ).fetchall()

        return [self._row_to_flow_run(row) for row in rows]

    @staticmethod
    def _row_to_flow_run(row: Any) -> FlowRun:
        return FlowRun(
            flow_run_id=_as_str(row["flow_run_id"]),
            flow_name=_as_str(row["flow_name"]),
            status=status_from_sql(_as_str(row["status"])),
            started_at=ts_from_sql(row["started_at"]),
            finished_at=ts_from_sql(row["finished_at"]),
        )


@dataclass(frozen=True)
class TaskRunsStore:
    backend: SQLiteBackend

    def upsert(self, task_run: TaskRun) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT INTO task_runs (
                task_run_id,
                flow_run_id,
                task_name,
                status,
                started_at,
                finished_at,
                attempt_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_run_id) DO UPDATE SET
                flow_run_id = excluded.flow_run_id,
                task_name = excluded.task_name,
                status = excluded.status,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                attempt_count = excluded.attempt_count,
                updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    task_run.task_run_id,
                    task_run.flow_run_id,
                    task_run.task_name,
                    status_to_sql(task_run.status),
                    ts_to_sql(task_run.started_at),
                    ts_to_sql(task_run.finished_at),
                    int(task_run.attempt_count),
                ),
            )

    def get(self, task_run_id: str) -> TaskRun | None:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    task_run_id,
                    flow_run_id,
                    task_name,
                    status,
                    started_at,
                    finished_at,
                    attempt_count
                FROM task_runs
                WHERE task_run_id = ?
                """,
                (task_run_id,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_task_run(row)

    def list_for_flow_run(self, flow_run_id: str) -> list[TaskRun]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    task_run_id,
                    flow_run_id,
                    task_name,
                    status,
                    started_at,
                    finished_at,
                    attempt_count
                FROM task_runs
                WHERE flow_run_id = ?
                ORDER BY task_name ASC, task_run_id ASC
                """,
                (flow_run_id,),
            ).fetchall()

        return [self._row_to_task_run(row) for row in rows]

    @staticmethod
    def _row_to_task_run(row: Any) -> TaskRun:
        return TaskRun(
            task_run_id=_as_str(row["task_run_id"]),
            flow_run_id=_as_str(row["flow_run_id"]),
            task_name=_as_str(row["task_name"]),
            status=status_from_sql(_as_str(row["status"])),
            started_at=ts_from_sql(row["started_at"]),
            finished_at=ts_from_sql(row["finished_at"]),
            attempt_count=int(row["attempt_count"]),
        )


@dataclass(frozen=True)
class TaskAttemptsStore:
    backend: SQLiteBackend

    def upsert(self, task_attempt: TaskAttempt) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT INTO task_attempts (
                task_attempt_id,
                task_run_id,
                attempt_number,
                status,
                started_at,
                finished_at,
                error_summary,
                log_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_attempt_id) DO UPDATE SET
                task_run_id = excluded.task_run_id,
                attempt_number = excluded.attempt_number,
                status = excluded.status,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                error_summary = excluded.error_summary,
                log_ref = excluded.log_ref,
                updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    task_attempt.task_attempt_id,
                    task_attempt.task_run_id,
                    int(task_attempt.attempt_number),
                    status_to_sql(task_attempt.status),
                    ts_to_sql(task_attempt.started_at),
                    ts_to_sql(task_attempt.finished_at),
                    task_attempt.error_summary,
                    task_attempt.log_ref,
                ),
            )

    def get(self, task_attempt_id: str) -> TaskAttempt | None:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    task_attempt_id,
                    task_run_id,
                    attempt_number,
                    status,
                    started_at,
                    finished_at,
                    error_summary,
                    log_ref
                FROM task_attempts
                WHERE task_attempt_id = ?
                """,
                (task_attempt_id,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_task_attempt(row)

    def list_for_task_run(self, task_run_id: str) -> list[TaskAttempt]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    task_attempt_id,
                    task_run_id,
                    attempt_number,
                    status,
                    started_at,
                    finished_at,
                    error_summary,
                    log_ref
                FROM task_attempts
                WHERE task_run_id = ?
                ORDER BY attempt_number ASC, task_attempt_id ASC
                """,
                (task_run_id,),
            ).fetchall()

        return [self._row_to_task_attempt(row) for row in rows]

    @staticmethod
    def _row_to_task_attempt(row: Any) -> TaskAttempt:
        return TaskAttempt(
            task_attempt_id=_as_str(row["task_attempt_id"]),
            task_run_id=_as_str(row["task_run_id"]),
            attempt_number=int(row["attempt_number"]),
            status=status_from_sql(_as_str(row["status"])),
            started_at=ts_from_sql(row["started_at"]),
            finished_at=ts_from_sql(row["finished_at"]),
            error_summary=row["error_summary"],
            log_ref=row["log_ref"],
        )


@dataclass(frozen=True)
class ExecutionEventsStore:
    backend: SQLiteBackend

    def append(self, event: ExecutionEvent) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT OR IGNORE INTO execution_events (
                event_id,
                event_type,
                event_ts,
                entity_type,
                entity_id,
                flow_run_id,
                task_run_id,
                task_attempt_id,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    event.event_id,
                    event.event_type.value,
                    ts_to_sql(event.event_ts),
                    event.entity_type,
                    event.entity_id,
                    event.flow_run_id,
                    event.task_run_id,
                    event.task_attempt_id,
                    json_to_sql(event.payload),
                ),
            )

    def get(self, event_id: str) -> ExecutionEvent | None:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    event_id,
                    event_type,
                    event_ts,
                    entity_type,
                    entity_id,
                    flow_run_id,
                    task_run_id,
                    task_attempt_id,
                    payload_json
                FROM execution_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_execution_event(row)

    def list_for_flow_run(self, flow_run_id: str) -> list[ExecutionEvent]:
        return self._list_where("flow_run_id = ?", flow_run_id)

    def list_for_task_run(self, task_run_id: str) -> list[ExecutionEvent]:
        return self._list_where("task_run_id = ?", task_run_id)

    def list_for_task_attempt(self, task_attempt_id: str) -> list[ExecutionEvent]:
        return self._list_where("task_attempt_id = ?", task_attempt_id)

    def _list_where(self, where_sql: str, value: str) -> list[ExecutionEvent]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    event_id,
                    event_type,
                    event_ts,
                    entity_type,
                    entity_id,
                    flow_run_id,
                    task_run_id,
                    task_attempt_id,
                    payload_json
                FROM execution_events
                WHERE {where_sql}
                ORDER BY event_ts ASC, event_id ASC
                """,
                (value,),
            ).fetchall()

        return [self._row_to_execution_event(row) for row in rows]

    @staticmethod
    def _row_to_execution_event(row: Any) -> ExecutionEvent:
        return ExecutionEvent(
            event_id=_as_str(row["event_id"]),
            event_type=ExecutionEventType(_as_str(row["event_type"])),
            event_ts=_require_ts(
                row["event_ts"], field_name="execution_events.event_ts"
            ),
            entity_type=_as_str(row["entity_type"]),
            entity_id=_as_str(row["entity_id"]),
            flow_run_id=(
                None if row["flow_run_id"] is None else _as_str(row["flow_run_id"])
            ),
            task_run_id=(
                None if row["task_run_id"] is None else _as_str(row["task_run_id"])
            ),
            task_attempt_id=(
                None
                if row["task_attempt_id"] is None
                else _as_str(row["task_attempt_id"])
            ),
            payload=json_from_sql(_as_str(row["payload_json"])),
        )


@dataclass(frozen=True)
class SemanticEventsStore:
    backend: SQLiteBackend

    def append(self, event: SemanticEvent) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT OR IGNORE INTO semantic_events (
                event_id,
                task_attempt_id,
                event_type,
                event_ts,
                domain_key,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    event.event_id,
                    event.task_attempt_id,
                    event.event_type,
                    ts_to_sql(event.event_ts),
                    event.domain_key,
                    json_to_sql(event.payload),
                ),
            )

    def get(self, event_id: str) -> SemanticEvent | None:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    event_id,
                    task_attempt_id,
                    event_type,
                    event_ts,
                    domain_key,
                    payload_json
                FROM semantic_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_semantic_event(row)

    def list_for_task_attempt(self, task_attempt_id: str) -> list[SemanticEvent]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id,
                    task_attempt_id,
                    event_type,
                    event_ts,
                    domain_key,
                    payload_json
                FROM semantic_events
                WHERE task_attempt_id = ?
                ORDER BY event_ts ASC, event_id ASC
                """,
                (task_attempt_id,),
            ).fetchall()

        return [self._row_to_semantic_event(row) for row in rows]

    @staticmethod
    def _row_to_semantic_event(row: Any) -> SemanticEvent:
        return SemanticEvent(
            event_id=_as_str(row["event_id"]),
            task_attempt_id=_as_str(row["task_attempt_id"]),
            event_type=_as_str(row["event_type"]),
            event_ts=_require_ts(
                row["event_ts"], field_name="semantic_events.event_ts"
            ),
            domain_key=_as_str(row["domain_key"]),
            payload=json_from_sql(_as_str(row["payload_json"])),
        )


@dataclass(frozen=True)
class ReportingStore:
    flow_runs: FlowRunsStore
    task_runs: TaskRunsStore
    task_attempts: TaskAttemptsStore
    execution_events: ExecutionEventsStore
    semantic_events: SemanticEventsStore

    @classmethod
    def from_backend(cls, backend: SQLiteBackend) -> ReportingStore:
        return cls(
            flow_runs=FlowRunsStore(backend=backend),
            task_runs=TaskRunsStore(backend=backend),
            task_attempts=TaskAttemptsStore(backend=backend),
            execution_events=ExecutionEventsStore(backend=backend),
            semantic_events=SemanticEventsStore(backend=backend),
        )
