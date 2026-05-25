from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.utils.serde import (
    json_from_sql,
    json_to_sql,
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
class SemanticEventsStore:
    backend: SQLiteBackend

    def append(self, event: SemanticEvent) -> None:
        self.backend.ensure_migrated()

        sql = """
            INSERT OR IGNORE INTO semantic_events (
                event_id,
                flow_run_id,
                task_run_id,
                event_type,
                event_ts,
                domain_key,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        with self.backend.transaction() as conn:
            conn.execute(
                sql,
                (
                    event.event_id,
                    event.flow_run_id,
                    event.task_run_id,
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
                    flow_run_id,
                    task_run_id,
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

    def list_for_flow_run(self, flow_run_id: str) -> list[SemanticEvent]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id,
                    flow_run_id,
                    task_run_id,
                    event_type,
                    event_ts,
                    domain_key,
                    payload_json
                FROM semantic_events
                WHERE flow_run_id = ?
                ORDER BY event_ts ASC, event_id ASC
                """,
                (flow_run_id,),
            ).fetchall()

        return [self._row_to_semantic_event(row) for row in rows]

    def list_for_task_run(self, task_run_id: str) -> list[SemanticEvent]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id,
                    flow_run_id,
                    task_run_id,
                    event_type,
                    event_ts,
                    domain_key,
                    payload_json
                FROM semantic_events
                WHERE task_run_id = ?
                ORDER BY event_ts ASC, event_id ASC
                """,
                (task_run_id,),
            ).fetchall()

        return [self._row_to_semantic_event(row) for row in rows]

    def list_all(self) -> list[SemanticEvent]:
        self.backend.ensure_migrated()

        with self.backend.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id,
                    flow_run_id,
                    task_run_id,
                    event_type,
                    event_ts,
                    domain_key,
                    payload_json
                FROM semantic_events
                ORDER BY event_ts ASC, event_id ASC
                """
            ).fetchall()

        return [self._row_to_semantic_event(row) for row in rows]

    @staticmethod
    def _row_to_semantic_event(row: Any) -> SemanticEvent:
        return SemanticEvent(
            event_id=_as_str(row["event_id"]),
            flow_run_id=_as_str(row["flow_run_id"]),
            task_run_id=_as_str(row["task_run_id"]),
            event_type=_as_str(row["event_type"]),
            event_ts=_require_ts(
                row["event_ts"], field_name="semantic_events.event_ts"
            ),
            domain_key=_as_str(row["domain_key"]),
            payload=json_from_sql(_as_str(row["payload_json"])),
        )
