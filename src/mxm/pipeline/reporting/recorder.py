from __future__ import annotations

from dataclasses import dataclass

from mxm.pipeline.reporting.events import ExecutionEvent, ExecutionEventType
from mxm.pipeline.reporting.models import (
    FlowRun,
    RunStatus,
    SemanticEvent,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.reporting.stores import ReportingStore
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.pipeline.utils.ids import new_execution_event_id
from mxm.types import JSONObj
from mxm.types.timestamps import TSNSScalar


@dataclass(frozen=True)
class ReportingRecorder:
    """
    Operational reporting writer for MXM pipeline execution.

    Responsibilities:
    - Materialise current-state FlowRun / TaskRun / TaskAttempt snapshots
    - Append execution events describing lifecycle transitions
    - Persist semantic events emitted from task execution
    - Provide a narrow imperative API for runners

    Non-responsibilities:
    - No execution scheduling or orchestration
    - No transition validation beyond constructing the requested records
    - No report querying or summarisation
    """

    store: ReportingStore

    # -------------------------------------------------------------------------
    # Flow lifecycle
    # -------------------------------------------------------------------------

    def record_flow_started(
        self,
        *,
        flow_run_id: str,
        flow_name: str,
        started_at: TSNSScalar | None = None,
    ) -> FlowRun:
        started_at = started_at or utc_now_ts_ns()

        flow_run = FlowRun(
            flow_run_id=flow_run_id,
            flow_name=flow_name,
            status=RunStatus.RUNNING,
            started_at=started_at,
            finished_at=None,
        )
        self.store.flow_runs.upsert(flow_run)

        self._append_execution_event(
            event_type=ExecutionEventType.FLOW_RUN_STARTED,
            event_ts=started_at,
            entity_type="flow_run",
            entity_id=flow_run_id,
            flow_run_id=flow_run_id,
        )

        return flow_run

    def record_flow_finished(
        self,
        *,
        flow_run_id: str,
        flow_name: str,
        status: RunStatus,
        started_at: TSNSScalar | None = None,
        finished_at: TSNSScalar | None = None,
    ) -> FlowRun:
        finished_at = finished_at or utc_now_ts_ns()

        flow_run = FlowRun(
            flow_run_id=flow_run_id,
            flow_name=flow_name,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
        )
        self.store.flow_runs.upsert(flow_run)

        self._append_execution_event(
            event_type=ExecutionEventType.FLOW_RUN_FINISHED,
            event_ts=finished_at,
            entity_type="flow_run",
            entity_id=flow_run_id,
            flow_run_id=flow_run_id,
            payload={"status": status.value},
        )

        return flow_run

    # -------------------------------------------------------------------------
    # Task-run lifecycle
    # -------------------------------------------------------------------------

    def record_task_run_created(
        self,
        *,
        task_run_id: str,
        flow_run_id: str,
        task_name: str,
        attempt_count: int = 0,
    ) -> TaskRun:
        task_run = TaskRun(
            task_run_id=task_run_id,
            flow_run_id=flow_run_id,
            task_name=task_name,
            status=RunStatus.PENDING,
            started_at=None,
            finished_at=None,
            attempt_count=attempt_count,
        )
        self.store.task_runs.upsert(task_run)

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_RUN_CREATED,
            event_ts=utc_now_ts_ns(),
            entity_type="task_run",
            entity_id=task_run_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
        )

        return task_run

    def record_task_run_blocked(
        self,
        *,
        task_run_id: str,
        flow_run_id: str,
        task_name: str,
        attempt_count: int = 0,
        event_ts: TSNSScalar | None = None,
    ) -> TaskRun:
        event_ts = event_ts or utc_now_ts_ns()

        task_run = TaskRun(
            task_run_id=task_run_id,
            flow_run_id=flow_run_id,
            task_name=task_name,
            status=RunStatus.BLOCKED,
            started_at=None,
            finished_at=event_ts,
            attempt_count=attempt_count,
        )
        self.store.task_runs.upsert(task_run)

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_RUN_BLOCKED,
            event_ts=event_ts,
            entity_type="task_run",
            entity_id=task_run_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
        )

        return task_run

    def record_task_run_skipped(
        self,
        *,
        task_run_id: str,
        flow_run_id: str,
        task_name: str,
        attempt_count: int = 0,
        event_ts: TSNSScalar | None = None,
    ) -> TaskRun:
        event_ts = event_ts or utc_now_ts_ns()

        task_run = TaskRun(
            task_run_id=task_run_id,
            flow_run_id=flow_run_id,
            task_name=task_name,
            status=RunStatus.SKIPPED,
            started_at=None,
            finished_at=event_ts,
            attempt_count=attempt_count,
        )
        self.store.task_runs.upsert(task_run)

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_RUN_SKIPPED,
            event_ts=event_ts,
            entity_type="task_run",
            entity_id=task_run_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
        )

        return task_run

    def record_task_run_updated(
        self,
        *,
        task_run_id: str,
        flow_run_id: str,
        task_name: str,
        status: RunStatus,
        attempt_count: int,
        started_at: TSNSScalar | None = None,
        finished_at: TSNSScalar | None = None,
    ) -> TaskRun:
        task_run = TaskRun(
            task_run_id=task_run_id,
            flow_run_id=flow_run_id,
            task_name=task_name,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            attempt_count=attempt_count,
        )
        self.store.task_runs.upsert(task_run)
        return task_run

    # -------------------------------------------------------------------------
    # Task-attempt lifecycle
    # -------------------------------------------------------------------------

    def record_task_attempt_started(
        self,
        *,
        task_attempt_id: str,
        task_run_id: str,
        flow_run_id: str,
        attempt_number: int,
        started_at: TSNSScalar | None = None,
    ) -> TaskAttempt:
        started_at = started_at or utc_now_ts_ns()

        task_attempt = TaskAttempt(
            task_attempt_id=task_attempt_id,
            task_run_id=task_run_id,
            attempt_number=attempt_number,
            status=RunStatus.RUNNING,
            started_at=started_at,
            finished_at=None,
            error_summary=None,
            log_ref=None,
        )
        self.store.task_attempts.upsert(task_attempt)

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
            event_ts=started_at,
            entity_type="task_attempt",
            entity_id=task_attempt_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            task_attempt_id=task_attempt_id,
        )

        return task_attempt

    def record_task_attempt_succeeded(
        self,
        *,
        task_attempt_id: str,
        task_run_id: str,
        flow_run_id: str,
        attempt_number: int,
        started_at: TSNSScalar | None = None,
        finished_at: TSNSScalar | None = None,
        log_ref: str | None = None,
    ) -> TaskAttempt:
        finished_at = finished_at or utc_now_ts_ns()

        task_attempt = TaskAttempt(
            task_attempt_id=task_attempt_id,
            task_run_id=task_run_id,
            attempt_number=attempt_number,
            status=RunStatus.SUCCEEDED,
            started_at=started_at,
            finished_at=finished_at,
            error_summary=None,
            log_ref=log_ref,
        )
        self.store.task_attempts.upsert(task_attempt)

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_ATTEMPT_SUCCEEDED,
            event_ts=finished_at,
            entity_type="task_attempt",
            entity_id=task_attempt_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            task_attempt_id=task_attempt_id,
        )

        return task_attempt

    def record_task_attempt_failed(
        self,
        *,
        task_attempt_id: str,
        task_run_id: str,
        flow_run_id: str,
        attempt_number: int,
        started_at: TSNSScalar | None = None,
        finished_at: TSNSScalar | None = None,
        error_summary: str | None = None,
        log_ref: str | None = None,
    ) -> TaskAttempt:
        finished_at = finished_at or utc_now_ts_ns()

        task_attempt = TaskAttempt(
            task_attempt_id=task_attempt_id,
            task_run_id=task_run_id,
            attempt_number=attempt_number,
            status=RunStatus.FAILED,
            started_at=started_at,
            finished_at=finished_at,
            error_summary=error_summary,
            log_ref=log_ref,
        )
        self.store.task_attempts.upsert(task_attempt)

        payload: JSONObj = {}
        if error_summary is not None:
            payload["error_summary"] = error_summary
        if log_ref is not None:
            payload["log_ref"] = log_ref

        self._append_execution_event(
            event_type=ExecutionEventType.TASK_ATTEMPT_FAILED,
            event_ts=finished_at,
            entity_type="task_attempt",
            entity_id=task_attempt_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            task_attempt_id=task_attempt_id,
            payload=payload,
        )

        return task_attempt

    # -------------------------------------------------------------------------
    # Semantic events
    # -------------------------------------------------------------------------

    def record_semantic_event(self, event: SemanticEvent) -> SemanticEvent:
        self.store.semantic_events.append(event)

        self._append_execution_event(
            event_type=ExecutionEventType.SEMANTIC_EVENT_EMITTED,
            event_ts=event.event_ts,
            entity_type="task_attempt",
            entity_id=event.task_attempt_id,
            task_attempt_id=event.task_attempt_id,
            payload={
                "semantic_event_id": event.event_id,
                "semantic_event_type": event.event_type,
                "domain_key": event.domain_key,
            },
        )

        return event

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _append_execution_event(
        self,
        *,
        event_type: ExecutionEventType,
        event_ts: TSNSScalar,
        entity_type: str,
        entity_id: str,
        flow_run_id: str | None = None,
        task_run_id: str | None = None,
        task_attempt_id: str | None = None,
        payload: JSONObj | None = None,
    ) -> ExecutionEvent:
        event = ExecutionEvent(
            event_id=new_execution_event_id(),
            event_type=event_type,
            event_ts=event_ts,
            entity_type=entity_type,
            entity_id=entity_id,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            task_attempt_id=task_attempt_id,
            payload={} if payload is None else payload,
        )
        self.store.execution_events.append(event)
        return event
