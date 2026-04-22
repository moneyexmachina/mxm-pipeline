from __future__ import annotations

from uuid import uuid4


def new_id() -> str:
    return uuid4().hex


def new_flow_run_id() -> str:
    return f"flowrun_{new_id()}"


def new_task_run_id() -> str:
    return f"taskrun_{new_id()}"


def new_task_attempt_id() -> str:
    return f"taskattempt_{new_id()}"


def new_execution_event_id() -> str:
    return f"execevt_{new_id()}"


def new_semantic_event_id() -> str:
    return f"semevt_{new_id()}"
