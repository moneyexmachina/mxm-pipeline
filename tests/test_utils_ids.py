from __future__ import annotations

from mxm.pipeline.utils.ids import (
    new_execution_event_id,
    new_flow_run_id,
    new_id,
    new_semantic_event_id,
    new_task_attempt_id,
    new_task_run_id,
)


def test_new_id_returns_non_empty_string() -> None:
    _id = new_id()
    assert isinstance(_id, str)
    assert _id != ""


def test_new_id_uniqueness_small_sample() -> None:
    ids = {new_id() for _ in range(100)}
    assert len(ids) == 100


def test_prefixed_ids_have_correct_prefixes() -> None:
    assert new_flow_run_id().startswith("flowrun_")
    assert new_task_run_id().startswith("taskrun_")
    assert new_task_attempt_id().startswith("taskattempt_")
    assert new_execution_event_id().startswith("execevt_")
    assert new_semantic_event_id().startswith("semevt_")


def test_prefixed_ids_are_unique_small_sample() -> None:
    ids = {new_task_attempt_id() for _ in range(100)}
    assert len(ids) == 100
