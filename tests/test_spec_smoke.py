from mxm.pipeline.spec import AssetDecl, FlowSpec, TaskSpec
from mxm.types import JSONObj


def _dummy_task(params: JSONObj) -> int:
    val = params.get("x")
    if isinstance(val, (int, float)):
        return int(val) + 1
    return 1


def test_task_spec_defaults():
    task = TaskSpec(name="A", fn=_dummy_task)

    assert task.name == "A"
    assert task.fn is _dummy_task
    assert task.retries == 2
    assert task.retry_delay_s == 30

    # default factories should be *new* objects, not shared
    assert task.params == {}
    assert task.upstream == []


def test_asset_decl_defaults():
    asset = AssetDecl(id="justetf/daily")

    assert asset.id == "justetf/daily"
    assert asset.partition_key is None
    assert asset.format == "parquet"
    assert asset.path_template is None


def test_flow_spec_defaults_and_composition():
    # Two simple tasks, B depends on A
    task_a = TaskSpec(name="A", fn=_dummy_task)
    task_b = TaskSpec(name="B", fn=_dummy_task, upstream=["A"])

    flow = FlowSpec(name="demo", tasks=[task_a, task_b])

    assert flow.name == "demo"
    assert flow.schedule_cron is None
    assert flow.params == {}
    assert len(flow.tasks) == 2

    names = [t.name for t in flow.tasks]
    assert names == ["A", "B"]
    assert flow.tasks[1].upstream == ["A"]
