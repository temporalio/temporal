"""User-facing stages API — called inside @workflow.run methods.

Usage inside a pipeline workflow::

    from temporal_pipeline import stages

    prep = await stages.run("data_prep", engine="spark", inputs={...})
    ckpt = prep.artifact("packed_dataset")
"""

from __future__ import annotations

from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline.models import StageHandle, StageSpec
    from temporal_pipeline.stage_workflow import StageWorkflow


async def run(
    name: str,
    engine: str,
    inputs: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    timeout_seconds: int = 86400,
) -> StageHandle:
    """Start a child StageWorkflow and wait for it to complete.

    Returns a :class:`StageHandle` whose ``.artifact(name)`` method gives
    typed artifact refs from the committed manifest.
    """
    spec = StageSpec(
        name=name,
        engine=engine,
        inputs=inputs or {},
        config=config or {},
        timeout_seconds=timeout_seconds,
    )
    parent_wf_id = workflow.info().workflow_id
    child_id = f"{parent_wf_id}-stage-{name}"

    handle: StageHandle = await workflow.execute_child_workflow(
        StageWorkflow.run,
        spec,
        id=child_id,
    )
    return handle


async def run_parallel(
    specs: list[dict[str, Any]],
) -> list[StageHandle]:
    """Run multiple stages concurrently and collect all results.

    Each element in *specs* is a dict with keys matching :func:`run`'s params:
    ``name``, ``engine``, ``inputs``, ``config``, ``timeout_seconds``.
    """
    parent_wf_id = workflow.info().workflow_id
    handles = []
    for s in specs:
        spec = StageSpec(
            name=s["name"],
            engine=s["engine"],
            inputs=s.get("inputs", {}),
            config=s.get("config", {}),
            timeout_seconds=s.get("timeout_seconds", 86400),
        )
        child_id = f"{parent_wf_id}-stage-{spec.name}"
        h = await workflow.start_child_workflow(
            StageWorkflow.run,
            spec,
            id=child_id,
        )
        handles.append(h)
    return [await h for h in handles]
