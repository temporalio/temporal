"""Repair engine — workflow-safe helpers for bug recovery.

These are meant to be called from inside a pipeline workflow's ``@workflow.run``
method.  They send Temporal Updates to running StageWorkflows.
"""

from __future__ import annotations

from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline.models import (
        RepairAction,
        RepairPlan,
        StageHandle,
    )


async def resume_from_boundary(
    boundary_stage_name: str,
    completed_handles: dict[str, StageHandle],
    patch: dict[str, Any] | None = None,
) -> dict[str, StageHandle]:
    """Determine which stages can be reused from a prior run.

    Returns the subset of *completed_handles* whose stage name is at or
    before *boundary_stage_name* in insertion order — those stages are
    preserved and should not be re-executed.
    """
    preserved: dict[str, StageHandle] = {}
    for name, handle in completed_handles.items():
        preserved[name] = handle
        if name == boundary_stage_name:
            break
    return preserved


async def apply(
    plan: RepairPlan,
    stage_workflow_id: str,
) -> StageHandle:
    """Send a repair update to a running (blocked) StageWorkflow."""
    ext_handle = workflow.get_external_workflow_handle(stage_workflow_id)
    result: StageHandle = await ext_handle.execute_update(
        "apply_repair",
        plan,
    )
    return result
