"""Tests for StageWorkflow lifecycle.

The full workflow tests require a Temporal test server binary.
If unavailable (e.g., sandboxed CI), those tests are skipped and
the pure-logic tests still run.
"""

from __future__ import annotations

import dataclasses

import pytest
from temporalio import activity

from temporal_pipeline.models import (
    ArtifactManifest,
    ArtifactRef,
    BugRecord,
    ExternalJobRef,
    ExternalJobState,
    RepairAction,
    RepairPlan,
    StageCarryOver,
    StageHandle,
    StageSpec,
    StageState,
)


# ---------------------------------------------------------------------------
# Check if Temporal test server is available
# ---------------------------------------------------------------------------

async def _get_env():
    from temporalio.testing import WorkflowEnvironment
    return await WorkflowEnvironment.start_time_skipping()


def _can_start_test_server() -> bool:
    """Return True if we can spin up a local test server."""
    import asyncio
    try:
        loop = asyncio.new_event_loop()
        env = loop.run_until_complete(_get_env())
        loop.run_until_complete(env.__aexit__(None, None, None))
        loop.close()
        return True
    except Exception:
        return False


_HAS_SERVER = _can_start_test_server()
needs_server = pytest.mark.skipif(
    not _HAS_SERVER,
    reason="Temporal test server not available (no network access)",
)


# ---------------------------------------------------------------------------
# Mock activities
# ---------------------------------------------------------------------------


@activity.defn(name="submit_stage")
async def mock_submit(spec: StageSpec) -> ExternalJobRef:
    return ExternalJobRef(engine=spec.engine, job_id=f"mock-{spec.name}")


@activity.defn(name="poll_stage")
async def mock_poll(ref: ExternalJobRef) -> ExternalJobState:
    return ExternalJobState(state=StageState.SUCCEEDED, message="done")


@activity.defn(name="collect_stage_artifacts")
async def mock_collect(ref: ExternalJobRef) -> ArtifactManifest:
    return ArtifactManifest(
        stage_name=ref.job_id.replace("mock-", ""),
        run_id="test-run",
        artifacts={
            "model_ckpt": ArtifactRef(
                name="model_ckpt",
                uri=f"s3://bucket/{ref.job_id}/ckpt",
                stage_name=ref.job_id.replace("mock-", ""),
            ),
        },
    )


@activity.defn(name="cancel_stage")
async def mock_cancel(ref: ExternalJobRef) -> None:
    pass


@activity.defn(name="resume_stage")
async def mock_resume(ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
    return ExternalJobRef(engine=ref.engine, job_id=f"{ref.job_id}-resumed")


@activity.defn(name="collect_debug_bundle")
async def mock_debug(ref: ExternalJobRef) -> dict:
    return {"logs": "mock logs"}


TASK_QUEUE = "test-stage-queue"


# ---------------------------------------------------------------------------
# Pure-logic tests (always run, no server needed)
# ---------------------------------------------------------------------------


class TestStageCarryOverRestore:
    """Verify carry-over state reconstruction logic."""

    def test_carry_over_preserves_state(self):
        spec = StageSpec(name="pretrain", engine="torchtitan")
        job_ref = ExternalJobRef(engine="torchtitan", job_id="j1")
        manifest = ArtifactManifest(
            stage_name="pretrain",
            run_id="r1",
            artifacts={"ckpt": ArtifactRef(name="ckpt", uri="s3://ckpt")},
        )
        carry = StageCarryOver(
            spec=spec,
            state=StageState.RUNNING,
            job_ref=job_ref,
            manifest=manifest,
            event_count=42,
        )
        assert carry.spec.name == "pretrain"
        assert carry.state == StageState.RUNNING
        assert carry.job_ref is not None
        assert carry.job_ref.job_id == "j1"
        assert carry.manifest is not None
        assert "ckpt" in carry.manifest.artifacts
        assert carry.event_count == 42

    def test_carry_over_defaults(self):
        spec = StageSpec(name="sft", engine="verl")
        carry = StageCarryOver(spec=spec)
        assert carry.state == StageState.CREATED
        assert carry.job_ref is None
        assert carry.manifest is None
        assert carry.bug is None
        assert carry.event_count == 0


class TestStageHandleFromWorkflowResult:
    """Verify that StageHandle behaves correctly as a workflow result."""

    def test_handle_with_manifest(self):
        manifest = ArtifactManifest(
            stage_name="pretrain",
            run_id="r1",
            artifacts={
                "model_ckpt": ArtifactRef(
                    name="model_ckpt", uri="s3://ckpt"
                ),
            },
        )
        handle = StageHandle(
            stage_name="pretrain",
            workflow_id="wf-1",
            run_id="r1",
            manifest=manifest,
        )
        ref = handle.artifact("model_ckpt")
        assert ref.uri == "s3://ckpt"

    def test_handle_serialization_roundtrip(self):
        manifest = ArtifactManifest(
            stage_name="pretrain",
            run_id="r1",
            artifacts={
                "model_ckpt": ArtifactRef(name="model_ckpt", uri="s3://ckpt"),
            },
        )
        handle = StageHandle(
            stage_name="pretrain",
            workflow_id="wf-1",
            run_id="r1",
            manifest=manifest,
        )
        d = dataclasses.asdict(handle)
        reconstructed = StageHandle(**d)
        # After asdict, manifest is a plain dict — verify top-level fields
        assert reconstructed.stage_name == "pretrain"
        assert reconstructed.workflow_id == "wf-1"


class TestRepairPlanValidation:
    """Verify repair plan construction."""

    def test_resume_plan(self):
        plan = RepairPlan(
            action=RepairAction.RESUME_FROM_BOUNDARY,
            resume_from_boundary="pretrain_complete",
            preserve_artifacts=["model_ckpt"],
        )
        assert plan.action == RepairAction.RESUME_FROM_BOUNDARY
        assert plan.resume_from_boundary == "pretrain_complete"

    def test_retry_plan(self):
        plan = RepairPlan(
            action=RepairAction.RETRY_STAGE,
            target_stage="sft",
        )
        assert plan.target_stage == "sft"

    def test_abort_plan(self):
        plan = RepairPlan(action=RepairAction.ABORT_PIPELINE)
        assert plan.patch == {}


class TestBugRecordConstruction:
    def test_from_failure(self):
        bug = BugRecord(
            stage_name="rl",
            error_type="OOM",
            error_message="Rank 3 OOM at step 500",
            debug_bundle_uri="s3://debug/rl-bug-001",
        )
        assert bug.stage_name == "rl"
        assert bug.debug_bundle_uri.startswith("s3://")


# ---------------------------------------------------------------------------
# Workflow tests (need Temporal test server)
# ---------------------------------------------------------------------------


@needs_server
@pytest.mark.asyncio
async def test_happy_path():
    """Stage goes CREATED -> SUBMITTED -> RUNNING -> SUCCEEDED."""
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    from temporal_pipeline.stage_workflow import StageWorkflow

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[StageWorkflow],
            activities=[
                mock_submit, mock_poll, mock_collect,
                mock_cancel, mock_resume, mock_debug,
            ],
        ):
            result = await env.client.execute_workflow(
                StageWorkflow.run,
                StageSpec(name="pretrain", engine="torchtitan"),
                id="test-stage-pretrain",
                task_queue=TASK_QUEUE,
            )
            assert isinstance(result, StageHandle)
            assert result.stage_name == "pretrain"
            assert result.manifest is not None
            assert "model_ckpt" in result.manifest.artifacts


@needs_server
@pytest.mark.asyncio
async def test_artifact_access():
    """StageHandle.artifact() returns the correct ref."""
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    from temporal_pipeline.stage_workflow import StageWorkflow

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[StageWorkflow],
            activities=[
                mock_submit, mock_poll, mock_collect,
                mock_cancel, mock_resume, mock_debug,
            ],
        ):
            result = await env.client.execute_workflow(
                StageWorkflow.run,
                StageSpec(name="sft", engine="verl"),
                id="test-stage-sft",
                task_queue=TASK_QUEUE,
            )
            ref = result.artifact("model_ckpt")
            assert ref.name == "model_ckpt"
            assert "sft" in ref.uri
