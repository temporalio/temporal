"""Integration test — full pipeline with mock adapters.

Full workflow tests require a Temporal test server binary.
Pure-logic tests always run.
"""

from __future__ import annotations

import pytest
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline import stages, artifacts, repair
    from temporal_pipeline.models import (
        ArtifactManifest,
        ArtifactRef,
        ExternalJobRef,
        ExternalJobState,
        FinalArtifacts,
        PipelineReq,
        RepairAction,
        RepairPlan,
        StageHandle,
        StageSpec,
        StageState,
    )
    from temporal_pipeline.stage_workflow import StageWorkflow


# ---------------------------------------------------------------------------
# Check if Temporal test server is available
# ---------------------------------------------------------------------------

async def _get_env():
    from temporalio.testing import WorkflowEnvironment
    return await WorkflowEnvironment.start_time_skipping()


def _can_start_test_server() -> bool:
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

_MOCK_ARTIFACTS: dict[str, dict[str, ArtifactRef]] = {
    "data_prep": {
        "packed_dataset": ArtifactRef(
            name="packed_dataset", uri="s3://data/packed"
        ),
    },
    "pretrain": {
        "model_ckpt": ArtifactRef(
            name="model_ckpt", uri="s3://models/pretrain/ckpt"
        ),
    },
    "sft": {
        "model_ckpt": ArtifactRef(
            name="model_ckpt", uri="s3://models/sft/ckpt"
        ),
    },
    "rl": {
        "policy_ckpt": ArtifactRef(
            name="policy_ckpt", uri="s3://models/rl/policy"
        ),
    },
}


@activity.defn(name="submit_stage")
async def mock_submit(spec: StageSpec) -> ExternalJobRef:
    return ExternalJobRef(engine=spec.engine, job_id=f"mock-{spec.name}")


@activity.defn(name="poll_stage")
async def mock_poll(ref: ExternalJobRef) -> ExternalJobState:
    return ExternalJobState(state=StageState.SUCCEEDED)


@activity.defn(name="collect_stage_artifacts")
async def mock_collect(ref: ExternalJobRef) -> ArtifactManifest:
    stage_name = ref.job_id.replace("mock-", "")
    arts = _MOCK_ARTIFACTS.get(stage_name, {})
    return ArtifactManifest(
        stage_name=stage_name, run_id="test-run", artifacts=arts
    )


@activity.defn(name="cancel_stage")
async def mock_cancel(ref: ExternalJobRef) -> None:
    pass


@activity.defn(name="resume_stage")
async def mock_resume(ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
    return ref


@activity.defn(name="collect_debug_bundle")
async def mock_debug(ref: ExternalJobRef) -> dict:
    return {}


TASK_QUEUE = "test-pipeline-queue"


# ---------------------------------------------------------------------------
# Pure-logic tests (no server needed)
# ---------------------------------------------------------------------------


class TestArtifactsModule:
    """Test artifacts.py helper functions."""

    def test_get_from_handle(self):
        ref = ArtifactRef(name="ckpt", uri="s3://ckpt")
        manifest = ArtifactManifest(
            stage_name="s", run_id="r", artifacts={"ckpt": ref}
        )
        handle = StageHandle(
            stage_name="s", workflow_id="w", run_id="r", manifest=manifest
        )
        assert artifacts.get(handle, "ckpt") is ref

    def test_all_refs(self):
        ref1 = ArtifactRef(name="a", uri="u1")
        ref2 = ArtifactRef(name="b", uri="u2")
        manifest = ArtifactManifest(
            stage_name="s",
            run_id="r",
            artifacts={"a": ref1, "b": ref2},
        )
        handle = StageHandle(
            stage_name="s", workflow_id="w", run_id="r", manifest=manifest
        )
        refs = artifacts.all_refs(handle)
        assert len(refs) == 2

    def test_all_refs_no_manifest(self):
        handle = StageHandle(stage_name="s", workflow_id="w", run_id="r")
        assert artifacts.all_refs(handle) == {}

    def test_merge_manifests(self):
        m1 = ArtifactManifest(
            stage_name="s1",
            run_id="r1",
            artifacts={"a": ArtifactRef(name="a", uri="u1")},
        )
        m2 = ArtifactManifest(
            stage_name="s2",
            run_id="r2",
            artifacts={
                "a": ArtifactRef(name="a", uri="u2"),  # overrides m1
                "b": ArtifactRef(name="b", uri="u3"),
            },
        )
        merged = artifacts.merge_manifests(m1, m2)
        assert merged["a"].uri == "u2"
        assert merged["b"].uri == "u3"


class TestRepairModule:
    """Test repair.py helper functions (pure-logic parts)."""

    @pytest.mark.asyncio
    async def test_resume_from_boundary(self):
        handles = {
            "data_prep": StageHandle(
                stage_name="data_prep", workflow_id="w1", run_id="r1"
            ),
            "pretrain": StageHandle(
                stage_name="pretrain", workflow_id="w2", run_id="r2"
            ),
            "sft": StageHandle(
                stage_name="sft", workflow_id="w3", run_id="r3"
            ),
            "rl": StageHandle(
                stage_name="rl", workflow_id="w4", run_id="r4"
            ),
        }
        preserved = await repair.resume_from_boundary(
            "pretrain", handles
        )
        assert list(preserved.keys()) == ["data_prep", "pretrain"]
        assert "sft" not in preserved
        assert "rl" not in preserved

    @pytest.mark.asyncio
    async def test_resume_from_boundary_first(self):
        handles = {
            "data_prep": StageHandle(
                stage_name="data_prep", workflow_id="w1", run_id="r1"
            ),
            "pretrain": StageHandle(
                stage_name="pretrain", workflow_id="w2", run_id="r2"
            ),
        }
        preserved = await repair.resume_from_boundary(
            "data_prep", handles
        )
        assert list(preserved.keys()) == ["data_prep"]


class TestAdapterRegistry:
    """Test the adapter registry."""

    def test_register_and_get(self):
        from temporal_pipeline.adapters import AdapterRegistry
        from temporal_pipeline.adapters.torchtitan import TorchTitanAdapter

        reg = AdapterRegistry()
        adapter = TorchTitanAdapter()
        reg.register("torchtitan", adapter)
        assert reg.get("torchtitan") is adapter

    def test_missing_engine(self):
        from temporal_pipeline.adapters import AdapterRegistry

        reg = AdapterRegistry()
        with pytest.raises(KeyError, match="No adapter registered"):
            reg.get("nonexistent")

    def test_engines_list(self):
        from temporal_pipeline.adapters import AdapterRegistry
        from temporal_pipeline.adapters.verl import VerlAdapter

        reg = AdapterRegistry()
        reg.register("verl", VerlAdapter())
        assert "verl" in reg.engines()


class TestWorkerFactory:
    """Test create_worker construction (without actually running)."""

    def test_create_worker_imports(self):
        from temporal_pipeline.worker import create_worker
        assert callable(create_worker)


# ---------------------------------------------------------------------------
# Pipeline workflow (for integration tests)
# ---------------------------------------------------------------------------


@workflow.defn
class TestPipeline:
    @workflow.run
    async def run(self, req: PipelineReq) -> FinalArtifacts:
        prep = await stages.run(
            "data_prep",
            engine="spark",
            inputs={"raw_data": req.inputs.get("raw_data", "")},
        )
        pretrain = await stages.run(
            "pretrain",
            engine="torchtitan",
            inputs={"dataset": prep.artifact("packed_dataset")},
        )
        sft = await stages.run(
            "sft",
            engine="verl",
            inputs={
                "base_ckpt": pretrain.artifact("model_ckpt"),
                "sft_data": req.inputs.get("sft_data", ""),
            },
        )
        rl = await stages.run(
            "rl",
            engine="verl",
            inputs={"policy_ckpt": sft.artifact("model_ckpt")},
        )
        return FinalArtifacts(
            artifacts={"model": rl.artifact("policy_ckpt")}
        )


# ---------------------------------------------------------------------------
# Workflow integration tests (need Temporal test server)
# ---------------------------------------------------------------------------


@needs_server
@pytest.mark.asyncio
async def test_full_pipeline():
    """End-to-end: pipeline chains four stages and returns final artifacts."""
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[StageWorkflow, TestPipeline],
            activities=[
                mock_submit, mock_poll, mock_collect,
                mock_cancel, mock_resume, mock_debug,
            ],
        ):
            result = await env.client.execute_workflow(
                TestPipeline.run,
                PipelineReq(
                    pipeline_name="posttrain",
                    inputs={"raw_data": "s3://raw", "sft_data": "s3://sft"},
                ),
                id="test-pipeline-posttrain",
                task_queue=TASK_QUEUE,
            )
            assert isinstance(result, FinalArtifacts)
            assert "model" in result.artifacts
            assert result.artifacts["model"].uri == "s3://models/rl/policy"
