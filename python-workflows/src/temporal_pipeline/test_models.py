"""Tests for data models — serialization, StageHandle.artifact(), enums."""

from __future__ import annotations

import dataclasses
import json

import pytest

from temporal_pipeline.models import (
    ArtifactManifest,
    ArtifactRef,
    BugRecord,
    ExternalJobRef,
    ExternalJobState,
    FinalArtifacts,
    PipelineReq,
    RepairAction,
    RepairPlan,
    StageCarryOver,
    StageHandle,
    StageSpec,
    StageState,
    SubworkflowHandle,
)


class TestStageState:
    def test_values(self):
        assert StageState.CREATED == "CREATED"
        assert StageState.SUCCEEDED == "SUCCEEDED"
        assert StageState.BLOCKED_ON_BUG == "BLOCKED_ON_BUG"

    def test_all_members(self):
        assert len(StageState) == 8


class TestRepairAction:
    def test_values(self):
        assert RepairAction.RESUME_FROM_BOUNDARY == "RESUME_FROM_BOUNDARY"
        assert len(RepairAction) == 4


class TestArtifactRef:
    def test_basic(self):
        ref = ArtifactRef(name="ckpt", uri="s3://bucket/ckpt")
        assert ref.name == "ckpt"
        assert ref.uri == "s3://bucket/ckpt"
        assert ref.metadata == {}

    def test_dataclass_asdict(self):
        ref = ArtifactRef(
            name="ckpt",
            uri="s3://bucket/ckpt",
            artifact_type="checkpoint",
            stage_name="pretrain",
        )
        d = dataclasses.asdict(ref)
        assert d["name"] == "ckpt"
        assert d["artifact_type"] == "checkpoint"
        # Ensure JSON-serializable
        json.dumps(d)


class TestArtifactManifest:
    def test_empty(self):
        m = ArtifactManifest(stage_name="test", run_id="r1")
        assert m.artifacts == {}

    def test_with_artifacts(self):
        ref = ArtifactRef(name="data", uri="/data")
        m = ArtifactManifest(
            stage_name="prep", run_id="r1", artifacts={"data": ref}
        )
        assert m.artifacts["data"].uri == "/data"


class TestStageHandle:
    def test_artifact_success(self):
        ref = ArtifactRef(name="model_ckpt", uri="s3://ckpt")
        manifest = ArtifactManifest(
            stage_name="pretrain",
            run_id="r1",
            artifacts={"model_ckpt": ref},
        )
        handle = StageHandle(
            stage_name="pretrain",
            workflow_id="wf-1",
            run_id="r1",
            manifest=manifest,
        )
        assert handle.artifact("model_ckpt") is ref

    def test_artifact_no_manifest(self):
        handle = StageHandle(
            stage_name="pretrain", workflow_id="wf-1", run_id="r1"
        )
        with pytest.raises(ValueError, match="no committed manifest"):
            handle.artifact("model_ckpt")

    def test_artifact_missing_key(self):
        manifest = ArtifactManifest(
            stage_name="pretrain", run_id="r1", artifacts={}
        )
        handle = StageHandle(
            stage_name="pretrain",
            workflow_id="wf-1",
            run_id="r1",
            manifest=manifest,
        )
        with pytest.raises(KeyError, match="not found"):
            handle.artifact("missing")


class TestStageSpec:
    def test_defaults(self):
        spec = StageSpec(name="pretrain", engine="torchtitan")
        assert spec.inputs == {}
        assert spec.config == {}
        assert spec.timeout_seconds == 86400


class TestRepairPlan:
    def test_resume(self):
        plan = RepairPlan(
            action=RepairAction.RESUME_FROM_BOUNDARY,
            resume_from_boundary="sft_complete",
            preserve_artifacts=["model_ckpt", "sft_data"],
        )
        assert plan.action == RepairAction.RESUME_FROM_BOUNDARY
        assert len(plan.preserve_artifacts) == 2


class TestPipelineReq:
    def test_basic(self):
        req = PipelineReq(
            pipeline_name="posttrain",
            inputs={"raw_data": "s3://data"},
        )
        assert req.pipeline_name == "posttrain"


class TestFinalArtifacts:
    def test_empty(self):
        fa = FinalArtifacts()
        assert fa.artifacts == {}


class TestSubworkflowHandle:
    def test_basic(self):
        h = SubworkflowHandle(
            boundary_name="pretrain_complete",
            workflow_id="wf-1",
            run_id="r1",
        )
        assert h.manifests == []


class TestStageCarryOver:
    def test_defaults(self):
        spec = StageSpec(name="pretrain", engine="torchtitan")
        carry = StageCarryOver(spec=spec)
        assert carry.state == StageState.CREATED
        assert carry.job_ref is None
        assert carry.event_count == 0


class TestExternalJobRef:
    def test_basic(self):
        ref = ExternalJobRef(engine="torchtitan", job_id="j-123")
        assert ref.metadata == {}


class TestExternalJobState:
    def test_basic(self):
        state = ExternalJobState(state=StageState.RUNNING, progress_pct=0.5)
        assert state.progress_pct == 0.5


class TestBugRecord:
    def test_basic(self):
        bug = BugRecord(
            stage_name="rl",
            error_type="OOM",
            error_message="Out of memory on rank 3",
        )
        assert bug.error_type == "OOM"


class TestJsonRoundtrip:
    """All models must be JSON-serializable via dataclasses.asdict."""

    def test_all_models(self):
        ref = ArtifactRef(name="x", uri="y")
        manifest = ArtifactManifest(
            stage_name="s", run_id="r", artifacts={"x": ref}
        )
        models = [
            ref,
            manifest,
            ExternalJobRef(engine="e", job_id="j"),
            ExternalJobState(state=StageState.RUNNING),
            StageSpec(name="n", engine="e"),
            StageHandle(
                stage_name="s", workflow_id="w", run_id="r", manifest=manifest
            ),
            SubworkflowHandle(boundary_name="b", workflow_id="w", run_id="r"),
            BugRecord(stage_name="s", error_type="t", error_message="m"),
            RepairPlan(action=RepairAction.RETRY_STAGE),
            PipelineReq(pipeline_name="p"),
            FinalArtifacts(),
            StageCarryOver(spec=StageSpec(name="n", engine="e")),
        ]
        for m in models:
            d = dataclasses.asdict(m)
            serialized = json.dumps(d)
            assert isinstance(serialized, str)
