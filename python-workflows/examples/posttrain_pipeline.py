"""Example: Post-training pipeline (pretrain -> SFT -> RL).

This is what user-authored workflow code looks like.  The workflow is simple
Python orchestration — all heavy compute runs in external systems via adapters.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline import stages
    from temporal_pipeline.models import FinalArtifacts, PipelineReq


@workflow.defn
class PosttrainPipeline:
    @workflow.run
    async def run(self, req: PipelineReq) -> FinalArtifacts:
        # Stage 1: data preparation
        prep = await stages.run(
            "data_prep",
            engine="spark",
            inputs={"raw_data": req.inputs.get("raw_data", "")},
        )

        # Stage 2: pretrain with TorchTitan
        pretrain = await stages.run(
            "pretrain",
            engine="torchtitan",
            inputs={"dataset": prep.artifact("packed_dataset")},
        )

        # Stage 3: supervised fine-tuning with verl
        sft = await stages.run(
            "sft",
            engine="verl",
            inputs={
                "base_ckpt": pretrain.artifact("model_ckpt"),
                "sft_data": req.inputs.get("sft_data", ""),
            },
        )

        # Stage 4: RL training with verl
        rl = await stages.run(
            "rl",
            engine="verl",
            inputs={"policy_ckpt": sft.artifact("model_ckpt")},
        )

        return FinalArtifacts(
            artifacts={"model": rl.artifact("policy_ckpt")}
        )
