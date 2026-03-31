"""Megatron-LM adapter stub.

Extend this class to integrate with a real Megatron training cluster.
"""

from __future__ import annotations

from temporal_pipeline.adapters.base import ExternalAdapter
from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


class MegatronAdapter(ExternalAdapter):
    async def submit(self, spec: StageSpec) -> ExternalJobRef:
        raise NotImplementedError("MegatronAdapter.submit")

    async def poll(self, ref: ExternalJobRef) -> ExternalJobState:
        raise NotImplementedError("MegatronAdapter.poll")

    async def collect_artifacts(self, ref: ExternalJobRef) -> ArtifactManifest:
        raise NotImplementedError("MegatronAdapter.collect_artifacts")

    async def resume(self, ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
        raise NotImplementedError("MegatronAdapter.resume")

    async def cancel(self, ref: ExternalJobRef) -> None:
        raise NotImplementedError("MegatronAdapter.cancel")

    async def debug_bundle(self, ref: ExternalJobRef) -> dict:
        raise NotImplementedError("MegatronAdapter.debug_bundle")
