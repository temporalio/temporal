"""vLLM-Omni stage adapter stub.

Extend this class to wrap vLLM-Omni's multi-stage serving infrastructure.
The durable workflow layer tracks stage state and manifests; it never sits
on the hot inference path.  vLLM-Omni's own orchestrator and connectors
handle stage-to-stage tensor transfer.
"""

from __future__ import annotations

from temporal_pipeline.adapters.base import ExternalAdapter
from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


class VllmOmniAdapter(ExternalAdapter):
    async def submit(self, spec: StageSpec) -> ExternalJobRef:
        raise NotImplementedError("VllmOmniAdapter.submit")

    async def poll(self, ref: ExternalJobRef) -> ExternalJobState:
        raise NotImplementedError("VllmOmniAdapter.poll")

    async def collect_artifacts(self, ref: ExternalJobRef) -> ArtifactManifest:
        raise NotImplementedError("VllmOmniAdapter.collect_artifacts")

    async def resume(self, ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
        raise NotImplementedError("VllmOmniAdapter.resume")

    async def cancel(self, ref: ExternalJobRef) -> None:
        raise NotImplementedError("VllmOmniAdapter.cancel")

    async def debug_bundle(self, ref: ExternalJobRef) -> dict:
        raise NotImplementedError("VllmOmniAdapter.debug_bundle")
