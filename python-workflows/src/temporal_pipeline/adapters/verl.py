"""verl / MILES adapter stub.

Extend this class to integrate with a real verl post-training / RL cluster.
"""

from __future__ import annotations

from temporal_pipeline.adapters.base import ExternalAdapter
from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


class VerlAdapter(ExternalAdapter):
    async def submit(self, spec: StageSpec) -> ExternalJobRef:
        raise NotImplementedError("VerlAdapter.submit")

    async def poll(self, ref: ExternalJobRef) -> ExternalJobState:
        raise NotImplementedError("VerlAdapter.poll")

    async def collect_artifacts(self, ref: ExternalJobRef) -> ArtifactManifest:
        raise NotImplementedError("VerlAdapter.collect_artifacts")

    async def resume(self, ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
        raise NotImplementedError("VerlAdapter.resume")

    async def cancel(self, ref: ExternalJobRef) -> None:
        raise NotImplementedError("VerlAdapter.cancel")

    async def debug_bundle(self, ref: ExternalJobRef) -> dict:
        raise NotImplementedError("VerlAdapter.debug_bundle")
