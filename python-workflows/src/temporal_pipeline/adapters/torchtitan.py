"""TorchTitan adapter stub.

Extend this class to integrate with a real TorchTitan training cluster.
The adapter should only perform thin control-plane calls — submitting jobs,
polling status, and collecting checkpoint manifests.  The heavy distributed
training runs inside TorchTitan itself.
"""

from __future__ import annotations

from temporal_pipeline.adapters.base import ExternalAdapter
from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


class TorchTitanAdapter(ExternalAdapter):
    async def submit(self, spec: StageSpec) -> ExternalJobRef:
        raise NotImplementedError("TorchTitanAdapter.submit")

    async def poll(self, ref: ExternalJobRef) -> ExternalJobState:
        raise NotImplementedError("TorchTitanAdapter.poll")

    async def collect_artifacts(self, ref: ExternalJobRef) -> ArtifactManifest:
        raise NotImplementedError("TorchTitanAdapter.collect_artifacts")

    async def resume(self, ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
        raise NotImplementedError("TorchTitanAdapter.resume")

    async def cancel(self, ref: ExternalJobRef) -> None:
        raise NotImplementedError("TorchTitanAdapter.cancel")

    async def debug_bundle(self, ref: ExternalJobRef) -> dict:
        raise NotImplementedError("TorchTitanAdapter.debug_bundle")
