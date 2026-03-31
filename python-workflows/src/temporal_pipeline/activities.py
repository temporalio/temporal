"""Thin control-plane activities.

Each activity delegates to an ExternalAdapter looked up by engine name.
Activities are the *only* place I/O happens — workflow code stays deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import activity

from temporal_pipeline.adapters import AdapterRegistry
from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


@dataclass
class StageActivities:
    """Activity implementations backed by an adapter registry.

    Construct once at worker startup, then register the individual methods
    with the Temporal worker.
    """

    registry: AdapterRegistry

    @activity.defn
    async def submit_stage(self, spec: StageSpec) -> ExternalJobRef:
        adapter = self.registry.get(spec.engine)
        return await adapter.submit(spec)

    @activity.defn
    async def poll_stage(self, ref: ExternalJobRef) -> ExternalJobState:
        adapter = self.registry.get(ref.engine)
        return await adapter.poll(ref)

    @activity.defn
    async def collect_stage_artifacts(
        self, ref: ExternalJobRef
    ) -> ArtifactManifest:
        adapter = self.registry.get(ref.engine)
        return await adapter.collect_artifacts(ref)

    @activity.defn
    async def cancel_stage(self, ref: ExternalJobRef) -> None:
        adapter = self.registry.get(ref.engine)
        await adapter.cancel(ref)

    @activity.defn
    async def resume_stage(self, ref: ExternalJobRef, patch: dict) -> ExternalJobRef:
        adapter = self.registry.get(ref.engine)
        return await adapter.resume(ref, patch)

    @activity.defn
    async def collect_debug_bundle(self, ref: ExternalJobRef) -> dict:
        adapter = self.registry.get(ref.engine)
        return await adapter.debug_bundle(ref)
