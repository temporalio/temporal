"""Abstract base class for external execution-engine adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod

from temporal_pipeline.models import (
    ArtifactManifest,
    ExternalJobRef,
    ExternalJobState,
    StageSpec,
)


class ExternalAdapter(ABC):
    """Thin control-plane adapter for one execution backend.

    Each method is a lightweight RPC — submit, poll, collect, resume, cancel,
    or fetch a debug bundle.  Heavy compute stays in the external system.
    """

    @abstractmethod
    async def submit(self, spec: StageSpec) -> ExternalJobRef:
        """Submit a stage job and return an opaque job reference."""

    @abstractmethod
    async def poll(self, ref: ExternalJobRef) -> ExternalJobState:
        """Poll the external system for current job state."""

    @abstractmethod
    async def collect_artifacts(self, ref: ExternalJobRef) -> ArtifactManifest:
        """Retrieve the artifact manifest from a completed job."""

    @abstractmethod
    async def resume(
        self, ref: ExternalJobRef, patch: dict
    ) -> ExternalJobRef:
        """Resume a paused/failed job, optionally applying a patch."""

    @abstractmethod
    async def cancel(self, ref: ExternalJobRef) -> None:
        """Request cancellation of a running job."""

    @abstractmethod
    async def debug_bundle(self, ref: ExternalJobRef) -> dict:
        """Collect a debug / repro bundle for a failed job."""
