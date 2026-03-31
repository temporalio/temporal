"""Artifact helpers — convenience wrappers over StageHandle manifests."""

from __future__ import annotations

from temporal_pipeline.models import ArtifactManifest, ArtifactRef, StageHandle


def get(handle: StageHandle, name: str) -> ArtifactRef:
    """Shorthand for ``handle.artifact(name)``."""
    return handle.artifact(name)


def all_refs(handle: StageHandle) -> dict[str, ArtifactRef]:
    """Return all artifact refs from a stage's committed manifest."""
    if handle.manifest is None:
        return {}
    return dict(handle.manifest.artifacts)


def merge_manifests(*manifests: ArtifactManifest) -> dict[str, ArtifactRef]:
    """Merge multiple manifests into a single flat artifact dict.

    Later manifests override earlier ones on name collision.
    """
    merged: dict[str, ArtifactRef] = {}
    for m in manifests:
        merged.update(m.artifacts)
    return merged
