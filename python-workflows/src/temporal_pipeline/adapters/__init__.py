"""Adapter registry and base class re-exports."""

from __future__ import annotations

from temporal_pipeline.adapters.base import ExternalAdapter


class AdapterRegistry:
    """Maps engine names to their concrete adapter instances."""

    def __init__(self) -> None:
        self._adapters: dict[str, ExternalAdapter] = {}

    def register(self, engine: str, adapter: ExternalAdapter) -> None:
        self._adapters[engine] = adapter

    def get(self, engine: str) -> ExternalAdapter:
        try:
            return self._adapters[engine]
        except KeyError:
            raise KeyError(
                f"No adapter registered for engine '{engine}'. "
                f"Available: {list(self._adapters.keys())}"
            )

    def engines(self) -> list[str]:
        return list(self._adapters.keys())


__all__ = ["AdapterRegistry", "ExternalAdapter"]
