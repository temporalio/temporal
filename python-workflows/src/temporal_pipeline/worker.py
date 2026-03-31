"""Worker setup helper.

Creates a Temporal Worker pre-configured with StageWorkflow, thin activities,
and the user's adapter registry + pipeline workflows.
"""

from __future__ import annotations

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_pipeline.activities import StageActivities
from temporal_pipeline.adapters import AdapterRegistry, ExternalAdapter
from temporal_pipeline.stage_workflow import StageWorkflow


def create_worker(
    client: Client,
    task_queue: str,
    adapters: dict[str, ExternalAdapter],
    extra_workflows: list | None = None,
) -> Worker:
    """Build a Worker with the pipeline SDK's workflows and activities.

    Parameters
    ----------
    client:
        Connected Temporal client.
    task_queue:
        Task queue name for this worker.
    adapters:
        Mapping of engine name -> concrete adapter instance.
    extra_workflows:
        User-authored pipeline workflow classes to register alongside
        the built-in StageWorkflow.
    """
    registry = AdapterRegistry()
    for engine, adapter in adapters.items():
        registry.register(engine, adapter)

    acts = StageActivities(registry=registry)

    return Worker(
        client,
        task_queue=task_queue,
        workflows=[StageWorkflow] + (extra_workflows or []),
        activities=[
            acts.submit_stage,
            acts.poll_stage,
            acts.collect_stage_artifacts,
            acts.cancel_stage,
            acts.resume_stage,
            acts.collect_debug_bundle,
        ],
    )
