"""System-owned StageWorkflow — one durable entity per logical stage execution.

Lifecycle:
    CREATED -> SUBMITTED -> RUNNING -> COMMITTING_OUTPUTS -> SUCCEEDED
                            RUNNING -> FAILED_TRANSIENT  (retry with backoff)
                            RUNNING -> BLOCKED_ON_BUG    (wait for repair)

External adapters push progress via signals.  Users apply repairs via updates.
Queries expose current state without mutating history.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline.models import (
        ArtifactManifest,
        BugRecord,
        ExternalJobRef,
        ExternalJobState,
        RepairPlan,
        StageCarryOver,
        StageHandle,
        StageSpec,
        StageState,
    )

_MAX_EVENTS_BEFORE_CAN = 500
_POLL_INTERVAL = timedelta(seconds=30)
_MAX_TRANSIENT_RETRIES = 5


@workflow.defn
class StageWorkflow:
    """Durable control object around one external-job-backed stage."""

    def __init__(self) -> None:
        self._state: StageState = StageState.CREATED
        self._spec: StageSpec | None = None
        self._job_ref: ExternalJobRef | None = None
        self._manifest: ArtifactManifest | None = None
        self._bug: BugRecord | None = None
        self._cancel_requested: bool = False
        self._repair_plan: RepairPlan | None = None
        self._event_count: int = 0

    # -- queries -------------------------------------------------------------

    @workflow.query
    def get_state(self) -> StageState:
        return self._state

    @workflow.query
    def get_manifest(self) -> ArtifactManifest | None:
        return self._manifest

    @workflow.query
    def get_bug_record(self) -> BugRecord | None:
        return self._bug

    # -- signals (from external monitors) ------------------------------------

    @workflow.signal
    async def signal_heartbeat(self, progress: float) -> None:
        self._event_count += 1

    @workflow.signal
    async def signal_external_event(self, event: dict) -> None:
        self._event_count += 1

    # -- updates (from users / repair engine) --------------------------------

    @workflow.update
    async def apply_repair(self, plan: RepairPlan) -> StageHandle:
        if self._state not in (
            StageState.BLOCKED_ON_BUG,
            StageState.FAILED_TRANSIENT,
            StageState.PAUSED_FOR_DECISION,
        ):
            raise ValueError(
                f"Cannot apply repair in state {self._state}"
            )
        self._repair_plan = plan
        self._state = StageState.CREATED
        return StageHandle(
            stage_name=self._spec.name if self._spec else "",
            workflow_id=workflow.info().workflow_id,
            run_id=workflow.info().run_id,
            manifest=self._manifest,
        )

    @workflow.update
    async def request_cancel(self) -> None:
        self._cancel_requested = True

    # -- main run ------------------------------------------------------------

    @workflow.run
    async def run(self, input: StageSpec | StageCarryOver) -> StageHandle:
        if isinstance(input, StageCarryOver):
            self._restore(input)
        else:
            self._spec = input

        return await self._execute()

    # -- internals -----------------------------------------------------------

    async def _execute(self) -> StageHandle:
        assert self._spec is not None

        if self._state == StageState.CREATED:
            await self._submit()

        if self._state == StageState.SUBMITTED:
            self._state = StageState.RUNNING

        retries = 0
        while self._state == StageState.RUNNING:
            if self._cancel_requested:
                await self._do_cancel()
                break

            job_state = await workflow.execute_activity(
                "submit_stage" if self._job_ref is None else "poll_stage",
                self._job_ref,
                start_to_close_timeout=timedelta(seconds=60),
            )

            if not isinstance(job_state, ExternalJobState):
                job_state = ExternalJobState(**job_state) if isinstance(job_state, dict) else job_state

            self._event_count += 1

            if job_state.state == StageState.SUCCEEDED:
                await self._commit_outputs()
                break
            elif job_state.state == StageState.FAILED_TRANSIENT:
                retries += 1
                if retries >= _MAX_TRANSIENT_RETRIES:
                    self._state = StageState.BLOCKED_ON_BUG
                    self._bug = BugRecord(
                        stage_name=self._spec.name,
                        error_type="MAX_RETRIES_EXCEEDED",
                        error_message=job_state.message,
                    )
                    break
                self._state = StageState.FAILED_TRANSIENT
                await asyncio.sleep(0)
                self._state = StageState.RUNNING
            elif job_state.state == StageState.BLOCKED_ON_BUG:
                self._state = StageState.BLOCKED_ON_BUG
                self._bug = BugRecord(
                    stage_name=self._spec.name,
                    error_type="EXTERNAL_BUG",
                    error_message=job_state.message,
                )
                break
            else:
                await workflow.wait_condition(
                    lambda: self._cancel_requested or self._event_count > 0,
                    timeout=_POLL_INTERVAL,
                )

            self._maybe_continue_as_new()

        if self._state == StageState.BLOCKED_ON_BUG:
            await workflow.wait_condition(
                lambda: self._repair_plan is not None or self._cancel_requested,
            )
            if self._cancel_requested:
                await self._do_cancel()
            elif self._repair_plan is not None:
                return await self._execute()

        return StageHandle(
            stage_name=self._spec.name,
            workflow_id=workflow.info().workflow_id,
            run_id=workflow.info().run_id,
            manifest=self._manifest,
        )

    async def _submit(self) -> None:
        assert self._spec is not None
        self._state = StageState.SUBMITTED
        self._job_ref = await workflow.execute_activity(
            "submit_stage",
            self._spec,
            start_to_close_timeout=timedelta(seconds=120),
        )
        if isinstance(self._job_ref, dict):
            self._job_ref = ExternalJobRef(**self._job_ref)

    async def _commit_outputs(self) -> None:
        assert self._job_ref is not None
        self._state = StageState.COMMITTING_OUTPUTS
        self._manifest = await workflow.execute_activity(
            "collect_stage_artifacts",
            self._job_ref,
            start_to_close_timeout=timedelta(seconds=120),
        )
        if isinstance(self._manifest, dict):
            self._manifest = ArtifactManifest(**self._manifest)
        self._state = StageState.SUCCEEDED

    async def _do_cancel(self) -> None:
        if self._job_ref is not None:
            await workflow.execute_activity(
                "cancel_stage",
                self._job_ref,
                start_to_close_timeout=timedelta(seconds=60),
            )
        self._state = StageState.PAUSED_FOR_DECISION

    def _maybe_continue_as_new(self) -> None:
        if self._event_count >= _MAX_EVENTS_BEFORE_CAN:
            workflow.continue_as_new(
                StageCarryOver(
                    spec=self._spec,
                    state=self._state,
                    job_ref=self._job_ref,
                    manifest=self._manifest,
                    bug=self._bug,
                    event_count=0,
                )
            )

    def _restore(self, carry: StageCarryOver) -> None:
        self._spec = carry.spec
        self._state = carry.state
        self._job_ref = carry.job_ref
        self._manifest = carry.manifest
        self._bug = carry.bug
        self._event_count = carry.event_count
