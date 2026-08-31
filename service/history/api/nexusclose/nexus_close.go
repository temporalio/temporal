package nexusclose

import (
	"context"

	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// NexusOperationAutoClosePolicy controls what happens to pending async Nexus operations when the
// caller workflow closes.
//
// CONSIDER(stephanos): the prototype reads one global dynamic-config value at close time. The design
// stores it per-operation, captured at schedule time — a prerequisite for the public API field.
type NexusOperationAutoClosePolicy int32

const (
	// NexusOperationAutoClosePolicyAbandon leaves the handler running. Default.
	NexusOperationAutoClosePolicyAbandon NexusOperationAutoClosePolicy = 0
	// NexusOperationAutoClosePolicyRequestCancel sends a CancelOperation request
	// to the handler for every pending async Nexus operation.
	NexusOperationAutoClosePolicyRequestCancel NexusOperationAutoClosePolicy = 1
)

// CancelPendingNexusOperations requests cancellation of every pending async Nexus operation
// owned by the workflow using the CHASM execution model. It is a no-op unless policy is
// NexusOperationAutoClosePolicyRequestCancel. Must be called while mutable state is still
// writable (i.e. before the workflow close event is added).
func CancelPendingNexusOperations(
	ctx context.Context,
	ms historyi.MutableState,
	policy NexusOperationAutoClosePolicy,
) error {
	if policy != NexusOperationAutoClosePolicyRequestCancel {
		return nil
	}

	wf, chasmCtx, ok, err := chasmWorkflow(ctx, ms)
	if err != nil || !ok {
		return err
	}

	return wf.RequestCancelPendingNexusOperations(chasmCtx)
}

// OnWorkflowReset applies the Nexus auto-close policy to a run that a workflow reset is superseding.
// A reset is a force-close for exactly the operations the reset run does not adopt (scheduled after
// the reset point) and a no-op for the ones it does — except that an adopted operation's pending
// system-initiated cancellation must be called off, since the close that justified it is being
// undone. See Workflow.OnWorkflowReset for the full rule.
//
// adoptedThroughEventID is the reset point (baseRebuildLastEventID) for the reset's base run, and 0
// for any other run being superseded. Unlike CancelPendingNexusOperations this runs regardless of
// policy: the abort half is a correctness fix that applies even under ABANDON, because a cancellation
// created under REQUEST_CANCEL may still be in flight when the policy is later turned off.
func OnWorkflowReset(
	ctx context.Context,
	ms historyi.MutableState,
	policy NexusOperationAutoClosePolicy,
	adoptedThroughEventID int64,
) error {
	if !ms.ChasmEnabled() {
		return nil
	}
	requestCancel := policy == NexusOperationAutoClosePolicyRequestCancel

	// Probe read-only first. A reset walks runs it does not otherwise modify (the continue-as-new
	// chain), and taking a mutable CHASM context on one of those dirties its mutable state for a
	// transaction that never writes it back.
	roWf, roCtx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
	if err != nil {
		return nil //nolint:nilerr // no workflow component — nothing to do.
	}
	if !roWf.NeedsResetHandling(roCtx, adoptedThroughEventID, requestCancel) {
		return nil
	}

	wf, chasmCtx, ok, err := chasmWorkflow(ctx, ms)
	if err != nil || !ok {
		return err
	}

	return wf.OnWorkflowReset(chasmCtx, adoptedThroughEventID, requestCancel)
}

// chasmWorkflow resolves the CHASM workflow component, reporting ok=false when the workflow has no
// CHASM tree. The ChasmEnabled check is not optional: ChasmWorkflowComponent asserts the tree to
// *chasm.Node and panics on the noop tree an HSM-only workflow carries.
func chasmWorkflow(
	ctx context.Context,
	ms historyi.MutableState,
) (*chasmworkflow.Workflow, chasm.MutableContext, bool, error) {
	if !ms.ChasmEnabled() {
		return nil, nil, false, nil
	}
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		// The workflow has a CHASM tree but no workflow component — nothing to cancel.
		return nil, nil, false, nil //nolint:nilerr
	}
	return wf, chasmCtx, true, nil
}
