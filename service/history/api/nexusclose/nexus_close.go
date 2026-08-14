package nexusclose

import (
	"context"

	historyi "go.temporal.io/server/service/history/interfaces"
)

// NexusOperationAutoClosePolicy controls what happens to pending async Nexus
// operations when the caller workflow closes.
//
// CONSIDER(stephanos): the prototype reads a single global dynamic-config value at close time,
// so toggling the config retroactively changes the behavior of every in-flight operation. The
// production design stores the policy per-operation on the CHASM Operation component, captured
// from the caller-supplied value at schedule time (see design §3) and read here at close time.
// Capture-at-schedule is a prerequisite for the public per-operation API field; do not switch
// to reading the value live once that field lands.
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

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		// CHASM not enabled or workflow has no CHASM component — nothing to cancel.
		return nil //nolint:nilerr
	}

	return wf.RequestCancelPendingNexusOperations(chasmCtx)
}
