package update

import (
	"fmt"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/service/history/consts"
)

type (
	AbortReason uint32

	reasonState struct {
		r  AbortReason
		st state
	}

	failureError struct {
		f   *failurepb.Failure
		err error
	}
)

const (
	// For more details on these reasons, check the comments in reasonStateMatrix bellow.

	AbortReasonRegistryCleared AbortReason = iota + 1
	AbortReasonWorkflowCompleted
	AbortReasonWorkflowContinuing
	AbortReasonWorkflowTaskFailed
	lastAbortReason
)

// Matrix of "abort reason/Update state" to "failure/error" pair. Only one value (failure or error) is allowed per pair.
var reasonStateMatrix = map[reasonState]failureError{
	// If the registry is cleared, then all Updates (no matter what state they are)
	// are aborted with retryable registryClearedErr error.
	reasonState{r: AbortReasonRegistryCleared, st: stateCreated}:                             {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateProvisionallyAdmitted}:               {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateAdmitted}:                            {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateSent}:                                {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateProvisionallyAccepted}:               {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateAccepted}:                            {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateProvisionallyCompleted}:              {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonRegistryCleared, st: stateProvisionallyCompletedAfterAccepted}: {f: nil, err: registryClearedErr},
	// Completed Updates can't be aborted.
	reasonState{r: AbortReasonRegistryCleared, st: stateCompleted}:            {f: nil, err: nil},
	reasonState{r: AbortReasonRegistryCleared, st: stateProvisionallyAborted}: {f: nil, err: nil},
	reasonState{r: AbortReasonRegistryCleared, st: stateAborted}:              {f: nil, err: nil},

	// If the Workflow is completed, then pre-accepted Updates are aborted with non-retryable error.
	reasonState{r: AbortReasonWorkflowCompleted, st: stateCreated}:               {f: nil, err: AbortedByWorkflowClosingErr},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyAdmitted}: {f: nil, err: AbortedByWorkflowClosingErr},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateAdmitted}:              {f: nil, err: AbortedByWorkflowClosingErr},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateSent}:                  {f: nil, err: AbortedByWorkflowClosingErr},
	// Accepted Updates are failed with special server failure because if a client knows that Update has been accepted,
	// it expects any following requests to return an Update result (or failure) but not an error.
	// There can be different types of Update failures coming from worker and a client must handle them anyway.
	// It is easier and less error-prone for a client to handle only Update failures instead of both failures and
	// not obvious NotFound errors in case if the Workflow completes before the Update completes.
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyAccepted}:               {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateAccepted}:                            {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyCompleted}:              {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyCompletedAfterAccepted}: {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	// Completed Updates can't be aborted.
	reasonState{r: AbortReasonWorkflowCompleted, st: stateCompleted}:            {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyAborted}: {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateAborted}:              {f: nil, err: nil},

	// If Workflow is starting new run, then all Updates are aborted with retryable ErrWorkflowClosing error.
	// Internal retries will send them to the new run.
	reasonState{r: AbortReasonWorkflowContinuing, st: stateCreated}:               {f: nil, err: consts.ErrWorkflowClosing},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateProvisionallyAdmitted}: {f: nil, err: consts.ErrWorkflowClosing},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateAdmitted}:              {f: nil, err: consts.ErrWorkflowClosing},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateSent}:                  {f: nil, err: consts.ErrWorkflowClosing},
	// Accepted Update can't be applied to the new run, and must be failed same way as if Workflow is completed.
	reasonState{r: AbortReasonWorkflowContinuing, st: stateProvisionallyAccepted}:               {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateAccepted}:                            {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateProvisionallyCompleted}:              {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateProvisionallyCompletedAfterAccepted}: {f: acceptedUpdateCompletedWorkflowFailure, err: nil},
	// Completed Updates can't be aborted.
	reasonState{r: AbortReasonWorkflowContinuing, st: stateCompleted}:            {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateProvisionallyAborted}: {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowContinuing, st: stateAborted}:              {f: nil, err: nil},

	// AbortReasonWorkflowTaskFailed reason is used when the WFT fails unexpectedly,
	// for example, during completion (call to RespondWorkflowTaskCompleted API)
	// - not when WFT is explicitly failed by SDK (call to RespondWorkflowTaskFailed API).
	// Updates which have *not* been seen by the Workflow are aborted with a retryable error.
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateCreated}:               {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateProvisionallyAdmitted}: {f: nil, err: registryClearedErr},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateAdmitted}:              {f: nil, err: registryClearedErr},
	// Updates which *have* been seen by the Workflow are aborted with non-retryable error.
	// Failed WFT will be retried but Update must not. Otherwise, internal retries will exhaust and Unavailable error will be returned to the client.
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateSent}: {f: nil, err: workflowTaskFailErr},
	// Updates which passed Accepted state are not retried when the registry is cleared, so there is no need to abort them.
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateProvisionallyAccepted}:               {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateAccepted}:                            {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateProvisionallyCompleted}:              {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateProvisionallyCompletedAfterAccepted}: {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateCompleted}:                           {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateProvisionallyAborted}:                {f: nil, err: nil},
	reasonState{r: AbortReasonWorkflowTaskFailed, st: stateAborted}:                             {f: nil, err: nil},
}

// FailureError returns failure or error which will be set on Update futures while aborting Update.
// Only one of the return values will be non-nil.
func (r AbortReason) FailureError(st state) (*failurepb.Failure, error) {
	fe, ok := reasonStateMatrix[reasonState{r: r, st: st}]
	if !ok {
		panic(fmt.Sprintf("unknown workflow update abort reason %s or update state %s", r, st))
	}
	return fe.f, fe.err
}

func (r AbortReason) String() string {
	switch r {
	case AbortReasonRegistryCleared:
		return "RegistryCleared"
	case AbortReasonWorkflowCompleted:
		return "WorkflowCompleted"
	case AbortReasonWorkflowContinuing:
		return "WorkflowContinuing"
	case lastAbortReason:
		return fmt.Sprintf("invalid reason %d", r)
	}
	return fmt.Sprintf("unrecognized reason %d", r)
}
