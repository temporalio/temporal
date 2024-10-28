// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	AbortReasonRegistryCleared AbortReason = iota + 1
	AbortReasonWorkflowCompleted
	AbortReasonWorkflowContinuing
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

	// If the Workflow is completed, then pre-accepted Updates are aborted with non-retryable ErrWorkflowCompleted error.
	reasonState{r: AbortReasonWorkflowCompleted, st: stateCreated}:               {f: nil, err: consts.ErrWorkflowCompleted},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateProvisionallyAdmitted}: {f: nil, err: consts.ErrWorkflowCompleted},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateAdmitted}:              {f: nil, err: consts.ErrWorkflowCompleted},
	reasonState{r: AbortReasonWorkflowCompleted, st: stateSent}:                  {f: nil, err: consts.ErrWorkflowCompleted},
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
