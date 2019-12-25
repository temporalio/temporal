// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type workflowContext interface {
	getContext() workflowExecutionContext
	getMutableState() mutableState
	reloadMutableState() (mutableState, error)
	getReleaseFn() releaseWorkflowExecutionFunc
	getWorkflowID() string
	getRunID() string
}

type workflowContextImpl struct {
	context      workflowExecutionContext
	mutableState mutableState
	releaseFn    releaseWorkflowExecutionFunc
}

type updateWorkflowAction struct {
	noop           bool
	createDecision bool
}

var (
	updateWorkflowWithNewDecision = &updateWorkflowAction{
		createDecision: true,
	}
	updateWorkflowWithoutDecision = &updateWorkflowAction{
		createDecision: false,
	}
)

type updateWorkflowActionFunc func(workflowExecutionContext, mutableState) (*updateWorkflowAction, error)

func (w *workflowContextImpl) getContext() workflowExecutionContext {
	return w.context
}

func (w *workflowContextImpl) getMutableState() mutableState {
	return w.mutableState
}

func (w *workflowContextImpl) reloadMutableState() (mutableState, error) {
	mutableState, err := w.getContext().loadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	w.mutableState = mutableState
	return mutableState, nil
}

func (w *workflowContextImpl) getReleaseFn() releaseWorkflowExecutionFunc {
	return w.releaseFn
}

func (w *workflowContextImpl) getWorkflowID() string {
	return w.getContext().getExecution().GetWorkflowId()
}

func (w *workflowContextImpl) getRunID() string {
	return w.getContext().getExecution().GetRunId()
}

func newWorkflowContext(
	context workflowExecutionContext,
	releaseFn releaseWorkflowExecutionFunc,
	mutableState mutableState,
) *workflowContextImpl {

	return &workflowContextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func failDecision(
	mutableState mutableState,
	decision *decisionInfo,
	decisionFailureCause workflow.DecisionTaskFailedCause,
) error {

	if _, err := mutableState.AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		decisionFailureCause,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	return mutableState.FlushBufferedEvents()
}

func scheduleDecision(
	mutableState mutableState,
) error {

	if mutableState.HasPendingDecision() {
		return nil
	}

	_, err := mutableState.AddDecisionTaskScheduledEvent(false)
	if err != nil {
		return &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
	}
	return nil
}

func retryWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
	parentDomainName string,
	continueAsNewAttributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes,
) (mutableState, error) {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			workflow.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return nil, err
		}
	}

	_, newMutableState, err := mutableState.AddContinueAsNewEvent(
		eventBatchFirstEventID,
		common.EmptyEventID,
		parentDomainName,
		continueAsNewAttributes,
	)
	if err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func timeoutWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			workflow.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
	)
	return err
}

func terminateWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
	terminateReason string,
	terminateDetails []byte,
	terminateIdentity string,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			workflow.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
	)
	return err
}
