// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/pborman/uuid"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cron"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	decisionAttrValidator struct {
		maxIDLengthLimit int
	}

	decisionBlobSizeChecker struct {
		sizeLimitWarn  int
		sizeLimitError int
		completedID    int64
		mutableState   mutableState
		metricsClient  metrics.Client
		logger         log.Logger
	}
)

func newDecisionAttrValidator(maxIDLengthLimit int) *decisionAttrValidator {
	return &decisionAttrValidator{maxIDLengthLimit: maxIDLengthLimit}
}

func newDecisionBlobSizeChecker(
	sizeLimitWarn int,
	sizeLimitError int,
	completedID int64,
	mutableState mutableState,
	metricsClient metrics.Client,
	logger log.Logger,
) *decisionBlobSizeChecker {
	return &decisionBlobSizeChecker{
		sizeLimitWarn:  sizeLimitWarn,
		sizeLimitError: sizeLimitError,
		completedID:    completedID,
		mutableState:   mutableState,
		metricsClient:  metricsClient,
		logger:         logger,
	}
}

func (c *decisionBlobSizeChecker) failWorkflowIfBlobSizeExceedsLimit(
	blob []byte,
	message string,
) (bool, error) {

	executionInfo := c.mutableState.GetExecutionInfo()
	err := common.CheckEventBlobSizeLimit(
		len(blob),
		c.sizeLimitWarn,
		c.sizeLimitError,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		c.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope),
		c.logger,
	)
	if err == nil {
		return false, nil
	}

	attributes := &workflow.FailWorkflowExecutionDecisionAttributes{
		Reason:  common.StringPtr(common.FailureReasonDecisionBlobSizeExceedsLimit),
		Details: []byte(message),
	}

	if evt := c.mutableState.AddFailWorkflowEvent(c.completedID, attributes); evt == nil {
		return false, &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
	}

	return true, nil
}

func (v *decisionAttrValidator) validateActivityScheduleAttributes(
	attributes *workflow.ScheduleActivityTaskDecisionAttributes,
	wfTimeout int32,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "ScheduleActivityTaskDecisionAttributes is not set on decision."}
	}

	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		return &workflow.BadRequestError{Message: "TaskList is not set on decision."}
	}

	if attributes.GetActivityId() == "" {
		return &workflow.BadRequestError{Message: "ActivityId is not set on decision."}
	}

	if attributes.ActivityType == nil || attributes.ActivityType.GetName() == "" {
		return &workflow.BadRequestError{Message: "ActivityType is not set on decision."}
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	if len(attributes.GetActivityId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityID exceeds length limit."}
	}

	if len(attributes.GetActivityType().GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityType exceeds length limit."}
	}

	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if attributes.GetScheduleToCloseTimeoutSeconds() < 0 || attributes.GetScheduleToStartTimeoutSeconds() < 0 ||
		attributes.GetStartToCloseTimeoutSeconds() < 0 || attributes.GetHeartbeatTimeoutSeconds() < 0 {
		return &workflow.BadRequestError{Message: "A valid timeout may not be negative."}
	}

	// ensure activity timeout never larger than workflow timeout
	if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetScheduleToStartTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetStartToCloseTimeoutSeconds() > wfTimeout {
		attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetHeartbeatTimeoutSeconds() > wfTimeout {
		attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}

	validScheduleToClose := attributes.GetScheduleToCloseTimeoutSeconds() > 0
	validScheduleToStart := attributes.GetScheduleToStartTimeoutSeconds() > 0
	validStartToClose := attributes.GetStartToCloseTimeoutSeconds() > 0

	if validScheduleToClose {
		if !validScheduleToStart {
			attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToCloseTimeoutSeconds())
		}
		if !validStartToClose {
			attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToCloseTimeoutSeconds())
		}
	} else if validScheduleToStart && validStartToClose {
		attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToStartTimeoutSeconds() + attributes.GetStartToCloseTimeoutSeconds())
		if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
			attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return &workflow.BadRequestError{Message: "A valid ScheduleToCloseTimeout is not set on decision."}
	}
	// ensure activity's SCHEDULE_TO_START and SCHEDULE_TO_CLOSE is as long as expiration on retry policy
	p := attributes.RetryPolicy
	if p != nil {
		expiration := p.GetExpirationIntervalInSeconds()
		if expiration == 0 {
			expiration = wfTimeout
		}
		if attributes.GetScheduleToStartTimeoutSeconds() < expiration {
			attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(expiration)
		}
		if attributes.GetScheduleToCloseTimeoutSeconds() < expiration {
			attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(expiration)
		}
	}
	return nil
}

func (v *decisionAttrValidator) validateTimerScheduleAttributes(
	attributes *workflow.StartTimerDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartTimerDecisionAttributes is not set on decision."}
	}
	if attributes.GetTimerId() == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TimerId exceeds length limit."}
	}
	if attributes.GetStartToFireTimeoutSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "A valid StartToFireTimeoutSeconds is not set on decision."}
	}
	return nil
}

func (v *decisionAttrValidator) validateActivityCancelAttributes(
	attributes *workflow.RequestCancelActivityTaskDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelActivityTaskDecisionAttributes is not set on decision."}
	}
	if attributes.GetActivityId() == "" {
		return &workflow.BadRequestError{Message: "ActivityId is not set on decision."}
	}
	if len(attributes.GetActivityId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityId exceeds length limit."}
	}
	return nil
}

func (v *decisionAttrValidator) validateTimerCancelAttributes(
	attributes *workflow.CancelTimerDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "CancelTimerDecisionAttributes is not set on decision."}
	}
	if attributes.GetTimerId() == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TimerId exceeds length limit."}
	}
	return nil
}

func (v *decisionAttrValidator) validateRecordMarkerAttributes(
	attributes *workflow.RecordMarkerDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "RecordMarkerDecisionAttributes is not set on decision."}
	}
	if attributes.GetMarkerName() == "" {
		return &workflow.BadRequestError{Message: "MarkerName is not set on decision."}
	}
	if len(attributes.GetMarkerName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "MarkerName exceeds length limit."}
	}

	return nil
}

func (v *decisionAttrValidator) validateCompleteWorkflowExecutionAttributes(
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "CompleteWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	return nil
}

func (v *decisionAttrValidator) validateFailWorkflowExecutionAttributes(
	attributes *workflow.FailWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "FailWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.Reason == nil {
		return &workflow.BadRequestError{Message: "Reason is not set on decision."}
	}
	return nil
}

func (v *decisionAttrValidator) validateCancelWorkflowExecutionAttributes(
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "CancelWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	return nil
}

func (v *decisionAttrValidator) validateCancelExternalWorkflowExecutionAttributes(
	attributes *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}
	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}
	runID := attributes.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return &workflow.BadRequestError{Message: "Invalid RunId set on decision."}
	}

	return nil
}

func (v *decisionAttrValidator) validateSignalExternalWorkflowExecutionAttributes(
	attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "SignalExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.Execution == nil {
		return &workflow.BadRequestError{Message: "Execution is nil on decision."}
	}
	if attributes.Execution.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}
	if len(attributes.Execution.GetWorkflowId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}

	targetRunID := attributes.Execution.GetRunId()
	if targetRunID != "" && uuid.Parse(targetRunID) == nil {
		return &workflow.BadRequestError{Message: "Invalid RunId set on decision."}
	}
	if attributes.SignalName == nil {
		return &workflow.BadRequestError{Message: "SignalName is not set on decision."}
	}
	if attributes.Input == nil {
		return &workflow.BadRequestError{Message: "Input is not set on decision."}
	}

	return nil
}

func (v *decisionAttrValidator) validateContinueAsNewWorkflowExecutionAttributes(
	executionInfo *persistence.WorkflowExecutionInfo,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	// Inherit workflow type from previous execution if not provided on decision
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)}
	}

	// Inherit Tasklist from previous execution if not provided on decision
	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		attributes.TaskList = &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)}
	}
	if len(attributes.TaskList.GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TaskList exceeds length limit."}
	}
	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
	}

	// Inherit workflow timeout from previous execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from previous execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.DecisionTimeoutValue)
	}

	return nil
}

func (v *decisionAttrValidator) validateStartChildExecutionAttributes(
	parentInfo *persistence.WorkflowExecutionInfo,
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartChildWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	if attributes.GetWorkflowId() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowID is not set on decision."}
	}

	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowType is not set on decision."}
	}

	if attributes.ChildPolicy == nil {
		return &workflow.BadRequestError{Message: "Required field ChildPolicy is not set on decision."}
	}

	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}

	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	if err := cron.ValidateSchedule(attributes.GetCronSchedule()); err != nil {
		return err
	}

	// Inherit tasklist from parent workflow execution if not provided on decision
	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		attributes.TaskList = &workflow.TaskList{Name: common.StringPtr(parentInfo.TaskList)}
	}

	if len(attributes.TaskList.GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TaskList exceeds length limit."}
	}

	// Inherit workflow timeout from parent workflow execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from parent workflow execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.DecisionTimeoutValue)
	}

	return nil
}
