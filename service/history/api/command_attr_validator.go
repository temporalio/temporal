// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package api

import (
	"fmt"
	"strings"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	CommandAttrValidator struct {
		namespaceRegistry               namespace.Registry
		config                          *configs.Config
		maxIDLengthLimit                int
		searchAttributesValidator       *searchattribute.Validator
		getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
		getDefaultWorkflowRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
		enableCrossNamespaceCommands    dynamicconfig.BoolPropertyFn
	}
)

func NewCommandAttrValidator(
	namespaceRegistry namespace.Registry,
	config *configs.Config,
	searchAttributesValidator *searchattribute.Validator,
) *CommandAttrValidator {
	return &CommandAttrValidator{
		namespaceRegistry:               namespaceRegistry,
		config:                          config,
		maxIDLengthLimit:                config.MaxIDLengthLimit(),
		searchAttributesValidator:       searchAttributesValidator,
		getDefaultActivityRetrySettings: config.DefaultActivityRetryPolicy,
		getDefaultWorkflowRetrySettings: config.DefaultWorkflowRetryPolicy,
		enableCrossNamespaceCommands:    config.EnableCrossNamespaceCommands,
	}
}

func (v *CommandAttrValidator) ValidateProtocolMessageAttributes(
	namespaceID namespace.ID,
	attributes *commandpb.ProtocolMessageCommandAttributes,
	runTimeout *durationpb.Duration,
) (enumspb.WorkflowTaskFailedCause, error) {
	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("ProtocolMessageCommandAttributes is not set on ProtocolMessageCommand.")
	}

	if attributes.MessageId == "" {
		return failedCause, serviceerror.NewInvalidArgument("MessageID is not set on ProtocolMessageCommand.")
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

//nolint:revive
func (v *CommandAttrValidator) ValidateActivityScheduleAttributes(
	namespaceID namespace.ID,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
	runTimeout *durationpb.Duration,
) (enumspb.WorkflowTaskFailedCause, error) {
	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("ScheduleActivityTaskCommandAttributes is not set on ScheduleActivityTaskCommand.")
	}

	// The typescript SDK requires that the ScheduleToStart and/or the ScheduleToClose timeouts are non-nil.
	// Since we override those using the potentially-nil run timeout we need to make sure it is always non-nil
	if runTimeout == nil {
		runTimeout = durationpb.New(0)
	}

	activityID := attributes.GetActivityId()
	activityType := ""
	if attributes.ActivityType != nil {
		activityType = attributes.ActivityType.GetName()
	}

	if err := tqid.NormalizeAndValidate(attributes.TaskQueue, "", v.maxIDLengthLimit); err != nil {
		return failedCause, fmt.Errorf("invalid TaskQueue on ScheduleActivityTaskCommand: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}

	if activityID == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("ActivityId is not set on ScheduleActivityTaskCommand. ActivityType=%s", activityType))
	}
	if activityType == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("ActivityType is not set on ScheduleActivityTaskCommand. ActivityID=%s", activityID))
	}
	if attributes.RetryPolicy == nil {
		attributes.RetryPolicy = &commonpb.RetryPolicy{}
	}

	if err := v.validateActivityRetryPolicy(namespaceID, attributes.RetryPolicy); err != nil {
		return failedCause, fmt.Errorf("invalid ActivityRetryPolicy on SechduleActivityTaskCommand: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}
	if len(activityID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("ActivityId on ScheduleActivityTaskCommand exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d", activityID, activityType, len(activityID), v.maxIDLengthLimit))
	}
	if len(activityType) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("ActivityType on ScheduleActivityTaskCommand exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d", activityID, activityType, len(activityType), v.maxIDLengthLimit))
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if err := timestamp.ValidateProtoDuration(attributes.GetScheduleToCloseTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid ScheduleToCloseTimeout for ScheduleActivityTaskCommand: %v. ActivityId=%s ActivityType=%s", err, activityID, activityType))
	}
	if err := timestamp.ValidateProtoDuration(attributes.GetScheduleToStartTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid ScheduleToStartTimeout for ScheduleActivityTaskCommand: %v. ActivityId=%s ActivityType=%s", err, activityID, activityType))
	}
	if err := timestamp.ValidateProtoDuration(attributes.GetStartToCloseTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid StartToCloseTimeout for ScheduleActivityTaskCommand: %v. ActivityId=%s ActivityType=%s", err, activityID, activityType))
	}
	if err := timestamp.ValidateProtoDuration(attributes.GetHeartbeatTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid HeartbeatTimeout for ScheduleActivityTaskCommand: %v. ActivityId=%s ActivityType=%s", err, activityID, activityType))
	}

	ScheduleToCloseSet := attributes.GetScheduleToCloseTimeout().AsDuration() > 0
	ScheduleToStartSet := attributes.GetScheduleToStartTimeout().AsDuration() > 0
	StartToCloseSet := attributes.GetStartToCloseTimeout().AsDuration() > 0

	if ScheduleToCloseSet {
		if ScheduleToStartSet {
			attributes.ScheduleToStartTimeout = timestamp.MinDurationPtr(attributes.GetScheduleToStartTimeout(),
				attributes.GetScheduleToCloseTimeout())
		} else {
			attributes.ScheduleToStartTimeout = attributes.GetScheduleToCloseTimeout()
		}
		if StartToCloseSet {
			attributes.StartToCloseTimeout = timestamp.MinDurationPtr(attributes.GetStartToCloseTimeout(),
				attributes.GetScheduleToCloseTimeout())
		} else {
			attributes.StartToCloseTimeout = attributes.GetScheduleToCloseTimeout()
		}
	} else if StartToCloseSet {
		// We are in !validScheduleToClose due to the first if above
		attributes.ScheduleToCloseTimeout = runTimeout
		if !ScheduleToStartSet {
			attributes.ScheduleToStartTimeout = runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand. ActivityId=%s ActivityType=%s", activityID, activityType))
	}
	// ensure activity timeout never larger than workflow timeout
	if runTimeout.AsDuration() > 0 {
		runTimeoutDur := runTimeout.AsDuration()
		if attributes.GetScheduleToCloseTimeout().AsDuration() > runTimeoutDur {
			attributes.ScheduleToCloseTimeout = runTimeout
		}
		if attributes.GetScheduleToStartTimeout().AsDuration() > runTimeoutDur {
			attributes.ScheduleToStartTimeout = runTimeout
		}
		if attributes.GetStartToCloseTimeout().AsDuration() > runTimeoutDur {
			attributes.StartToCloseTimeout = runTimeout
		}
		if attributes.GetHeartbeatTimeout().AsDuration() > runTimeoutDur {
			attributes.HeartbeatTimeout = runTimeout
		}
	}
	attributes.HeartbeatTimeout = timestamp.MinDurationPtr(attributes.GetHeartbeatTimeout(), attributes.GetStartToCloseTimeout())

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateTimerScheduleAttributes(
	attributes *commandpb.StartTimerCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("StartTimerCommandAttributes is not set on StartTimerCommand.")
	}

	timerID := attributes.GetTimerId()

	if timerID == "" {
		return failedCause, serviceerror.NewInvalidArgument("TimerId is not set on StartTimerCommand.")
	}
	if len(timerID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("TimerId on StartTimerCommand exceeds length limit. TimerId=%s Length=%d Limit=%d", timerID, len(timerID), v.maxIDLengthLimit))
	}
	if err := common.ValidateUTF8String("TimerId", timerID); err != nil {
		return failedCause, err
	}
	if err := timestamp.ValidateProtoDuration(attributes.GetStartToFireTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("An invalid StartToFireTimeout is set on StartTimerCommand: %v. TimerId=%s", err, timerID))
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateActivityCancelAttributes(
	attributes *commandpb.RequestCancelActivityTaskCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("RequestCancelActivityTaskCommandAttributes is not set on RequestCancelActivityTaskCommand.")
	}
	if attributes.GetScheduledEventId() <= 0 {
		return failedCause, serviceerror.NewInvalidArgument("ScheduledEventId is not set on RequestCancelActivityTaskCommand.")
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateTimerCancelAttributes(
	attributes *commandpb.CancelTimerCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("CancelTimerCommandAttributes is not set on CancelTimerCommand.")
	}

	timerID := attributes.GetTimerId()

	if timerID == "" {
		return failedCause, serviceerror.NewInvalidArgument("TimerId is not set on CancelTimerCommand.")
	}
	if len(timerID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("TimerId on CancelTimerCommand exceeds length limit. TimerId=%s Length=%d Limit=%d", timerID, len(timerID), v.maxIDLengthLimit))
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateRecordMarkerAttributes(
	attributes *commandpb.RecordMarkerCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("RecordMarkerCommandAttributes is not set on RecordMarkerCommand.")
	}

	markerName := attributes.GetMarkerName()
	if markerName == "" {
		return failedCause, serviceerror.NewInvalidArgument("MarkerName is not set on RecordMarkerCommand.")
	}
	if len(markerName) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("MarkerName on RecordMarkerCommand exceeds length limit. MarkerName=%s Length=%d Limit=%d", markerName, len(markerName), v.maxIDLengthLimit))
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateCompleteWorkflowExecutionAttributes(
	attributes *commandpb.CompleteWorkflowExecutionCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("CompleteWorkflowExecutionCommandAttributes is not set on CompleteWorkflowExecutionCommand.")
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateFailWorkflowExecutionAttributes(
	attributes *commandpb.FailWorkflowExecutionCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("FailWorkflowExecutionCommandAttributes is not set on FailWorkflowExecutionCommand.")
	}
	if attributes.GetFailure() == nil {
		return failedCause, serviceerror.NewInvalidArgument("Failure is not set on FailWorkflowExecutionCommand.")
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateCancelWorkflowExecutionAttributes(
	attributes *commandpb.CancelWorkflowExecutionCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("CancelWorkflowExecutionCommandAttributes is not set on CancelWorkflowExecutionCommand.")
	}
	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateCancelExternalWorkflowExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	initiatedChildExecutionsInSession map[string]struct{},
	attributes *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES
	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return failedCause, err
	}

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("RequestCancelExternalWorkflowExecutionCommandAttributes is not set on RequestCancelExternalWorkflowExecutionCommand.")
	}

	workflowID := attributes.GetWorkflowId()
	ns := attributes.GetNamespace()
	runID := attributes.GetRunId()

	if workflowID == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowId is not set on RequestCancelExternalWorkflowExecutionCommand. Namespace=%s RunId=%s", ns, runID))
	}
	if len(ns) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Namespace on RequestCancelExternalWorkflowExecutionCommand exceeds length limit. WorkflowId=%s RunId=%s Namespace=%s Length=%d Limit=%d", workflowID, runID, ns, len(ns), v.maxIDLengthLimit))
	}
	if len(workflowID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowId on RequestCancelExternalWorkflowExecutionCommand exceeds length limit. WorkflowId=%s Length=%d Limit=%d RunId=%s Namespace=%s", workflowID, len(workflowID), v.maxIDLengthLimit, runID, ns))
	}
	if err := common.ValidateUTF8String("WorkflowId", workflowID); err != nil {
		return failedCause, err
	}
	if runID != "" && uuid.Parse(runID) == nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid RunId set on RequestCancelExternalWorkflowExecutionCommand. WorkflowId=%s RunId=%s Namespace=%s", workflowID, runID, ns))
	}
	if _, ok := initiatedChildExecutionsInSession[workflowID]; ok {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Start and RequestCancel for child workflow is not allowed in same workflow task. WorkflowId=%s RunId=%s Namespace=%s", workflowID, runID, ns))
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateSignalExternalWorkflowExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES
	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return failedCause, err
	}

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("SignalExternalWorkflowExecutionCommandAttributes is not set on SignalExternalWorkflowExecutionCommand.")
	}
	if attributes.Execution == nil {
		return failedCause, serviceerror.NewInvalidArgument("Execution is not set on SignalExternalWorkflowExecutionCommand.")
	}

	workflowID := attributes.Execution.GetWorkflowId()
	ns := attributes.GetNamespace()
	targetRunID := attributes.Execution.GetRunId()
	signalName := attributes.GetSignalName()

	if workflowID == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowId is not set on SignalExternalWorkflowExecutionCommand. Namespace=%s RunId=%s SignalName=%s", ns, targetRunID, signalName))
	}
	if len(ns) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Namespace on SignalExternalWorkflowExecutionCommand exceeds length limit. WorkflowId=%s Namespace=%s Length=%d Limit=%d RunId=%s SignalName=%s", workflowID, ns, len(ns), v.maxIDLengthLimit, targetRunID, signalName))
	}
	if len(workflowID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowId on SignalExternalWorkflowExecutionCommand exceeds length limit. WorkflowId=%s Length=%d Limit=%d Namespace=%s RunId=%s SignalName=%s", workflowID, len(workflowID), v.maxIDLengthLimit, ns, targetRunID, signalName))
	}
	if err := common.ValidateUTF8String("WorkflowId", workflowID); err != nil {
		return failedCause, err
	}
	if targetRunID != "" && uuid.Parse(targetRunID) == nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid RunId set on SignalExternalWorkflowExecutionCommand. WorkflowId=%s Namespace=%s RunId=%s SignalName=%s", workflowID, ns, targetRunID, signalName))
	}
	if attributes.GetSignalName() == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("SignalName is not set on SignalExternalWorkflowExecutionCommand. WorkflowId=%s Namespace=%s RunId=%s", workflowID, ns, targetRunID))
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateUpsertWorkflowSearchAttributes(
	namespaceName namespace.Name,
	attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("UpsertWorkflowSearchAttributesCommandAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	}
	if attributes.SearchAttributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("SearchAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	}
	if len(attributes.GetSearchAttributes().GetIndexedFields()) == 0 {
		return failedCause, serviceerror.NewInvalidArgument("IndexedFields is not set on UpsertWorkflowSearchAttributesCommand.")
	}
	if err := v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), namespaceName.String()); err != nil {
		return failedCause, err
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateModifyWorkflowProperties(
	attributes *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) (enumspb.WorkflowTaskFailedCause, error) {
	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("ModifyWorkflowPropertiesCommandAttributes is not set on ModifyWorkflowPropertiesCommand.")
	}

	if attributes.UpsertedMemo == nil {
		return failedCause, serviceerror.NewInvalidArgument("UpsertedMemo is not set on ModifyWorkflowPropertiesCommand.")
	}

	if len(attributes.GetUpsertedMemo().GetFields()) == 0 {
		return failedCause, serviceerror.NewInvalidArgument("UpsertedMemo.Fields is not set on ModifyWorkflowPropertiesCommand.")
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateContinueAsNewWorkflowExecutionAttributes(
	namespaceName namespace.Name,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES
	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("ContinueAsNewWorkflowExecutionCommandAttributes is not set on ContinueAsNewWorkflowExecutionCommand.")
	}

	// Inherit workflow type from previous execution if not provided on command
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName}
	}

	wfType := attributes.WorkflowType.GetName()
	if len(wfType) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowType on ContinueAsNewWorkflowExecutionCommand exceeds length limit. WorkflowType=%s Length=%d Limit=%d", wfType, len(wfType), v.maxIDLengthLimit))
	}

	// Inherit task queue from previous execution if not provided on command
	if attributes.TaskQueue == nil {
		attributes.TaskQueue = &taskqueuepb.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
	}
	if err := tqid.NormalizeAndValidate(attributes.TaskQueue, executionInfo.TaskQueue, v.maxIDLengthLimit); err != nil {
		return failedCause, fmt.Errorf("error validating ContinueAsNewWorkflowExecutionCommand TaskQueue: %w. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue)
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetWorkflowRunTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid WorkflowRunTimeout on ContinueAsNewWorkflowExecutionCommand: %v. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue))
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetWorkflowTaskTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid WorkflowTaskTimeout on ContinueAsNewWorkflowExecutionCommand: %v. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue))
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetBackoffStartInterval()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid BackoffStartInterval on ContinueAsNewWorkflowExecutionCommand: %v. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue))
	}

	if attributes.GetWorkflowRunTimeout().AsDuration() == 0 {
		attributes.WorkflowRunTimeout = executionInfo.WorkflowRunTimeout
	}

	if attributes.GetWorkflowTaskTimeout().AsDuration() == 0 {
		attributes.WorkflowTaskTimeout = executionInfo.DefaultWorkflowTaskTimeout
	}

	attributes.WorkflowRunTimeout = durationpb.New(common.OverrideWorkflowRunTimeout(attributes.GetWorkflowRunTimeout().AsDuration(), executionInfo.GetWorkflowExecutionTimeout().AsDuration()))

	attributes.WorkflowTaskTimeout = durationpb.New(common.OverrideWorkflowTaskTimeout(namespaceName.String(), attributes.GetWorkflowTaskTimeout().AsDuration(), attributes.GetWorkflowRunTimeout().AsDuration(), v.config.DefaultWorkflowTaskTimeout))

	if err := v.validateWorkflowRetryPolicy(namespaceName, attributes.RetryPolicy); err != nil {
		return failedCause, fmt.Errorf("invalid WorkflowRetryPolicy on ContinueAsNewWorkflowExecutionCommand: %w. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue)
	}

	if err := v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), namespaceName.String()); err != nil {
		return enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fmt.Errorf("invalid SearchAttributes on ContinueAsNewWorkflowExecutionCommand: %w. WorkflowType=%s TaskQueue=%s", err, wfType, attributes.TaskQueue)
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) ValidateStartChildExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	targetNamespace namespace.Name,
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
	parentInfo *persistencespb.WorkflowExecutionInfo,
	defaultWorkflowTaskTimeoutFn dynamicconfig.DurationPropertyFnWithNamespaceFilter,
) (enumspb.WorkflowTaskFailedCause, error) {

	const failedCause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES
	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return failedCause, err
	}

	if attributes == nil {
		return failedCause, serviceerror.NewInvalidArgument("StartChildWorkflowExecutionCommandAttributes is not set on StartChildWorkflowExecutionCommand.")
	}

	wfID := attributes.GetWorkflowId()
	wfType := ""
	if attributes.WorkflowType != nil {
		wfType = attributes.WorkflowType.GetName()
	}
	ns := attributes.GetNamespace()

	if wfID == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Required field WorkflowId is not set on StartChildWorkflowExecutionCommand. WorkflowType=%s Namespace=%s", wfType, ns))
	}

	if wfType == "" {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Required field WorkflowType is not set on StartChildWorkflowExecutionCommand. WorkflowId=%s Namespace=%s", wfID, ns))
	}

	if len(ns) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Namespace on StartChildWorkflowExecutionCommand exceeds length limit. WorkflowId=%s WorkflowType=%s Namespace=%s Length=%d Limit=%d", wfID, wfType, ns, len(ns), v.maxIDLengthLimit))
	}

	if len(wfID) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowId on StartChildWorkflowExecutionCommand exceeds length limit. WorkflowId=%s Length=%d Limit=%d WorkflowType=%s Namespace=%s", wfID, len(wfID), v.maxIDLengthLimit, wfType, ns))
	}

	if len(wfType) > v.maxIDLengthLimit {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("WorkflowType on StartChildWorkflowExecutionCommand exceeds length limit. WorkflowId=%s WorkflowType=%s Length=%d Limit=%d Namespace=%s", wfID, wfType, len(wfType), v.maxIDLengthLimit, ns))
	}

	if err := common.ValidateUTF8String("WorkflowId", wfID); err != nil {
		return failedCause, err
	}

	if err := common.ValidateUTF8String("WorkflowType", wfType); err != nil {
		return failedCause, err
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetWorkflowExecutionTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid WorkflowExecutionTimeout on StartChildWorkflowExecutionCommand: %v. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns))
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetWorkflowRunTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid WorkflowRunTimeout on StartChildWorkflowExecutionCommand: %v. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns))
	}

	if err := timestamp.ValidateProtoDuration(attributes.GetWorkflowTaskTimeout()); err != nil {
		return failedCause, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid WorkflowTaskTimeout on StartChildWorkflowExecutionCommand: %v. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns))
	}

	if err := v.validateWorkflowRetryPolicy(namespace.Name(attributes.GetNamespace()), attributes.RetryPolicy); err != nil {
		return failedCause, fmt.Errorf("invalid WorkflowRetryPolicy on StartChildWorkflowExecutionCommand: %w. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns)
	}

	if err := backoff.ValidateSchedule(attributes.GetCronSchedule()); err != nil {
		return failedCause, fmt.Errorf("invalid CronSchedule on StartChildWorkflowExecutionCommand: %w. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns)
	}

	if err := v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), targetNamespace.String()); err != nil {
		return enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fmt.Errorf("invalid SearchAttributes on StartChildWorkflowCommand: %w. WorkflowId=%s WorkflowType=%s Namespace=%s", err, wfID, wfType, ns)
	}

	// Inherit taskqueue from parent workflow execution if not provided on command
	if attributes.TaskQueue == nil {
		attributes.TaskQueue = &taskqueuepb.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
	}
	if err := tqid.NormalizeAndValidate(attributes.TaskQueue, parentInfo.TaskQueue, v.maxIDLengthLimit); err != nil {
		return failedCause, fmt.Errorf("invalid TaskQueue on StartChildWorkflowExecutionCommand: %w. WorkflowId=%s WorkflowType=%s Namespace=%s TaskQueue=%s", err, wfID, wfType, ns, attributes.TaskQueue)
	}

	// workflow execution timeout is left as is
	//  if workflow execution timeout == 0 -> infinity

	attributes.WorkflowRunTimeout = durationpb.New(common.OverrideWorkflowRunTimeout(attributes.GetWorkflowRunTimeout().AsDuration(), attributes.GetWorkflowExecutionTimeout().AsDuration()))

	attributes.WorkflowTaskTimeout = durationpb.New(common.OverrideWorkflowTaskTimeout(targetNamespace.String(), attributes.GetWorkflowTaskTimeout().AsDuration(), attributes.GetWorkflowRunTimeout().AsDuration(), defaultWorkflowTaskTimeoutFn))

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil
}

func (v *CommandAttrValidator) validateActivityRetryPolicy(
	namespaceID namespace.ID,
	retryPolicy *commonpb.RetryPolicy,
) error {
	if retryPolicy == nil {
		return nil
	}
	// TODO: this is a namespace setting, not a namespace id setting
	defaultActivityRetrySettings := v.getDefaultActivityRetrySettings(namespaceID.String())
	retrypolicy.EnsureDefaults(retryPolicy, defaultActivityRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func (v *CommandAttrValidator) validateWorkflowRetryPolicy(
	namespaceName namespace.Name,
	retryPolicy *commonpb.RetryPolicy,
) error {
	if retryPolicy == nil {
		// By default, if the user does not explicitly set a retry policy for a Child Workflow, do not perform any retries.
		return nil
	}

	// Otherwise, for any unset fields on the retry policy, set with defaults
	defaultWorkflowRetrySettings := v.getDefaultWorkflowRetrySettings(namespaceName.String())
	retrypolicy.EnsureDefaults(retryPolicy, defaultWorkflowRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func (v *CommandAttrValidator) validateCrossNamespaceCall(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
) error {

	// same name, no check needed
	if namespaceID == targetNamespaceID {
		return nil
	}

	if !v.enableCrossNamespaceCommands() {
		return serviceerror.NewInvalidArgument("cross namespace commands are not allowed")
	}

	namespaceEntry, err := v.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	targetNamespaceEntry, err := v.namespaceRegistry.GetNamespaceByID(targetNamespaceID)
	if err != nil {
		return err
	}

	// both local namespace
	if !namespaceEntry.IsGlobalNamespace() && !targetNamespaceEntry.IsGlobalNamespace() {
		return nil
	}

	namespaceClusters := namespaceEntry.ClusterNames()
	targetNamespaceClusters := targetNamespaceEntry.ClusterNames()

	// one is local namespace, another one is global namespace or both global namespace
	// treat global namespace with one replication cluster as local namespace
	if len(namespaceClusters) == 1 && len(targetNamespaceClusters) == 1 {
		if namespaceClusters[0] == targetNamespaceClusters[0] {
			return nil
		}
		return v.createCrossNamespaceCallError(namespaceEntry, targetNamespaceEntry)
	}
	return v.createCrossNamespaceCallError(namespaceEntry, targetNamespaceEntry)
}

func (v *CommandAttrValidator) createCrossNamespaceCallError(
	namespaceEntry *namespace.Namespace,
	targetNamespaceEntry *namespace.Namespace,
) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf("unable to process cross namespace command between %v and %v", namespaceEntry.Name(), targetNamespaceEntry.Name()))
}

func (v *CommandAttrValidator) ValidateCommandSequence(
	commands []*commandpb.Command,
) error {
	closeCommand := enumspb.COMMAND_TYPE_UNSPECIFIED

	for _, command := range commands {
		if closeCommand != enumspb.COMMAND_TYPE_UNSPECIFIED {
			return serviceerror.NewInvalidArgument(fmt.Sprintf(
				"invalid command sequence: [%v], command %s must be the last command.",
				strings.Join(v.commandTypes(commands), ", "), closeCommand.String(),
			))
		}

		// nolint:exhaustive
		switch command.GetCommandType() {
		case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
			enumspb.COMMAND_TYPE_START_TIMER,
			enumspb.COMMAND_TYPE_CANCEL_TIMER,
			enumspb.COMMAND_TYPE_RECORD_MARKER,
			enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES,
			enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION:
			// noop
		case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
			closeCommand = command.GetCommandType()
		default:
			// The default is to fail with invalid argument to force authors of new commands to consider whether it's a
			// close command however unlikely that may be.
			return serviceerror.NewInvalidArgument(fmt.Sprintf("unknown command type: %v", command.GetCommandType()))
		}
	}
	return nil
}

func (v *CommandAttrValidator) commandTypes(
	commands []*commandpb.Command,
) []string {
	result := make([]string, len(commands))
	for index, command := range commands {
		result[index] = command.GetCommandType().String()
	}
	return result
}
