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

package history

import (
	"fmt"
	"strings"
	"time"

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
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/workflow"
)

type (
	commandAttrValidator struct {
		namespaceRegistry               namespace.Registry
		config                          *configs.Config
		maxIDLengthLimit                int
		searchAttributesValidator       *searchattribute.Validator
		getDefaultActivityRetrySettings dynamicconfig.MapPropertyFnWithNamespaceFilter
		getDefaultWorkflowRetrySettings dynamicconfig.MapPropertyFnWithNamespaceFilter
		enableCrossNamespaceCommands    dynamicconfig.BoolPropertyFn
	}

	workflowSizeChecker struct {
		blobSizeLimitWarn  int
		blobSizeLimitError int

		memoSizeLimitWarn  int
		memoSizeLimitError int

		historySizeLimitWarn  int
		historySizeLimitError int

		historyCountLimitWarn  int
		historyCountLimitError int

		completedID               int64
		mutableState              workflow.MutableState
		searchAttributesValidator *searchattribute.Validator
		executionStats            *persistencespb.ExecutionStats
		metricsScope              metrics.Scope
		logger                    log.Logger
	}
)

const (
	reservedTaskQueuePrefix = "/_sys/"
)

func newCommandAttrValidator(
	namespaceRegistry namespace.Registry,
	config *configs.Config,
	searchAttributesValidator *searchattribute.Validator,
) *commandAttrValidator {
	return &commandAttrValidator{
		namespaceRegistry:               namespaceRegistry,
		config:                          config,
		maxIDLengthLimit:                config.MaxIDLengthLimit(),
		searchAttributesValidator:       searchAttributesValidator,
		getDefaultActivityRetrySettings: config.DefaultActivityRetryPolicy,
		getDefaultWorkflowRetrySettings: config.DefaultWorkflowRetryPolicy,
		enableCrossNamespaceCommands:    config.EnableCrossNamespaceCommands,
	}
}

func newWorkflowSizeChecker(
	blobSizeLimitWarn int,
	blobSizeLimitError int,
	memoSizeLimitWarn int,
	memoSizeLimitError int,
	historySizeLimitWarn int,
	historySizeLimitError int,
	historyCountLimitWarn int,
	historyCountLimitError int,
	completedID int64,
	mutableState workflow.MutableState,
	searchAttributesValidator *searchattribute.Validator,
	executionStats *persistencespb.ExecutionStats,
	metricsScope metrics.Scope,
	logger log.Logger,
) *workflowSizeChecker {
	return &workflowSizeChecker{
		blobSizeLimitWarn:         blobSizeLimitWarn,
		blobSizeLimitError:        blobSizeLimitError,
		memoSizeLimitWarn:         memoSizeLimitWarn,
		memoSizeLimitError:        memoSizeLimitError,
		historySizeLimitWarn:      historySizeLimitWarn,
		historySizeLimitError:     historySizeLimitError,
		historyCountLimitWarn:     historyCountLimitWarn,
		historyCountLimitError:    historyCountLimitError,
		completedID:               completedID,
		mutableState:              mutableState,
		searchAttributesValidator: searchAttributesValidator,
		executionStats:            executionStats,
		metricsScope:              metricsScope,
		logger:                    logger,
	}
}

func (c *workflowSizeChecker) failWorkflowIfPayloadSizeExceedsLimit(
	commandTypeTag metrics.Tag,
	payloadSize int,
	message string,
) (bool, error) {

	executionInfo := c.mutableState.GetExecutionInfo()
	executionState := c.mutableState.GetExecutionState()
	err := common.CheckEventBlobSizeLimit(
		payloadSize,
		c.blobSizeLimitWarn,
		c.blobSizeLimitError,
		executionInfo.NamespaceId,
		executionInfo.WorkflowId,
		executionState.RunId,
		c.metricsScope.Tagged(commandTypeTag),
		c.logger,
		tag.BlobSizeViolationOperation(commandTypeTag.Value()),
	)
	if err == nil {
		return false, nil
	}

	attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
		Failure: failure.NewServerFailure(message, true),
	}

	if _, err := c.mutableState.AddFailWorkflowEvent(c.completedID, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, attributes, ""); err != nil {
		return false, err
	}

	return true, nil
}

func (c *workflowSizeChecker) failWorkflowIfMemoSizeExceedsLimit(
	commandTypeTag metrics.Tag,
	memoSize int,
	message string,
) (bool, error) {

	executionInfo := c.mutableState.GetExecutionInfo()
	executionState := c.mutableState.GetExecutionState()
	err := common.CheckEventBlobSizeLimit(
		memoSize,
		c.memoSizeLimitWarn,
		c.memoSizeLimitError,
		executionInfo.NamespaceId,
		executionInfo.WorkflowId,
		executionState.RunId,
		c.metricsScope.Tagged(commandTypeTag),
		c.logger,
		tag.BlobSizeViolationOperation(commandTypeTag.Value()),
	)
	if err == nil {
		return false, nil
	}

	attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
		Failure: failure.NewServerFailure(message, true),
	}

	if _, err := c.mutableState.AddFailWorkflowEvent(c.completedID, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, attributes, ""); err != nil {
		return false, err
	}

	return true, nil
}

func (c *workflowSizeChecker) failWorkflowIfSearchAttributesSizeExceedsLimit(
	searchAttributes *commonpb.SearchAttributes,
	namespace namespace.Name,
	commandTypeTag metrics.Tag,
) (bool, error) {
	c.metricsScope.Tagged(commandTypeTag).RecordDistribution(metrics.SearchAttributesSize, searchAttributes.Size())

	err := c.searchAttributesValidator.ValidateSize(searchAttributes, namespace.String())
	if err == nil {
		return false, nil
	}

	c.logger.Warn("Search attributes size exceeds limits. Fail workflow.", tag.Error(err), tag.WorkflowNamespace(namespace.String()))

	attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
		Failure: failure.NewServerFailure(err.Error(), true),
	}

	if _, err := c.mutableState.AddFailWorkflowEvent(c.completedID, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, attributes, ""); err != nil {
		return false, err
	}

	return true, nil
}

func (v *commandAttrValidator) validateActivityScheduleAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
	runTimeout time.Duration,
) error {

	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("ScheduleActivityTaskCommandAttributes is not set on command.")
	}

	defaultTaskQueueName := ""
	if _, err := v.validateTaskQueue(attributes.TaskQueue, defaultTaskQueueName); err != nil {
		return err
	}

	if attributes.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("ActivityId is not set on command.")
	}

	if attributes.ActivityType == nil || attributes.ActivityType.GetName() == "" {
		return serviceerror.NewInvalidArgument("ActivityType is not set on command.")
	}

	if err := v.validateActivityRetryPolicy(attributes); err != nil {
		return err
	}

	if len(attributes.GetActivityId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("ActivityID exceeds length limit.")
	}

	if len(attributes.GetActivityType().GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("ActivityType exceeds length limit.")
	}

	if len(attributes.GetNamespace()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if timestamp.DurationValue(attributes.GetScheduleToCloseTimeout()) < 0 || timestamp.DurationValue(attributes.GetScheduleToStartTimeout()) < 0 ||
		timestamp.DurationValue(attributes.GetStartToCloseTimeout()) < 0 || timestamp.DurationValue(attributes.GetHeartbeatTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("A valid timeout may not be negative.")
	}

	validScheduleToClose := timestamp.DurationValue(attributes.GetScheduleToCloseTimeout()) > 0
	validScheduleToStart := timestamp.DurationValue(attributes.GetScheduleToStartTimeout()) > 0
	validStartToClose := timestamp.DurationValue(attributes.GetStartToCloseTimeout()) > 0

	if validScheduleToClose {
		if validScheduleToStart {
			attributes.ScheduleToStartTimeout = timestamp.MinDurationPtr(attributes.GetScheduleToStartTimeout(),
				attributes.GetScheduleToCloseTimeout())
		} else {
			attributes.ScheduleToStartTimeout = attributes.GetScheduleToCloseTimeout()
		}
		if validStartToClose {
			attributes.StartToCloseTimeout = timestamp.MinDurationPtr(attributes.GetStartToCloseTimeout(),
				attributes.GetScheduleToCloseTimeout())
		} else {
			attributes.StartToCloseTimeout = attributes.GetScheduleToCloseTimeout()
		}
	} else if validStartToClose {
		// We are in !validScheduleToClose due to the first if above
		attributes.ScheduleToCloseTimeout = &runTimeout
		if !validScheduleToStart {
			attributes.ScheduleToStartTimeout = &runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgument("A valid StartToClose or ScheduleToCloseTimeout is not set on command.")
	}
	// ensure activity timeout never larger than workflow timeout
	if runTimeout > 0 {
		if timestamp.DurationValue(attributes.GetScheduleToCloseTimeout()) > runTimeout {
			attributes.ScheduleToCloseTimeout = &runTimeout
		}
		if timestamp.DurationValue(attributes.GetScheduleToStartTimeout()) > runTimeout {
			attributes.ScheduleToStartTimeout = &runTimeout
		}
		if timestamp.DurationValue(attributes.GetStartToCloseTimeout()) > runTimeout {
			attributes.StartToCloseTimeout = &runTimeout
		}
		if timestamp.DurationValue(attributes.GetHeartbeatTimeout()) > runTimeout {
			attributes.HeartbeatTimeout = &runTimeout
		}
	}
	attributes.HeartbeatTimeout = timestamp.MinDurationPtr(attributes.GetHeartbeatTimeout(), attributes.GetStartToCloseTimeout())

	return nil
}

func (v *commandAttrValidator) validateTimerScheduleAttributes(
	attributes *commandpb.StartTimerCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("StartTimerCommandAttributes is not set on command.")
	}
	if attributes.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("TimerId is not set on command.")
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TimerId exceeds length limit.")
	}
	if timestamp.DurationValue(attributes.GetStartToFireTimeout()) <= 0 {
		return serviceerror.NewInvalidArgument("A valid StartToFireTimeout is not set on command.")
	}
	return nil
}

func (v *commandAttrValidator) validateActivityCancelAttributes(
	attributes *commandpb.RequestCancelActivityTaskCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RequestCancelActivityTaskCommandAttributes is not set on command.")
	}
	if attributes.GetScheduledEventId() <= 0 {
		return serviceerror.NewInvalidArgument("ScheduledEventId is not set on command.")
	}
	return nil
}

func (v *commandAttrValidator) validateTimerCancelAttributes(
	attributes *commandpb.CancelTimerCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CancelTimerCommandAttributes is not set on command.")
	}
	if attributes.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("TimerId is not set on command.")
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TimerId exceeds length limit.")
	}
	return nil
}

func (v *commandAttrValidator) validateRecordMarkerAttributes(
	attributes *commandpb.RecordMarkerCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RecordMarkerCommandAttributes is not set on command.")
	}
	if attributes.GetMarkerName() == "" {
		return serviceerror.NewInvalidArgument("MarkerName is not set on command.")
	}
	if len(attributes.GetMarkerName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("MarkerName exceeds length limit.")
	}

	return nil
}

func (v *commandAttrValidator) validateCompleteWorkflowExecutionAttributes(
	attributes *commandpb.CompleteWorkflowExecutionCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CompleteWorkflowExecutionCommandAttributes is not set on command.")
	}
	return nil
}

func (v *commandAttrValidator) validateFailWorkflowExecutionAttributes(
	attributes *commandpb.FailWorkflowExecutionCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("FailWorkflowExecutionCommandAttributes is not set on command.")
	}
	if attributes.GetFailure() == nil {
		return serviceerror.NewInvalidArgument("Failure is not set on command.")
	}
	return nil
}

func (v *commandAttrValidator) validateCancelWorkflowExecutionAttributes(
	attributes *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CancelWorkflowExecutionCommandAttributes is not set on command.")
	}
	return nil
}

func (v *commandAttrValidator) validateCancelExternalWorkflowExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	initiatedChildExecutionsInSession map[string]struct{},
	attributes *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) error {

	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RequestCancelExternalWorkflowExecutionCommandAttributes is not set on command.")
	}
	if attributes.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("WorkflowId is not set on command.")
	}
	if len(attributes.GetNamespace()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}
	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}
	runID := attributes.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return serviceerror.NewInvalidArgument("Invalid RunId set on command.")
	}
	if _, ok := initiatedChildExecutionsInSession[attributes.GetWorkflowId()]; ok {
		return serviceerror.NewInvalidArgument("Start and RequestCancel for child workflow is not allowed in same workflow task.")
	}

	return nil
}

func (v *commandAttrValidator) validateSignalExternalWorkflowExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) error {

	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("SignalExternalWorkflowExecutionCommandAttributes is not set on command.")
	}
	if attributes.Execution == nil {
		return serviceerror.NewInvalidArgument("Execution is nil on command.")
	}
	if attributes.Execution.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("WorkflowId is not set on command.")
	}
	if len(attributes.GetNamespace()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}
	if len(attributes.Execution.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}

	targetRunID := attributes.Execution.GetRunId()
	if targetRunID != "" && uuid.Parse(targetRunID) == nil {
		return serviceerror.NewInvalidArgument("Invalid RunId set on command.")
	}
	if attributes.GetSignalName() == "" {
		return serviceerror.NewInvalidArgument("SignalName is not set on command.")
	}

	return nil
}

func (v *commandAttrValidator) validateUpsertWorkflowSearchAttributes(
	namespace namespace.Name,
	attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
	visibilityIndexName string,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("UpsertWorkflowSearchAttributesCommandAttributes is not set on command.")
	}

	if attributes.SearchAttributes == nil {
		return serviceerror.NewInvalidArgument("SearchAttributes is not set on command.")
	}

	if len(attributes.GetSearchAttributes().GetIndexedFields()) == 0 {
		return serviceerror.NewInvalidArgument("IndexedFields is empty on command.")
	}

	return v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), namespace.String(), visibilityIndexName)
}

func (v *commandAttrValidator) validateContinueAsNewWorkflowExecutionAttributes(
	namespace namespace.Name,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	visibilityIndexName string,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("ContinueAsNewWorkflowExecutionCommandAttributes is not set on command.")
	}

	// Inherit workflow type from previous execution if not provided on command
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName}
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	// Inherit Taskqueue from previous execution if not provided on command
	taskQueue, err := v.validateTaskQueue(attributes.TaskQueue, executionInfo.TaskQueue)
	if err != nil {
		return err
	}
	attributes.TaskQueue = taskQueue

	if timestamp.DurationValue(attributes.GetWorkflowRunTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowRunTimeout.")
	}

	if timestamp.DurationValue(attributes.GetWorkflowTaskTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowTaskTimeout.")
	}

	if timestamp.DurationValue(attributes.GetBackoffStartInterval()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid BackoffStartInterval.")
	}

	if timestamp.DurationValue(attributes.GetWorkflowRunTimeout()) == 0 {
		attributes.WorkflowRunTimeout = timestamp.DurationPtr(timestamp.DurationValue(executionInfo.WorkflowRunTimeout))
	}

	if timestamp.DurationValue(attributes.GetWorkflowTaskTimeout()) == 0 {
		attributes.WorkflowTaskTimeout = timestamp.DurationPtr(timestamp.DurationValue(executionInfo.DefaultWorkflowTaskTimeout))
	}

	return v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), namespace.String(), visibilityIndexName)
}

func (v *commandAttrValidator) validateStartChildExecutionAttributes(
	namespaceID namespace.ID,
	targetNamespaceID namespace.ID,
	targetNamespace namespace.Name,
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
	parentInfo *persistencespb.WorkflowExecutionInfo,
	defaultWorkflowTaskTimeoutFn dynamicconfig.DurationPropertyFnWithNamespaceFilter,
	visibilityIndexName string,
) error {

	if err := v.validateCrossNamespaceCall(
		namespaceID,
		targetNamespaceID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("StartChildWorkflowExecutionCommandAttributes is not set on command.")
	}

	if attributes.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("Required field WorkflowId is not set on command.")
	}

	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		return serviceerror.NewInvalidArgument("Required field WorkflowType is not set on command.")
	}

	if len(attributes.GetNamespace()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}

	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	if timestamp.DurationValue(attributes.GetWorkflowExecutionTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowExecutionTimeout.")
	}

	if timestamp.DurationValue(attributes.GetWorkflowRunTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowRunTimeout.")
	}

	if timestamp.DurationValue(attributes.GetWorkflowTaskTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowTaskTimeout.")
	}

	if err := v.validateWorkflowRetryPolicy(attributes); err != nil {
		return err
	}

	if err := backoff.ValidateSchedule(attributes.GetCronSchedule()); err != nil {
		return err
	}

	if err := v.searchAttributesValidator.Validate(attributes.GetSearchAttributes(), targetNamespace.String(), visibilityIndexName); err != nil {
		return err
	}

	// Inherit taskqueue from parent workflow execution if not provided on command
	taskQueue, err := v.validateTaskQueue(attributes.TaskQueue, parentInfo.TaskQueue)
	if err != nil {
		return err
	}
	attributes.TaskQueue = taskQueue

	// workflow execution timeout is left as is
	//  if workflow execution timeout == 0 -> infinity

	attributes.WorkflowRunTimeout = timestamp.DurationPtr(
		common.OverrideWorkflowRunTimeout(
			timestamp.DurationValue(attributes.GetWorkflowRunTimeout()),
			timestamp.DurationValue(attributes.GetWorkflowExecutionTimeout()),
		),
	)

	attributes.WorkflowTaskTimeout = timestamp.DurationPtr(
		common.OverrideWorkflowTaskTimeout(
			targetNamespace.String(),
			timestamp.DurationValue(attributes.GetWorkflowTaskTimeout()),
			timestamp.DurationValue(attributes.GetWorkflowRunTimeout()),
			defaultWorkflowTaskTimeoutFn,
		),
	)

	return nil
}

func (v *commandAttrValidator) validateTaskQueue(
	taskQueue *taskqueuepb.TaskQueue,
	defaultVal string,
) (*taskqueuepb.TaskQueue, error) {

	if taskQueue == nil {
		taskQueue = &taskqueuepb.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
	}

	if taskQueue.GetName() == "" {
		if defaultVal == "" {
			return taskQueue, serviceerror.NewInvalidArgument("missing task queue name")
		}
		taskQueue.Name = defaultVal
		return taskQueue, nil
	}

	name := taskQueue.GetName()
	if len(name) > v.maxIDLengthLimit {
		return taskQueue, serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name exceeds length limit of %v", v.maxIDLengthLimit))
	}

	if strings.HasPrefix(name, reservedTaskQueuePrefix) {
		return taskQueue, serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name cannot start with reserved prefix %v", reservedTaskQueuePrefix))
	}

	return taskQueue, nil
}

func (v *commandAttrValidator) validateActivityRetryPolicy(
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
) error {
	if attributes.RetryPolicy == nil {
		attributes.RetryPolicy = &commonpb.RetryPolicy{}
	}

	defaultActivityRetrySettings := common.FromConfigToDefaultRetrySettings(v.getDefaultActivityRetrySettings(attributes.GetNamespace()))
	common.EnsureRetryPolicyDefaults(attributes.RetryPolicy, defaultActivityRetrySettings)
	return common.ValidateRetryPolicy(attributes.RetryPolicy)
}

func (v *commandAttrValidator) validateWorkflowRetryPolicy(
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {
	if attributes.RetryPolicy == nil {
		// By default, if the user does not explicitly set a retry policy for a Child Workflow, do not perform any retries.
		return nil
	}

	// Otherwise, for any unset fields on the retry policy, set with defaults
	defaultWorkflowRetrySettings := common.FromConfigToDefaultRetrySettings(v.getDefaultWorkflowRetrySettings(attributes.GetNamespace()))
	common.EnsureRetryPolicyDefaults(attributes.RetryPolicy, defaultWorkflowRetrySettings)
	return common.ValidateRetryPolicy(attributes.RetryPolicy)
}

func (v *commandAttrValidator) validateCrossNamespaceCall(
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

func (v *commandAttrValidator) createCrossNamespaceCallError(
	namespaceEntry *namespace.Namespace,
	targetNamespaceEntry *namespace.Namespace,
) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf("unable to process cross namespace command between %v and %v", namespaceEntry.Name(), targetNamespaceEntry.Name()))
}

func (v *commandAttrValidator) validateCommandSequence(
	commands []*commandpb.Command,
) error {
	encounterTerminalCommand := false

	for _, command := range commands {
		if encounterTerminalCommand {
			return serviceerror.NewInvalidArgument(fmt.Sprintf(
				"encouter invalid commands sequence: %v",
				strings.Join(v.commandTypes(commands), ", "),
			))
		}

		switch command.GetCommandType() {
		case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
			// noop
		case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
			// noop
		case enumspb.COMMAND_TYPE_START_TIMER:
			// noop
		case enumspb.COMMAND_TYPE_CANCEL_TIMER:
			// noop
		case enumspb.COMMAND_TYPE_RECORD_MARKER:
			// noop
		case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
			// noop
		case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
			// noop
		case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
			// noop
		case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			// noop
		case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
			encounterTerminalCommand = true
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			encounterTerminalCommand = true
		case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
			encounterTerminalCommand = true
		case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
			encounterTerminalCommand = true
		default:
			return serviceerror.NewInvalidArgument(fmt.Sprintf("unknown command type: %v", command.GetCommandType()))
		}
	}
	return nil
}

func (v *commandAttrValidator) commandTypes(
	commands []*commandpb.Command,
) []string {
	result := make([]string, len(commands))
	for index, command := range commands {
		result[index] = command.GetCommandType().String()
	}
	return result
}
