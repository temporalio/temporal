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
	"fmt"
	"strings"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/elasticsearch/validator"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	decisionAttrValidator struct {
		domainCache               cache.DomainCache
		maxIDLengthLimit          int
		searchAttributesValidator *validator.SearchAttributesValidator
	}

	workflowSizeChecker struct {
		blobSizeLimitWarn  int
		blobSizeLimitError int

		historySizeLimitWarn  int
		historySizeLimitError int

		historyCountLimitWarn  int
		historyCountLimitError int

		completedID    int64
		mutableState   mutableState
		executionStats *persistence.ExecutionStats
		metricsClient  metrics.Client
		logger         log.Logger
	}
)

const (
	reservedTaskListPrefix = "/__temporal_sys/"
)

func newDecisionAttrValidator(
	domainCache cache.DomainCache,
	config *Config,
	logger log.Logger,
) *decisionAttrValidator {
	return &decisionAttrValidator{
		domainCache:      domainCache,
		maxIDLengthLimit: config.MaxIDLengthLimit(),
		searchAttributesValidator: validator.NewSearchAttributesValidator(
			logger,
			config.ValidSearchAttributes,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
	}
}

func newWorkflowSizeChecker(
	blobSizeLimitWarn int,
	blobSizeLimitError int,
	historySizeLimitWarn int,
	historySizeLimitError int,
	historyCountLimitWarn int,
	historyCountLimitError int,
	completedID int64,
	mutableState mutableState,
	executionStats *persistence.ExecutionStats,
	metricsClient metrics.Client,
	logger log.Logger,
) *workflowSizeChecker {
	return &workflowSizeChecker{
		blobSizeLimitWarn:      blobSizeLimitWarn,
		blobSizeLimitError:     blobSizeLimitError,
		historySizeLimitWarn:   historySizeLimitWarn,
		historySizeLimitError:  historySizeLimitError,
		historyCountLimitWarn:  historyCountLimitWarn,
		historyCountLimitError: historyCountLimitError,
		completedID:            completedID,
		mutableState:           mutableState,
		executionStats:         executionStats,
		metricsClient:          metricsClient,
		logger:                 logger,
	}
}

func (c *workflowSizeChecker) failWorkflowIfBlobSizeExceedsLimit(
	blob []byte,
	message string,
) (bool, error) {

	executionInfo := c.mutableState.GetExecutionInfo()
	err := common.CheckEventBlobSizeLimit(
		len(blob),
		c.blobSizeLimitWarn,
		c.blobSizeLimitError,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		c.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope),
		c.logger,
	)
	if err == nil {
		return false, nil
	}

	attributes := &commonproto.FailWorkflowExecutionDecisionAttributes{
		Reason:  common.FailureReasonDecisionBlobSizeExceedsLimit,
		Details: []byte(message),
	}

	if _, err := c.mutableState.AddFailWorkflowEvent(c.completedID, attributes); err != nil {
		return false, err
	}

	return true, nil
}

func (c *workflowSizeChecker) failWorkflowSizeExceedsLimit() (bool, error) {
	historyCount := int(c.mutableState.GetNextEventID()) - 1
	historySize := int(c.executionStats.HistorySize)

	if historySize > c.historySizeLimitError || historyCount > c.historyCountLimitError {
		executionInfo := c.mutableState.GetExecutionInfo()
		c.logger.Error("history size exceeds error limit.",
			tag.WorkflowDomainID(executionInfo.DomainID),
			tag.WorkflowID(executionInfo.WorkflowID),
			tag.WorkflowRunID(executionInfo.RunID),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))

		attributes := &commonproto.FailWorkflowExecutionDecisionAttributes{
			Reason:  common.FailureReasonSizeExceedsLimit,
			Details: []byte("Workflow history size / count exceeds limit."),
		}

		if _, err := c.mutableState.AddFailWorkflowEvent(c.completedID, attributes); err != nil {
			return false, err
		}
		return true, nil
	}

	if historySize > c.historySizeLimitWarn || historyCount > c.historyCountLimitWarn {
		executionInfo := c.mutableState.GetExecutionInfo()
		c.logger.Warn("history size exceeds warn limit.",
			tag.WorkflowDomainID(executionInfo.DomainID),
			tag.WorkflowID(executionInfo.WorkflowID),
			tag.WorkflowRunID(executionInfo.RunID),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))
		return false, nil
	}

	return false, nil
}

func (v *decisionAttrValidator) validateActivityScheduleAttributes(
	domainID string,
	targetDomainID string,
	attributes *commonproto.ScheduleActivityTaskDecisionAttributes,
	wfTimeout int32,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("ScheduleActivityTaskDecisionAttributes is not set on decision.")
	}

	defaultTaskListName := ""
	if _, err := v.validatedTaskList(attributes.TaskList, defaultTaskListName); err != nil {
		return err
	}

	if attributes.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("ActivityId is not set on decision.")
	}

	if attributes.ActivityType == nil || attributes.ActivityType.GetName() == "" {
		return serviceerror.NewInvalidArgument("ActivityType is not set on decision.")
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	if len(attributes.GetActivityId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("ActivityID exceeds length limit.")
	}

	if len(attributes.GetActivityType().GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("ActivityType exceeds length limit.")
	}

	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Domain exceeds length limit.")
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if attributes.GetScheduleToCloseTimeoutSeconds() < 0 || attributes.GetScheduleToStartTimeoutSeconds() < 0 ||
		attributes.GetStartToCloseTimeoutSeconds() < 0 || attributes.GetHeartbeatTimeoutSeconds() < 0 {
		return serviceerror.NewInvalidArgument("A valid timeout may not be negative.")
	}

	// ensure activity timeout never larger than workflow timeout
	if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToCloseTimeoutSeconds = wfTimeout
	}
	if attributes.GetScheduleToStartTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToStartTimeoutSeconds = wfTimeout
	}
	if attributes.GetStartToCloseTimeoutSeconds() > wfTimeout {
		attributes.StartToCloseTimeoutSeconds = wfTimeout
	}
	if attributes.GetHeartbeatTimeoutSeconds() > wfTimeout {
		attributes.HeartbeatTimeoutSeconds = wfTimeout
	}

	validScheduleToClose := attributes.GetScheduleToCloseTimeoutSeconds() > 0
	validScheduleToStart := attributes.GetScheduleToStartTimeoutSeconds() > 0
	validStartToClose := attributes.GetStartToCloseTimeoutSeconds() > 0

	if validScheduleToClose {
		if !validScheduleToStart {
			attributes.ScheduleToStartTimeoutSeconds = attributes.GetScheduleToCloseTimeoutSeconds()
		}
		if !validStartToClose {
			attributes.StartToCloseTimeoutSeconds = attributes.GetScheduleToCloseTimeoutSeconds()
		}
	} else if validScheduleToStart && validStartToClose {
		attributes.ScheduleToCloseTimeoutSeconds = attributes.GetScheduleToStartTimeoutSeconds() + attributes.GetStartToCloseTimeoutSeconds()
		if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
			attributes.ScheduleToCloseTimeoutSeconds = wfTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgument("A valid ScheduleToCloseTimeout is not set on decision.")
	}
	// ensure activity's SCHEDULE_TO_START and SCHEDULE_TO_CLOSE is as long as expiration on retry policy
	p := attributes.RetryPolicy
	if p != nil {
		expiration := p.GetExpirationIntervalInSeconds()
		if expiration == 0 {
			expiration = wfTimeout
		}
		if attributes.GetScheduleToStartTimeoutSeconds() < expiration {
			attributes.ScheduleToStartTimeoutSeconds = expiration
		}
		if attributes.GetScheduleToCloseTimeoutSeconds() < expiration {
			attributes.ScheduleToCloseTimeoutSeconds = expiration
		}
	}
	return nil
}

func (v *decisionAttrValidator) validateTimerScheduleAttributes(
	attributes *commonproto.StartTimerDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("StartTimerDecisionAttributes is not set on decision.")
	}
	if attributes.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("TimerId is not set on decision.")
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TimerId exceeds length limit.")
	}
	if attributes.GetStartToFireTimeoutSeconds() <= 0 {
		return serviceerror.NewInvalidArgument("A valid StartToFireTimeoutSeconds is not set on decision.")
	}
	return nil
}

func (v *decisionAttrValidator) validateActivityCancelAttributes(
	attributes *commonproto.RequestCancelActivityTaskDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RequestCancelActivityTaskDecisionAttributes is not set on decision.")
	}
	if attributes.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("ActivityId is not set on decision.")
	}
	if len(attributes.GetActivityId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("ActivityId exceeds length limit.")
	}
	return nil
}

func (v *decisionAttrValidator) validateTimerCancelAttributes(
	attributes *commonproto.CancelTimerDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CancelTimerDecisionAttributes is not set on decision.")
	}
	if attributes.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("TimerId is not set on decision.")
	}
	if len(attributes.GetTimerId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TimerId exceeds length limit.")
	}
	return nil
}

func (v *decisionAttrValidator) validateRecordMarkerAttributes(
	attributes *commonproto.RecordMarkerDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RecordMarkerDecisionAttributes is not set on decision.")
	}
	if attributes.GetMarkerName() == "" {
		return serviceerror.NewInvalidArgument("MarkerName is not set on decision.")
	}
	if len(attributes.GetMarkerName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("MarkerName exceeds length limit.")
	}

	return nil
}

func (v *decisionAttrValidator) validateCompleteWorkflowExecutionAttributes(
	attributes *commonproto.CompleteWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CompleteWorkflowExecutionDecisionAttributes is not set on decision.")
	}
	return nil
}

func (v *decisionAttrValidator) validateFailWorkflowExecutionAttributes(
	attributes *commonproto.FailWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("FailWorkflowExecutionDecisionAttributes is not set on decision.")
	}
	if attributes.GetReason() == "" {
		return serviceerror.NewInvalidArgument("Reason is not set on decision.")
	}
	return nil
}

func (v *decisionAttrValidator) validateCancelWorkflowExecutionAttributes(
	attributes *commonproto.CancelWorkflowExecutionDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("CancelWorkflowExecutionDecisionAttributes is not set on decision.")
	}
	return nil
}

func (v *decisionAttrValidator) validateCancelExternalWorkflowExecutionAttributes(
	domainID string,
	targetDomainID string,
	attributes *commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision.")
	}
	if attributes.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("WorkflowId is not set on decision.")
	}
	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Domain exceeds length limit.")
	}
	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}
	runID := attributes.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return serviceerror.NewInvalidArgument("Invalid RunId set on decision.")
	}

	return nil
}

func (v *decisionAttrValidator) validateSignalExternalWorkflowExecutionAttributes(
	domainID string,
	targetDomainID string,
	attributes *commonproto.SignalExternalWorkflowExecutionDecisionAttributes,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.")
	}
	if attributes.Execution == nil {
		return serviceerror.NewInvalidArgument("Execution is nil on decision.")
	}
	if attributes.Execution.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("WorkflowId is not set on decision.")
	}
	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Domain exceeds length limit.")
	}
	if len(attributes.Execution.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}

	targetRunID := attributes.Execution.GetRunId()
	if targetRunID != "" && uuid.Parse(targetRunID) == nil {
		return serviceerror.NewInvalidArgument("Invalid RunId set on decision.")
	}
	if attributes.GetSignalName() == "" {
		return serviceerror.NewInvalidArgument("SignalName is not set on decision.")
	}

	return nil
}

func (v *decisionAttrValidator) validateUpsertWorkflowSearchAttributes(
	domainName string,
	attributes *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision.")
	}

	if attributes.SearchAttributes == nil {
		return serviceerror.NewInvalidArgument("SearchAttributes is not set on decision.")
	}

	if len(attributes.GetSearchAttributes().GetIndexedFields()) == 0 {
		return serviceerror.NewInvalidArgument("IndexedFields is empty on decision.")
	}

	return v.searchAttributesValidator.ValidateSearchAttributes(attributes.GetSearchAttributes(), domainName)
}

func (v *decisionAttrValidator) validateContinueAsNewWorkflowExecutionAttributes(
	attributes *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes,
	executionInfo *persistence.WorkflowExecutionInfo,
) error {

	if attributes == nil {
		return serviceerror.NewInvalidArgument("ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision.")
	}

	// Inherit workflow type from previous execution if not provided on decision
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &commonproto.WorkflowType{Name: executionInfo.WorkflowTypeName}
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	// Inherit Tasklist from previous execution if not provided on decision
	taskList, err := v.validatedTaskList(attributes.TaskList, executionInfo.TaskList)
	if err != nil {
		return err
	}
	attributes.TaskList = taskList

	// Inherit workflow timeout from previous execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = executionInfo.WorkflowTimeout
	}

	// Inherit decision task timeout from previous execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = executionInfo.DecisionStartToCloseTimeout
	}

	// Check next run decision task delay
	if attributes.GetBackoffStartIntervalInSeconds() < 0 {
		return serviceerror.NewInvalidArgument("BackoffStartInterval is less than 0.")
	}

	domainEntry, err := v.domainCache.GetDomainByID(executionInfo.DomainID)
	if err != nil {
		return err
	}
	return v.searchAttributesValidator.ValidateSearchAttributes(attributes.GetSearchAttributes(), domainEntry.GetInfo().Name)
}

func (v *decisionAttrValidator) validateStartChildExecutionAttributes(
	domainID string,
	targetDomainID string,
	attributes *commonproto.StartChildWorkflowExecutionDecisionAttributes,
	parentInfo *persistence.WorkflowExecutionInfo,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return serviceerror.NewInvalidArgument("StartChildWorkflowExecutionDecisionAttributes is not set on decision.")
	}

	if attributes.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("Required field WorkflowID is not set on decision.")
	}

	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		return serviceerror.NewInvalidArgument("Required field WorkflowType is not set on decision.")
	}

	if len(attributes.GetDomain()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Domain exceeds length limit.")
	}

	if len(attributes.GetWorkflowId()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	if err := backoff.ValidateSchedule(attributes.GetCronSchedule()); err != nil {
		return err
	}

	// Inherit tasklist from parent workflow execution if not provided on decision
	taskList, err := v.validatedTaskList(attributes.TaskList, parentInfo.TaskList)
	if err != nil {
		return err
	}
	attributes.TaskList = taskList

	// Inherit workflow timeout from parent workflow execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = parentInfo.WorkflowTimeout
	}

	// Inherit decision task timeout from parent workflow execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = parentInfo.DecisionStartToCloseTimeout
	}

	return nil
}

func (v *decisionAttrValidator) validatedTaskList(
	taskList *commonproto.TaskList,
	defaultVal string,
) (*commonproto.TaskList, error) {

	if taskList == nil {
		taskList = &commonproto.TaskList{}
	}

	if taskList.GetName() == "" {
		if defaultVal == "" {
			return taskList, serviceerror.NewInvalidArgument("missing task list name")
		}
		taskList.Name = defaultVal
		return taskList, nil
	}

	name := taskList.GetName()
	if len(name) > v.maxIDLengthLimit {
		return taskList, serviceerror.NewInvalidArgument(fmt.Sprintf("task list name exceeds length limit of %v", v.maxIDLengthLimit))
	}

	if strings.HasPrefix(name, reservedTaskListPrefix) {
		return taskList, serviceerror.NewInvalidArgument(fmt.Sprintf("task list name cannot start with reserved prefix %v", reservedTaskListPrefix))
	}

	return taskList, nil
}

func (v *decisionAttrValidator) validateCrossDomainCall(
	domainID string,
	targetDomainID string,
) error {

	// same name, no check needed
	if domainID == targetDomainID {
		return nil
	}

	domainEntry, err := v.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}

	targetDomainEntry, err := v.domainCache.GetDomainByID(targetDomainID)
	if err != nil {
		return err
	}

	// both local domain
	if !domainEntry.IsGlobalDomain() && !targetDomainEntry.IsGlobalDomain() {
		return nil
	}

	domainClusters := domainEntry.GetReplicationConfig().Clusters
	targetDomainClusters := targetDomainEntry.GetReplicationConfig().Clusters

	// one is local domain, another one is global domain or both global domain
	// treat global domain with one replication cluster as local domain
	if len(domainClusters) == 1 && len(targetDomainClusters) == 1 {
		if *domainClusters[0] == *targetDomainClusters[0] {
			return nil
		}
		return v.createCrossDomainCallError(domainEntry, targetDomainEntry)
	}
	return v.createCrossDomainCallError(domainEntry, targetDomainEntry)
}

func (v *decisionAttrValidator) createCrossDomainCallError(
	domainEntry *cache.DomainCacheEntry,
	targetDomainEntry *cache.DomainCacheEntry,
) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf("cannot make cross domain call between %v and %v", domainEntry.GetInfo().Name, targetDomainEntry.GetInfo().Name))
}
