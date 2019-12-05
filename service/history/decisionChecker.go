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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/elasticsearch/validator"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
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
	reservedTaskListPrefix = "/__cadence_sys/"
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

	attributes := &workflow.FailWorkflowExecutionDecisionAttributes{
		Reason:  common.StringPtr(common.FailureReasonDecisionBlobSizeExceedsLimit),
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

		attributes := &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  common.StringPtr(common.FailureReasonSizeExceedsLimit),
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
	attributes *workflow.ScheduleActivityTaskDecisionAttributes,
	wfTimeout int32,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return &workflow.BadRequestError{Message: "ScheduleActivityTaskDecisionAttributes is not set on decision."}
	}

	defaultTaskListName := ""
	if _, err := v.validatedTaskList(attributes.TaskList, defaultTaskListName); err != nil {
		return err
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
	domainID string,
	targetDomainID string,
	attributes *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

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
	domainID string,
	targetDomainID string,
	attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

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

func (v *decisionAttrValidator) validateUpsertWorkflowSearchAttributes(
	domainName string,
	attributes *workflow.UpsertWorkflowSearchAttributesDecisionAttributes,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision."}
	}

	if !attributes.IsSetSearchAttributes() {
		return &workflow.BadRequestError{Message: "SearchAttributes is not set on decision."}
	}

	if len(attributes.GetSearchAttributes().GetIndexedFields()) == 0 {
		return &workflow.BadRequestError{Message: "IndexedFields is empty on decision."}
	}

	return v.searchAttributesValidator.ValidateSearchAttributes(attributes.GetSearchAttributes(), domainName)
}

func (v *decisionAttrValidator) validateContinueAsNewWorkflowExecutionAttributes(
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes,
	executionInfo *persistence.WorkflowExecutionInfo,
) error {

	if attributes == nil {
		return &workflow.BadRequestError{Message: "ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	// Inherit workflow type from previous execution if not provided on decision
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)}
	}

	if len(attributes.WorkflowType.GetName()) > v.maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
	}

	// Inherit Tasklist from previous execution if not provided on decision
	taskList, err := v.validatedTaskList(attributes.TaskList, executionInfo.TaskList)
	if err != nil {
		return err
	}
	attributes.TaskList = taskList

	// Inherit workflow timeout from previous execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from previous execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.DecisionStartToCloseTimeout)
	}

	// Check next run decision task delay
	if attributes.GetBackoffStartIntervalInSeconds() < 0 {
		return &workflow.BadRequestError{Message: "BackoffStartInterval is less than 0."}
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
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes,
	parentInfo *persistence.WorkflowExecutionInfo,
) error {

	if err := v.validateCrossDomainCall(
		domainID,
		targetDomainID,
	); err != nil {
		return err
	}

	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartChildWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	if attributes.GetWorkflowId() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowID is not set on decision."}
	}

	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowType is not set on decision."}
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
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from parent workflow execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.DecisionStartToCloseTimeout)
	}

	return nil
}

func (v *decisionAttrValidator) validatedTaskList(
	taskList *workflow.TaskList,
	defaultVal string,
) (*workflow.TaskList, error) {

	if taskList == nil {
		taskList = &workflow.TaskList{}
	}

	if taskList.GetName() == "" {
		if defaultVal == "" {
			return taskList, &workflow.BadRequestError{Message: "missing task list name"}
		}
		taskList.Name = &defaultVal
		return taskList, nil
	}

	name := taskList.GetName()
	if len(name) > v.maxIDLengthLimit {
		return taskList, &workflow.BadRequestError{
			Message: fmt.Sprintf("task list name exceeds length limit of %v", v.maxIDLengthLimit),
		}
	}

	if strings.HasPrefix(name, reservedTaskListPrefix) {
		return taskList, &workflow.BadRequestError{
			Message: fmt.Sprintf("task list name cannot start with reserved prefix %v", reservedTaskListPrefix),
		}
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
	return &workflow.BadRequestError{Message: fmt.Sprintf(
		"cannot make cross domain call between %v and %v",
		domainEntry.GetInfo().Name,
		targetDomainEntry.GetInfo().Name,
	)}
}
