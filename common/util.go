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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"go.uber.org/yarpc/yarpcerrors"

	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

const (
	golandMapReserverNumberOfBytes = 48

	retryPersistenceOperationInitialInterval    = 50 * time.Millisecond
	retryPersistenceOperationMaxInterval        = 10 * time.Second
	retryPersistenceOperationExpirationInterval = 30 * time.Second

	historyServiceOperationInitialInterval    = 50 * time.Millisecond
	historyServiceOperationMaxInterval        = 10 * time.Second
	historyServiceOperationExpirationInterval = 30 * time.Second

	matchingServiceOperationInitialInterval    = 1000 * time.Millisecond
	matchingServiceOperationMaxInterval        = 10 * time.Second
	matchingServiceOperationExpirationInterval = 30 * time.Second

	frontendServiceOperationInitialInterval    = 200 * time.Millisecond
	frontendServiceOperationMaxInterval        = 5 * time.Second
	frontendServiceOperationExpirationInterval = 15 * time.Second

	adminServiceOperationInitialInterval    = 200 * time.Millisecond
	adminServiceOperationMaxInterval        = 5 * time.Second
	adminServiceOperationExpirationInterval = 15 * time.Second

	retryKafkaOperationInitialInterval    = 50 * time.Millisecond
	retryKafkaOperationMaxInterval        = 10 * time.Second
	retryKafkaOperationExpirationInterval = 30 * time.Second

	retryTaskProcessingInitialInterval = 50 * time.Millisecond
	retryTaskProcessingMaxInterval     = 100 * time.Millisecond
	retryTaskProcessingMaxAttempts     = 3

	contextExpireThreshold = 10 * time.Millisecond

	// FailureReasonCompleteResultExceedsLimit is failureReason for complete result exceeds limit
	FailureReasonCompleteResultExceedsLimit = "COMPLETE_RESULT_EXCEEDS_LIMIT"
	// FailureReasonFailureDetailsExceedsLimit is failureReason for failure details exceeds limit
	FailureReasonFailureDetailsExceedsLimit = "FAILURE_DETAILS_EXCEEDS_LIMIT"
	// FailureReasonCancelDetailsExceedsLimit is failureReason for cancel details exceeds limit
	FailureReasonCancelDetailsExceedsLimit = "CANCEL_DETAILS_EXCEEDS_LIMIT"
	// FailureReasonHeartbeatExceedsLimit is failureReason for heartbeat exceeds limit
	FailureReasonHeartbeatExceedsLimit = "HEARTBEAT_EXCEEDS_LIMIT"
	// FailureReasonDecisionBlobSizeExceedsLimit is the failureReason for decision blob exceeds size limit
	FailureReasonDecisionBlobSizeExceedsLimit = "DECISION_BLOB_SIZE_EXCEEDS_LIMIT"
	// FailureReasonSizeExceedsLimit is reason to fail workflow when history size or count exceed limit
	FailureReasonSizeExceedsLimit = "HISTORY_EXCEEDS_LIMIT"
	// FailureReasonTransactionSizeExceedsLimit is the failureReason for when transaction cannot be committed because it exceeds size limit
	FailureReasonTransactionSizeExceedsLimit = "TRANSACTION_SIZE_EXCEEDS_LIMIT"
)

var (
	// ErrBlobSizeExceedsLimit is error for event blob size exceeds limit
	ErrBlobSizeExceedsLimit = &workflow.BadRequestError{Message: "Blob data size exceeds limit."}
	// ErrContextTimeoutTooShort is error for setting a very short context timeout when calling a long poll API
	ErrContextTimeoutTooShort = &workflow.BadRequestError{Message: "Context timeout is too short."}
	// ErrContextTimeoutNotSet is error for not setting a context timeout when calling a long poll API
	ErrContextTimeoutNotSet = &workflow.BadRequestError{Message: "Context timeout is not set."}
)

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func AwaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {

	doneC := make(chan struct{})

	go func() {
		wg.Wait()
		close(doneC)
	}()

	select {
	case <-doneC:
		return true
	case <-time.After(timeout):
		return false
	}
}

// AddSecondsToBaseTime - Gets the UnixNano with given duration and base time.
func AddSecondsToBaseTime(baseTimeInNanoSec int64, durationInSeconds int64) int64 {
	timeOut := time.Duration(durationInSeconds) * time.Second
	return time.Unix(0, baseTimeInNanoSec).Add(timeOut).UnixNano()
}

// CreatePersistenceRetryPolicy creates a retry policy for persistence layer operations
func CreatePersistenceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPersistenceOperationInitialInterval)
	policy.SetMaximumInterval(retryPersistenceOperationMaxInterval)
	policy.SetExpirationInterval(retryPersistenceOperationExpirationInterval)

	return policy
}

// CreateHistoryServiceRetryPolicy creates a retry policy for calls to history service
func CreateHistoryServiceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(historyServiceOperationInitialInterval)
	policy.SetMaximumInterval(historyServiceOperationMaxInterval)
	policy.SetExpirationInterval(historyServiceOperationExpirationInterval)

	return policy
}

// CreateMatchingServiceRetryPolicy creates a retry policy for calls to matching service
func CreateMatchingServiceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(matchingServiceOperationInitialInterval)
	policy.SetMaximumInterval(matchingServiceOperationMaxInterval)
	policy.SetExpirationInterval(matchingServiceOperationExpirationInterval)

	return policy
}

// CreateFrontendServiceRetryPolicy creates a retry policy for calls to frontend service
func CreateFrontendServiceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(frontendServiceOperationInitialInterval)
	policy.SetMaximumInterval(frontendServiceOperationMaxInterval)
	policy.SetExpirationInterval(frontendServiceOperationExpirationInterval)

	return policy
}

// CreateAdminServiceRetryPolicy creates a retry policy for calls to matching service
func CreateAdminServiceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(adminServiceOperationInitialInterval)
	policy.SetMaximumInterval(adminServiceOperationMaxInterval)
	policy.SetExpirationInterval(adminServiceOperationExpirationInterval)

	return policy
}

// CreateKafkaOperationRetryPolicy creates a retry policy for kafka operation
func CreateKafkaOperationRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryKafkaOperationInitialInterval)
	policy.SetMaximumInterval(retryKafkaOperationMaxInterval)
	policy.SetExpirationInterval(retryKafkaOperationExpirationInterval)

	return policy
}

// CreateTaskProcessingRetryPolicy creates a retry policy for task processing
func CreateTaskProcessingRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryTaskProcessingInitialInterval)
	policy.SetMaximumInterval(retryTaskProcessingMaxInterval)
	policy.SetMaximumAttempts(retryTaskProcessingMaxAttempts)

	return policy
}

// IsPersistenceTransientError checks if the error is a transient persistence error
func IsPersistenceTransientError(err error) bool {
	switch err.(type) {
	case *workflow.InternalServiceError, *workflow.ServiceBusyError:
		return true
	}

	return false
}

// IsKafkaTransientError check if the error is a transient kafka error
func IsKafkaTransientError(err error) bool {
	return true
}

// IsServiceTransientError checks if the error is a transient error.
func IsServiceTransientError(err error) bool {
	switch err.(type) {
	case *workflow.InternalServiceError:
		return true
	case *workflow.ServiceBusyError:
		return true
	case *workflow.LimitExceededError:
		return true
	case *h.ShardOwnershipLostError:
		return true
	case *yarpcerrors.Status:
		// We only selectively retry the following yarpc errors client can safe retry with a backoff
		if yarpcerrors.IsDeadlineExceeded(err) ||
			yarpcerrors.IsUnavailable(err) ||
			yarpcerrors.IsUnknown(err) ||
			yarpcerrors.IsInternal(err) {
			return true
		}
		return false
	}

	return false
}

// WorkflowIDToHistoryShard is used to map a workflowID to a shardID
func WorkflowIDToHistoryShard(workflowID string, numberOfShards int) int {
	hash := farm.Fingerprint32([]byte(workflowID))
	return int(hash % uint32(numberOfShards))
}

// DomainIDToHistoryShard is used to map a domainID to a shardID
func DomainIDToHistoryShard(domainID string, numberOfShards int) int {
	hash := farm.Fingerprint32([]byte(domainID))
	return int(hash % uint32(numberOfShards))
}

// PrettyPrintHistory prints history in human readable format
func PrettyPrintHistory(history *workflow.History, logger log.Logger) {
	data, err := json.MarshalIndent(history, "", "    ")

	if err != nil {
		logger.Error("Error serializing history: %v\n", tag.Error(err))
	}

	fmt.Println("******************************************")
	fmt.Println("History", tag.DetailInfo(string(data)))
	fmt.Println("******************************************")
}

// IsValidContext checks that the thrift context is not expired on cancelled.
// Returns nil if the context is still valid. Otherwise, returns the result of
// ctx.Err()
func IsValidContext(ctx context.Context) error {
	ch := ctx.Done()
	if ch != nil {
		select {
		case <-ch:
			return ctx.Err()
		default:
			return nil
		}
	}
	deadline, ok := ctx.Deadline()
	if ok && time.Until(deadline) < contextExpireThreshold {
		return context.DeadlineExceeded
	}
	return nil
}

// GenerateRandomString is used for generate test string
func GenerateRandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("random")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// CreateMatchingPollForDecisionTaskResponse create response for matching's PollForDecisionTask
func CreateMatchingPollForDecisionTaskResponse(historyResponse *h.RecordDecisionTaskStartedResponse, workflowExecution *workflow.WorkflowExecution, token []byte) *m.PollForDecisionTaskResponse {
	matchingResp := &m.PollForDecisionTaskResponse{
		WorkflowExecution:         workflowExecution,
		TaskToken:                 token,
		Attempt:                   Int64Ptr(historyResponse.GetAttempt()),
		WorkflowType:              historyResponse.WorkflowType,
		StartedEventId:            historyResponse.StartedEventId,
		StickyExecutionEnabled:    historyResponse.StickyExecutionEnabled,
		NextEventId:               historyResponse.NextEventId,
		DecisionInfo:              historyResponse.DecisionInfo,
		WorkflowExecutionTaskList: historyResponse.WorkflowExecutionTaskList,
		BranchToken:               historyResponse.BranchToken,
		ScheduledTimestamp:        historyResponse.ScheduledTimestamp,
		StartedTimestamp:          historyResponse.StartedTimestamp,
		Queries:                   historyResponse.Queries,
	}
	if historyResponse.GetPreviousStartedEventId() != EmptyEventID {
		matchingResp.PreviousStartedEventId = historyResponse.PreviousStartedEventId
	}
	return matchingResp
}

// MinInt64 returns the smaller of two given int64
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxInt64 returns the greater of two given int64
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinInt32 return smaller one of two inputs int32
func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// MinInt returns the smaller of two given integers
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt returns the greater one of two given integers
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinDuration returns the smaller of two given time duration
func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// MaxDuration returns the greater of two given time durations
func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// SortInt64Slice sorts the given int64 slice.
// Sort is not guaranteed to be stable.
func SortInt64Slice(slice []int64) {
	sort.Slice(slice, func(i int, j int) bool {
		return slice[i] < slice[j]
	})
}

// ValidateRetryPolicy validates a retry policy
func ValidateRetryPolicy(policy *workflow.RetryPolicy) error {
	if policy == nil {
		// nil policy is valid which means no retry
		return nil
	}
	if policy.GetInitialIntervalInSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "InitialIntervalInSeconds must be greater than 0 on retry policy."}
	}
	if policy.GetBackoffCoefficient() < 1 {
		return &workflow.BadRequestError{Message: "BackoffCoefficient cannot be less than 1 on retry policy."}
	}
	if policy.GetMaximumIntervalInSeconds() < 0 {
		return &workflow.BadRequestError{Message: "MaximumIntervalInSeconds cannot be less than 0 on retry policy."}
	}
	if policy.GetMaximumIntervalInSeconds() > 0 && policy.GetMaximumIntervalInSeconds() < policy.GetInitialIntervalInSeconds() {
		return &workflow.BadRequestError{Message: "MaximumIntervalInSeconds cannot be less than InitialIntervalInSeconds on retry policy."}
	}
	if policy.GetMaximumAttempts() < 0 {
		return &workflow.BadRequestError{Message: "MaximumAttempts cannot be less than 0 on retry policy."}
	}
	if policy.GetExpirationIntervalInSeconds() < 0 {
		return &workflow.BadRequestError{Message: "ExpirationIntervalInSeconds cannot be less than 0 on retry policy."}
	}
	if policy.GetMaximumAttempts() == 0 && policy.GetExpirationIntervalInSeconds() == 0 {
		return &workflow.BadRequestError{Message: "MaximumAttempts and ExpirationIntervalInSeconds are both 0. At least one of them must be specified."}
	}
	return nil
}

// CreateHistoryStartWorkflowRequest create a start workflow request for history
func CreateHistoryStartWorkflowRequest(
	domainID string,
	startRequest *workflow.StartWorkflowExecutionRequest,
) *h.StartWorkflowExecutionRequest {
	now := time.Now()
	histRequest := &h.StartWorkflowExecutionRequest{
		DomainUUID:   StringPtr(domainID),
		StartRequest: startRequest,
	}
	firstDecisionTaskBackoffSeconds := backoff.GetBackoffForNextScheduleInSeconds(startRequest.GetCronSchedule(), now, now)
	if startRequest.RetryPolicy != nil && startRequest.RetryPolicy.GetExpirationIntervalInSeconds() > 0 {
		expirationInSeconds := startRequest.RetryPolicy.GetExpirationIntervalInSeconds() + firstDecisionTaskBackoffSeconds
		// expirationTime calculates from first decision task schedule to the end of the workflow
		deadline := now.Add(time.Duration(expirationInSeconds) * time.Second)
		histRequest.ExpirationTimestamp = Int64Ptr(deadline.Round(time.Millisecond).UnixNano())
	}
	histRequest.FirstDecisionTaskBackoffSeconds = Int32Ptr(firstDecisionTaskBackoffSeconds)
	return histRequest
}

// CheckEventBlobSizeLimit checks if a blob data exceeds limits. It logs a warning if it exceeds warnLimit,
// and return ErrBlobSizeExceedsLimit if it exceeds errorLimit.
func CheckEventBlobSizeLimit(
	actualSize int,
	warnLimit int,
	errorLimit int,
	domainID string,
	workflowID string,
	runID string,
	scope metrics.Scope,
	logger log.Logger,
	blobSizeViolationOperationTag tag.Tag,
) error {

	scope.RecordTimer(metrics.EventBlobSize, time.Duration(actualSize))

	if actualSize > warnLimit {
		if logger != nil {
			logger.Warn("Blob size exceeds limit.",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.WorkflowSize(int64(actualSize)),
				blobSizeViolationOperationTag)
		}

		if actualSize > errorLimit {
			return ErrBlobSizeExceedsLimit
		}
	}
	return nil
}

// ValidateLongPollContextTimeout check if the context timeout for a long poll handler is too short or below a normal value.
// If the timeout is not set or too short, it logs an error, and return ErrContextTimeoutNotSet or ErrContextTimeoutTooShort
// accordingly. If the timeout is only below a normal value, it just logs an info and return nil.
func ValidateLongPollContextTimeout(
	ctx context.Context,
	handlerName string,
	logger log.Logger,
) error {

	deadline, err := ValidateLongPollContextTimeoutIsSet(ctx, handlerName, logger)
	if err != nil {
		return err
	}
	timeout := time.Until(deadline)
	if timeout < MinLongPollTimeout {
		err := ErrContextTimeoutTooShort
		logger.Error("Context timeout is too short for long poll API.",
			tag.WorkflowHandlerName(handlerName), tag.Error(err), tag.WorkflowPollContextTimeout(timeout))
		return err
	}
	if timeout < CriticalLongPollTimeout {
		logger.Warn("Context timeout is lower than critical value for long poll API.",
			tag.WorkflowHandlerName(handlerName), tag.WorkflowPollContextTimeout(timeout))
	}
	return nil
}

// ValidateLongPollContextTimeoutIsSet checks if the context timeout is set for long poll requests.
func ValidateLongPollContextTimeoutIsSet(
	ctx context.Context,
	handlerName string,
	logger log.Logger,
) (time.Time, error) {

	deadline, ok := ctx.Deadline()
	if !ok {
		err := ErrContextTimeoutNotSet
		logger.Error("Context timeout not set for long poll API.",
			tag.WorkflowHandlerName(handlerName), tag.Error(err))
		return deadline, err
	}
	return deadline, nil
}

// GetSizeOfMapStringToByteArray get size of map[string][]byte
func GetSizeOfMapStringToByteArray(input map[string][]byte) int {
	if input == nil {
		return 0
	}

	res := 0
	for k, v := range input {
		res += len(k) + len(v)
	}
	return res + golandMapReserverNumberOfBytes
}

// GetSizeOfHistoryEvent returns approximate size in bytes of the history event taking into account byte arrays only now
func GetSizeOfHistoryEvent(event *workflow.HistoryEvent) uint64 {
	if event == nil {
		return 0
	}

	res := 0
	switch *event.EventType {
	case workflow.EventTypeWorkflowExecutionStarted:
		res += len(event.WorkflowExecutionStartedEventAttributes.Input)
		res += len(event.WorkflowExecutionStartedEventAttributes.ContinuedFailureDetails)
		res += len(event.WorkflowExecutionStartedEventAttributes.LastCompletionResult)
		if event.WorkflowExecutionStartedEventAttributes.Memo != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionStartedEventAttributes.Memo.Fields)
		}
		if event.WorkflowExecutionStartedEventAttributes.Header != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionStartedEventAttributes.Header.Fields)
		}
		if event.WorkflowExecutionStartedEventAttributes.SearchAttributes != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionStartedEventAttributes.SearchAttributes.IndexedFields)
		}
	case workflow.EventTypeWorkflowExecutionCompleted:
		res += len(event.WorkflowExecutionCompletedEventAttributes.Result)
	case workflow.EventTypeWorkflowExecutionFailed:
		res += len(event.WorkflowExecutionFailedEventAttributes.Details)
	case workflow.EventTypeWorkflowExecutionTimedOut:
	case workflow.EventTypeDecisionTaskScheduled:
	case workflow.EventTypeDecisionTaskStarted:
	case workflow.EventTypeDecisionTaskCompleted:
		res += len(event.DecisionTaskCompletedEventAttributes.ExecutionContext)
	case workflow.EventTypeDecisionTaskTimedOut:
	case workflow.EventTypeDecisionTaskFailed:
		res += len(event.DecisionTaskFailedEventAttributes.Details)
	case workflow.EventTypeActivityTaskScheduled:
		res += len(event.ActivityTaskScheduledEventAttributes.Input)
		if event.ActivityTaskScheduledEventAttributes.Header != nil {
			res += GetSizeOfMapStringToByteArray(event.ActivityTaskScheduledEventAttributes.Header.Fields)
		}
	case workflow.EventTypeActivityTaskStarted:
		res += len(event.ActivityTaskStartedEventAttributes.LastFailureDetails)
	case workflow.EventTypeActivityTaskCompleted:
		res += len(event.ActivityTaskCompletedEventAttributes.Result)
	case workflow.EventTypeActivityTaskFailed:
		res += len(event.ActivityTaskFailedEventAttributes.Details)
	case workflow.EventTypeActivityTaskTimedOut:
		res += len(event.ActivityTaskTimedOutEventAttributes.Details)
		res += len(event.ActivityTaskTimedOutEventAttributes.LastFailureDetails)
	case workflow.EventTypeActivityTaskCancelRequested:
	case workflow.EventTypeRequestCancelActivityTaskFailed:
	case workflow.EventTypeActivityTaskCanceled:
		res += len(event.ActivityTaskCanceledEventAttributes.Details)
	case workflow.EventTypeTimerStarted:
	case workflow.EventTypeTimerFired:
	case workflow.EventTypeCancelTimerFailed:
	case workflow.EventTypeTimerCanceled:
	case workflow.EventTypeWorkflowExecutionCancelRequested:
	case workflow.EventTypeWorkflowExecutionCanceled:
		res += len(event.WorkflowExecutionCanceledEventAttributes.Details)
	case workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
		res += len(event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes.Control)
	case workflow.EventTypeRequestCancelExternalWorkflowExecutionFailed:
		res += len(event.RequestCancelExternalWorkflowExecutionFailedEventAttributes.Control)
	case workflow.EventTypeExternalWorkflowExecutionCancelRequested:
	case workflow.EventTypeMarkerRecorded:
		res += len(event.MarkerRecordedEventAttributes.Details)
	case workflow.EventTypeWorkflowExecutionSignaled:
		res += len(event.WorkflowExecutionSignaledEventAttributes.Input)
	case workflow.EventTypeWorkflowExecutionTerminated:
		res += len(event.WorkflowExecutionTerminatedEventAttributes.Details)
	case workflow.EventTypeWorkflowExecutionContinuedAsNew:
		res += len(event.WorkflowExecutionContinuedAsNewEventAttributes.Input)
		if event.WorkflowExecutionContinuedAsNewEventAttributes.Memo != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionContinuedAsNewEventAttributes.Memo.Fields)
		}
		if event.WorkflowExecutionContinuedAsNewEventAttributes.Header != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionContinuedAsNewEventAttributes.Header.Fields)
		}
		if event.WorkflowExecutionContinuedAsNewEventAttributes.SearchAttributes != nil {
			res += GetSizeOfMapStringToByteArray(event.WorkflowExecutionContinuedAsNewEventAttributes.SearchAttributes.IndexedFields)
		}
	case workflow.EventTypeStartChildWorkflowExecutionInitiated:
		res += len(event.StartChildWorkflowExecutionInitiatedEventAttributes.Input)
		res += len(event.StartChildWorkflowExecutionInitiatedEventAttributes.Control)
		if event.StartChildWorkflowExecutionInitiatedEventAttributes.Memo != nil {
			res += GetSizeOfMapStringToByteArray(event.StartChildWorkflowExecutionInitiatedEventAttributes.Memo.Fields)
		}
		if event.StartChildWorkflowExecutionInitiatedEventAttributes.Header != nil {
			res += GetSizeOfMapStringToByteArray(event.StartChildWorkflowExecutionInitiatedEventAttributes.Header.Fields)
		}
		if event.StartChildWorkflowExecutionInitiatedEventAttributes.SearchAttributes != nil {
			res += GetSizeOfMapStringToByteArray(event.StartChildWorkflowExecutionInitiatedEventAttributes.SearchAttributes.IndexedFields)
		}
	case workflow.EventTypeStartChildWorkflowExecutionFailed:
		res += len(event.StartChildWorkflowExecutionFailedEventAttributes.Control)
	case workflow.EventTypeChildWorkflowExecutionStarted:
		if event.ChildWorkflowExecutionStartedEventAttributes == nil {
			return 0
		}
		if event.ChildWorkflowExecutionStartedEventAttributes.Header != nil {
			res += GetSizeOfMapStringToByteArray(event.ChildWorkflowExecutionStartedEventAttributes.Header.Fields)
		}
	case workflow.EventTypeChildWorkflowExecutionCompleted:
		res += len(event.ChildWorkflowExecutionCompletedEventAttributes.Result)
	case workflow.EventTypeChildWorkflowExecutionFailed:
		res += len(event.ChildWorkflowExecutionFailedEventAttributes.Details)
	case workflow.EventTypeChildWorkflowExecutionCanceled:
		res += len(event.ChildWorkflowExecutionCanceledEventAttributes.Details)
	case workflow.EventTypeChildWorkflowExecutionTimedOut:
	case workflow.EventTypeChildWorkflowExecutionTerminated:
	case workflow.EventTypeSignalExternalWorkflowExecutionInitiated:
		res += len(event.SignalExternalWorkflowExecutionInitiatedEventAttributes.Input)
		res += len(event.SignalExternalWorkflowExecutionInitiatedEventAttributes.Control)
	case workflow.EventTypeSignalExternalWorkflowExecutionFailed:
		res += len(event.SignalExternalWorkflowExecutionFailedEventAttributes.Control)
	case workflow.EventTypeExternalWorkflowExecutionSignaled:
		res += len(event.ExternalWorkflowExecutionSignaledEventAttributes.Control)
	case workflow.EventTypeUpsertWorkflowSearchAttributes:
		if event.UpsertWorkflowSearchAttributesEventAttributes.SearchAttributes != nil {
			res += GetSizeOfMapStringToByteArray(event.UpsertWorkflowSearchAttributesEventAttributes.SearchAttributes.IndexedFields)
		}
	}
	return uint64(res)
}

// IsJustOrderByClause return true is query start with order by
func IsJustOrderByClause(clause string) bool {
	whereClause := strings.TrimSpace(clause)
	whereClause = strings.ToLower(whereClause)
	return strings.HasPrefix(whereClause, "order by")
}

// ConvertIndexedValueTypeToThriftType takes fieldType as interface{} and convert to IndexedValueType.
// Because different implementation of dynamic config client may lead to different types
func ConvertIndexedValueTypeToThriftType(fieldType interface{}, logger log.Logger) workflow.IndexedValueType {
	switch t := fieldType.(type) {
	case float64:
		return workflow.IndexedValueType(fieldType.(float64))
	case int:
		return workflow.IndexedValueType(fieldType.(int))
	case workflow.IndexedValueType:
		return fieldType.(workflow.IndexedValueType)
	default:
		// Unknown fieldType, please make sure dynamic config return correct value type
		logger.Error("unknown index value type", tag.Value(fieldType), tag.ValueType(t))
		return fieldType.(workflow.IndexedValueType) // it will panic and been captured by logger
	}
}

// DeserializeSearchAttributeValue takes json encoded search attribute value and it's type as input, then
// unmarshal the value into a concrete type and return the value
func DeserializeSearchAttributeValue(value []byte, valueType workflow.IndexedValueType) (interface{}, error) {
	switch valueType {
	case workflow.IndexedValueTypeString, workflow.IndexedValueTypeKeyword:
		var val string
		if err := json.Unmarshal(value, &val); err != nil {
			var listVal []string
			err = json.Unmarshal(value, &listVal)
			return listVal, err
		}
		return val, nil
	case workflow.IndexedValueTypeInt:
		var val int64
		if err := json.Unmarshal(value, &val); err != nil {
			var listVal []int64
			err = json.Unmarshal(value, &listVal)
			return listVal, err
		}
		return val, nil
	case workflow.IndexedValueTypeDouble:
		var val float64
		if err := json.Unmarshal(value, &val); err != nil {
			var listVal []float64
			err = json.Unmarshal(value, &listVal)
			return listVal, err
		}
		return val, nil
	case workflow.IndexedValueTypeBool:
		var val bool
		if err := json.Unmarshal(value, &val); err != nil {
			var listVal []bool
			err = json.Unmarshal(value, &listVal)
			return listVal, err
		}
		return val, nil
	case workflow.IndexedValueTypeDatetime:
		var val time.Time
		if err := json.Unmarshal(value, &val); err != nil {
			var listVal []time.Time
			err = json.Unmarshal(value, &listVal)
			return listVal, err
		}
		return val, nil
	default:
		return nil, fmt.Errorf("error: unknown index value type [%v]", valueType)
	}
}

// GetDefaultAdvancedVisibilityWritingMode get default advancedVisibilityWritingMode based on
// whether related config exists in static config file.
func GetDefaultAdvancedVisibilityWritingMode(isAdvancedVisConfigExist bool) string {
	if isAdvancedVisConfigExist {
		return AdvancedVisibilityWritingModeOn
	}
	return AdvancedVisibilityWritingModeOff
}

// ConvertIntMapToDynamicConfigMapProperty converts a map whose key value type are both int to
// a map value that is compatible with dynamic config's map property
func ConvertIntMapToDynamicConfigMapProperty(
	intMap map[int]int,
) map[string]interface{} {
	dcValue := make(map[string]interface{})
	for key, value := range intMap {
		dcValue[strconv.Itoa(key)] = value
	}
	return dcValue
}

// ConvertDynamicConfigMapPropertyToIntMap convert a map property from dynamic config to a map
// whose type for both key and value are int
func ConvertDynamicConfigMapPropertyToIntMap(
	dcValue map[string]interface{},
) (map[int]int, error) {
	intMap := make(map[int]int)
	for key, value := range dcValue {
		intKey, err := strconv.Atoi(strings.TrimSpace(key))
		if err != nil {
			return nil, fmt.Errorf("failed to convert key %v, error: %v", key, err)
		}

		var intValue int
		switch value.(type) {
		case float64:
			intValue = int(value.(float64))
		case int:
			intValue = value.(int)
		case int32:
			intValue = int(value.(int32))
		case int64:
			intValue = int(value.(int64))
		default:
			return nil, fmt.Errorf("unknown value %v with type %T", value, value)
		}
		intMap[intKey] = intValue
	}
	return intMap, nil
}
