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
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/net/context"

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

// CreatePersistanceRetryPolicy creates a retry policy for persistence layer operations
func CreatePersistanceRetryPolicy() backoff.RetryPolicy {
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

// IsServiceTransientError checks if the error is a retryable error.
func IsServiceTransientError(err error) bool {
	return !IsServiceNonRetryableError(err)
}

// IsServiceNonRetryableError checks if the error is a non retryable error.
func IsServiceNonRetryableError(err error) bool {
	switch err := err.(type) {
	case *workflow.EntityNotExistsError:
		return true
	case *workflow.BadRequestError:
		return true
	case *workflow.DomainNotActiveError:
		return true
	case *workflow.WorkflowExecutionAlreadyStartedError:
		return true
	case *workflow.CancellationAlreadyRequestedError:
		return true
	case *yarpcerrors.Status:
		if err.Code() != yarpcerrors.CodeDeadlineExceeded {
			return true
		}
		return false
	}

	return false
}

// IsWhitelistServiceTransientError checks if the error is a transient error.
func IsWhitelistServiceTransientError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}

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

// WorkflowIDToHistoryShard is used to map workflowID to a shardID
func WorkflowIDToHistoryShard(workflowID string, numberOfShards int) int {
	hash := farm.Fingerprint32([]byte(workflowID))
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
	if startRequest.RetryPolicy != nil && startRequest.RetryPolicy.GetExpirationIntervalInSeconds() > 0 {
		expirationInSeconds := startRequest.RetryPolicy.GetExpirationIntervalInSeconds()
		deadline := now.Add(time.Second * time.Duration(expirationInSeconds))
		histRequest.ExpirationTimestamp = Int64Ptr(deadline.Round(time.Millisecond).UnixNano())
	}
	histRequest.FirstDecisionTaskBackoffSeconds = Int32Ptr(backoff.GetBackoffForNextScheduleInSeconds(startRequest.GetCronSchedule(), now, now))
	return histRequest
}

// CheckEventBlobSizeLimit checks if a blob data exceeds limits. It logs a warning if it exceeds warnLimit,
// and return ErrBlobSizeExceedsLimit if it exceeds errorLimit.
func CheckEventBlobSizeLimit(actualSize, warnLimit, errorLimit int, domainID, workflowID, runID string, scope metrics.Scope, logger log.Logger) error {
	scope.RecordTimer(metrics.EventBlobSize, time.Duration(actualSize))

	if actualSize > warnLimit {
		if logger != nil {
			logger.Warn("Blob size exceeds limit.",
				tag.WorkflowDomainID(domainID), tag.WorkflowID(workflowID), tag.WorkflowRunID(runID), tag.WorkflowSize(int64(actualSize)))
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

// GetDefaultAdvancedVisibilityWritingMode get default advancedVisibilityWritingMode based on
// whether related config exists in static config file.
func GetDefaultAdvancedVisibilityWritingMode(isAdvancedVisConfigExist bool) string {
	if isAdvancedVisConfigExist {
		return AdvancedVisibilityWritingModeOn
	}
	return AdvancedVisibilityWritingModeOff
}
