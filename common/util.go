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

package common

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/gogo/protobuf/proto"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
)

const (
	persistenceClientRetryInitialInterval = 50 * time.Millisecond
	persistenceClientRetryMaxAttempts     = 2

	frontendClientRetryInitialInterval = 200 * time.Millisecond
	frontendClientRetryMaxAttempts     = 2

	historyClientRetryInitialInterval = 50 * time.Millisecond
	historyClientRetryMaxAttempts     = 2

	matchingClientRetryInitialInterval = 1000 * time.Millisecond
	matchingClientRetryMaxAttempts     = 2

	frontendHandlerRetryInitialInterval = 200 * time.Millisecond
	frontendHandlerRetryMaxInterval     = time.Second
	frontendHandlerRetryMaxAttempts     = 5

	historyHandlerRetryInitialInterval = 50 * time.Millisecond
	historyHandlerRetryMaxAttempts     = 2

	matchingHandlerRetryInitialInterval = 1000 * time.Millisecond
	matchingHandlerRetryMaxAttempts     = 2

	readTaskRetryInitialInterval    = 50 * time.Millisecond
	readTaskRetryMaxInterval        = 1 * time.Second
	readTaskRetryExpirationInterval = backoff.NoInterval

	completeTaskRetryInitialInterval = 100 * time.Millisecond
	completeTaskRetryMaxInterval     = 1 * time.Second
	completeTaskRetryMaxAttempts     = 10

	taskProcessingRetryInitialInterval = 50 * time.Millisecond
	taskProcessingRetryMaxAttempts     = 1

	taskRescheduleInitialInterval    = 1 * time.Second
	taskRescheduleBackoffCoefficient = 1.1
	taskRescheduleMaxInterval        = 3 * time.Minute

	taskNotReadyRescheduleInitialInterval    = 3 * time.Second
	taskNotReadyRescheduleBackoffCoefficient = 1.5
	taskNotReadyRescheduleMaxInterval        = 3 * time.Minute

	// dependencyTaskNotCompletedRescheduleInitialInterval is lower than the interval the ack level most queues are
	// updated at, which can lead to tasks being retried more frequently than they should be. If this becomes an issue,
	// we should consider increasing this interval.
	dependencyTaskNotCompletedRescheduleInitialInterval    = 3 * time.Second
	dependencyTaskNotCompletedRescheduleBackoffCoefficient = 1.5
	dependencyTaskNotCompletedRescheduleMaxInterval        = 3 * time.Minute

	taskResourceExhaustedRescheduleInitialInterval    = 3 * time.Second
	taskResourceExhaustedRescheduleBackoffCoefficient = 1.5
	taskResourceExhaustedRescheduleMaxInterval        = 5 * time.Minute

	sdkClientFactoryRetryInitialInterval    = 200 * time.Millisecond
	sdkClientFactoryRetryMaxInterval        = 5 * time.Second
	sdkClientFactoryRetryExpirationInterval = time.Minute

	defaultInitialInterval            = time.Second
	defaultMaximumIntervalCoefficient = 100.0
	defaultBackoffCoefficient         = 2.0
	defaultMaximumAttempts            = 0

	initialIntervalInSecondsConfigKey   = "InitialIntervalInSeconds"
	maximumIntervalCoefficientConfigKey = "MaximumIntervalCoefficient"
	backoffCoefficientConfigKey         = "BackoffCoefficient"
	maximumAttemptsConfigKey            = "MaximumAttempts"

	contextExpireThreshold = 10 * time.Millisecond

	// FailureReasonCompleteResultExceedsLimit is failureReason for complete result exceeds limit
	FailureReasonCompleteResultExceedsLimit = "Complete result exceeds size limit."
	// FailureReasonFailureDetailsExceedsLimit is failureReason for failure details exceeds limit
	FailureReasonFailureExceedsLimit = "Failure exceeds size limit."
	// FailureReasonCancelDetailsExceedsLimit is failureReason for cancel details exceeds limit
	FailureReasonCancelDetailsExceedsLimit = "Cancel details exceed size limit."
	// FailureReasonHeartbeatExceedsLimit is failureReason for heartbeat exceeds limit
	FailureReasonHeartbeatExceedsLimit = "Heartbeat details exceed size limit."
	// FailureReasonSizeExceedsLimit is reason to fail workflow when history size or count exceed limit
	FailureReasonSizeExceedsLimit = "Workflow history size / count exceeds limit."
	// FailureReasonTransactionSizeExceedsLimit is the failureReason for when transaction cannot be committed because it exceeds size limit
	FailureReasonTransactionSizeExceedsLimit = "Transaction size exceeds limit."
)

var (
	// ErrBlobSizeExceedsLimit is error for event blob size exceeds limit
	ErrBlobSizeExceedsLimit = serviceerror.NewInvalidArgument("Blob data size exceeds limit.")
	// ErrMemoSizeExceedsLimit is error for memo size exceeds limit
	ErrMemoSizeExceedsLimit = serviceerror.NewInvalidArgument("Memo size exceeds limit.")
	// ErrContextTimeoutTooShort is error for setting a very short context timeout when calling a long poll API
	ErrContextTimeoutTooShort = serviceerror.NewInvalidArgument("Context timeout is too short.")
	// ErrContextTimeoutNotSet is error for not setting a context timeout when calling a long poll API
	ErrContextTimeoutNotSet = serviceerror.NewInvalidArgument("Context timeout is not set.")
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

// CreatePersistenceClientRetryPolicy creates a retry policy for calls to persistence
func CreatePersistenceClientRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(persistenceClientRetryInitialInterval).
		WithMaximumAttempts(persistenceClientRetryMaxAttempts)
}

// CreateFrontendClientRetryPolicy creates a retry policy for calls to frontend service
func CreateFrontendClientRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(frontendClientRetryInitialInterval).
		WithMaximumAttempts(frontendClientRetryMaxAttempts)
}

// CreateHistoryClientRetryPolicy creates a retry policy for calls to history service
func CreateHistoryClientRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(historyClientRetryInitialInterval).
		WithMaximumAttempts(historyClientRetryMaxAttempts)

}

// CreateMatchingClientRetryPolicy creates a retry policy for calls to matching service
func CreateMatchingClientRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(matchingClientRetryInitialInterval).
		WithMaximumAttempts(matchingClientRetryMaxAttempts)
}

// CreateFrontendHandlerRetryPolicy creates a retry policy for calls to frontend service
func CreateFrontendHandlerRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(frontendHandlerRetryInitialInterval).
		WithMaximumInterval(frontendHandlerRetryMaxInterval).
		WithMaximumAttempts(frontendHandlerRetryMaxAttempts)
}

// CreateHistoryHandlerRetryPolicy creates a retry policy for calls to history service
func CreateHistoryHandlerRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(historyHandlerRetryInitialInterval).
		WithMaximumAttempts(historyHandlerRetryMaxAttempts)
}

// CreateMatchingHandlerRetryPolicy creates a retry policy for calls to matching service
func CreateMatchingHandlerRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(matchingHandlerRetryInitialInterval).
		WithMaximumAttempts(matchingHandlerRetryMaxAttempts)
}

// CreateReadTaskRetryPolicy creates a retry policy for loading background tasks
func CreateReadTaskRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(readTaskRetryInitialInterval).
		WithMaximumInterval(readTaskRetryMaxInterval).
		WithExpirationInterval(readTaskRetryExpirationInterval)
}

// CreateCompleteTaskRetryPolicy creates a retry policy for completing background tasks
func CreateCompleteTaskRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(completeTaskRetryInitialInterval).
		WithMaximumInterval(completeTaskRetryMaxInterval).
		WithMaximumAttempts(completeTaskRetryMaxAttempts)
}

// CreateTaskProcessingRetryPolicy creates a retry policy for task processing
func CreateTaskProcessingRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(taskProcessingRetryInitialInterval).
		WithMaximumAttempts(taskProcessingRetryMaxAttempts)
}

// CreateTaskReschedulePolicy creates a retry policy for rescheduling task with errors not equal to ErrTaskRetry
func CreateTaskReschedulePolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(taskRescheduleInitialInterval).
		WithBackoffCoefficient(taskRescheduleBackoffCoefficient).
		WithMaximumInterval(taskRescheduleMaxInterval).
		WithExpirationInterval(backoff.NoInterval)
}

// CreateDependencyTaskNotCompletedReschedulePolicy creates a retry policy for rescheduling task with
// ErrDependencyTaskNotCompleted
func CreateDependencyTaskNotCompletedReschedulePolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(dependencyTaskNotCompletedRescheduleInitialInterval).
		WithBackoffCoefficient(dependencyTaskNotCompletedRescheduleBackoffCoefficient).
		WithMaximumInterval(dependencyTaskNotCompletedRescheduleMaxInterval).
		WithExpirationInterval(backoff.NoInterval)
}

// CreateTaskNotReadyReschedulePolicy creates a retry policy for rescheduling task with ErrTaskRetry
func CreateTaskNotReadyReschedulePolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(taskNotReadyRescheduleInitialInterval).
		WithBackoffCoefficient(taskNotReadyRescheduleBackoffCoefficient).
		WithMaximumInterval(taskNotReadyRescheduleMaxInterval).
		WithExpirationInterval(backoff.NoInterval)
}

// CreateTaskResourceExhaustedReschedulePolicy creates a retry policy for rescheduling task with resource exhausted error
func CreateTaskResourceExhaustedReschedulePolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(taskResourceExhaustedRescheduleInitialInterval).
		WithBackoffCoefficient(taskResourceExhaustedRescheduleBackoffCoefficient).
		WithMaximumInterval(taskResourceExhaustedRescheduleMaxInterval).
		WithExpirationInterval(backoff.NoInterval)
}

// CreateSdkClientFactoryRetryPolicy creates a retry policy to handle SdkClientFactory NewClient when frontend service is not ready
func CreateSdkClientFactoryRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(sdkClientFactoryRetryInitialInterval).
		WithMaximumInterval(sdkClientFactoryRetryMaxInterval).
		WithExpirationInterval(sdkClientFactoryRetryExpirationInterval)
}

// IsPersistenceTransientError checks if the error is a transient persistence error
func IsPersistenceTransientError(err error) bool {
	switch err.(type) {
	case *serviceerror.Unavailable,
		*serviceerror.ResourceExhausted:
		return true
	}

	return false
}

// IsServiceTransientError checks if the error is a retryable error.
func IsServiceTransientError(err error) bool {
	switch err.(type) {
	case *serviceerror.NotFound,
		*serviceerror.NamespaceNotFound,
		*serviceerror.InvalidArgument,
		*serviceerror.NamespaceNotActive,
		*serviceerror.WorkflowExecutionAlreadyStarted:
		return false
	}

	if IsContextDeadlineExceededErr(err) {
		return false
	}

	if IsContextCanceledErr(err) {
		return false
	}

	return true
}

// IsContextDeadlineExceededErr checks if the error is context.DeadlineExceeded or serviceerror.DeadlineExceeded error
func IsContextDeadlineExceededErr(err error) bool {
	var deadlineExceededSvcErr *serviceerror.DeadlineExceeded
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &deadlineExceededSvcErr)
}

// IsContextCanceledErr checks if the error is context.Canceled or serviceerror.Canceled error
func IsContextCanceledErr(err error) bool {
	var canceledSvcErr *serviceerror.Canceled
	return errors.Is(err, context.Canceled) ||
		errors.As(err, &canceledSvcErr)
}

// IsServiceClientTransientError checks if the error is a transient error.
func IsServiceClientTransientError(err error) bool {
	if IsServiceHandlerRetryableError(err) {
		return true
	}

	switch err.(type) {
	case *serviceerror.ResourceExhausted,
		*serviceerrors.ShardOwnershipLost:
		return true
	}
	return false
}

func IsServiceHandlerRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.Internal,
		*serviceerror.Unavailable:
		return true
	}

	return false
}

func IsStickyWorkerUnavailable(err error) bool {
	switch err.(type) {
	case *serviceerrors.StickyWorkerUnavailable:
		return true
	}
	return false
}

// IsResourceExhausted checks if the error is a service busy error.
func IsResourceExhausted(err error) bool {
	switch err.(type) {
	case *serviceerror.ResourceExhausted:
		return true
	}
	return false
}

// WorkflowIDToHistoryShard is used to map namespaceID-workflowID pair to a shardID.
func WorkflowIDToHistoryShard(
	namespaceID string,
	workflowID string,
	numberOfShards int32,
) int32 {
	idBytes := []byte(namespaceID + "_" + workflowID)
	hash := farm.Fingerprint32(idBytes)
	return int32(hash%uint32(numberOfShards)) + 1 // ShardID starts with 1
}

// PrettyPrintHistory prints history in human-readable format
func PrettyPrintHistory(history *historypb.History, header ...string) {
	var sb strings.Builder
	sb.WriteString("==========================================================================\n")
	for _, h := range header {
		sb.WriteString(h)
		sb.WriteString("\n")
	}
	sb.WriteString("--------------------------------------------------------------------------\n")
	_ = proto.MarshalText(&sb, history)
	sb.WriteString("\n")
	fmt.Print(sb.String())
}

// PrettyPrintCommands prints commands in human-readable format
func PrettyPrintCommands(commands []*commandpb.Command, header ...string) {
	var sb strings.Builder
	sb.WriteString("==========================================================================\n")
	for _, h := range header {
		sb.WriteString(h)
		sb.WriteString("\n")
	}
	sb.WriteString("--------------------------------------------------------------------------\n")
	for _, command := range commands {
		_ = proto.MarshalText(&sb, command)
	}
	fmt.Print(sb.String())
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

// CreateMatchingPollWorkflowTaskQueueResponse create response for matching's PollWorkflowTaskQueue
func CreateMatchingPollWorkflowTaskQueueResponse(historyResponse *historyservice.RecordWorkflowTaskStartedResponse, workflowExecution *commonpb.WorkflowExecution, token []byte) *matchingservice.PollWorkflowTaskQueueResponse {
	matchingResp := &matchingservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  token,
		WorkflowExecution:          workflowExecution,
		WorkflowType:               historyResponse.WorkflowType,
		PreviousStartedEventId:     historyResponse.PreviousStartedEventId,
		StartedEventId:             historyResponse.StartedEventId,
		Attempt:                    historyResponse.GetAttempt(),
		NextEventId:                historyResponse.NextEventId,
		StickyExecutionEnabled:     historyResponse.StickyExecutionEnabled,
		TransientWorkflowTask:      historyResponse.TransientWorkflowTask,
		WorkflowExecutionTaskQueue: historyResponse.WorkflowExecutionTaskQueue,
		BranchToken:                historyResponse.BranchToken,
		ScheduledTime:              historyResponse.ScheduledTime,
		StartedTime:                historyResponse.StartedTime,
		Queries:                    historyResponse.Queries,
	}

	return matchingResp
}

// EnsureRetryPolicyDefaults ensures the policy subfields, if not explicitly set, are set to the specified defaults
func EnsureRetryPolicyDefaults(originalPolicy *commonpb.RetryPolicy, defaultSettings DefaultRetrySettings) {
	if originalPolicy.GetMaximumAttempts() == 0 {
		originalPolicy.MaximumAttempts = defaultSettings.MaximumAttempts
	}

	if timestamp.DurationValue(originalPolicy.GetInitialInterval()) == 0 {
		originalPolicy.InitialInterval = timestamp.DurationPtr(defaultSettings.InitialInterval)
	}

	if timestamp.DurationValue(originalPolicy.GetMaximumInterval()) == 0 {
		originalPolicy.MaximumInterval = timestamp.DurationPtr(time.Duration(defaultSettings.MaximumIntervalCoefficient) * timestamp.DurationValue(originalPolicy.GetInitialInterval()))
	}

	if originalPolicy.GetBackoffCoefficient() == 0 {
		originalPolicy.BackoffCoefficient = defaultSettings.BackoffCoefficient
	}
}

// ValidateRetryPolicy validates a retry policy
func ValidateRetryPolicy(policy *commonpb.RetryPolicy) error {
	if policy == nil {
		// nil policy is valid which means no retry
		return nil
	}

	if policy.GetMaximumAttempts() == 1 {
		// One maximum attempt effectively disable retries. Validating the
		// rest of the arguments is pointless
		return nil
	}
	if timestamp.DurationValue(policy.GetInitialInterval()) < 0 {
		return serviceerror.NewInvalidArgument("InitialInterval cannot be negative on retry policy.")
	}
	if policy.GetBackoffCoefficient() < 1 {
		return serviceerror.NewInvalidArgument("BackoffCoefficient cannot be less than 1 on retry policy.")
	}
	if timestamp.DurationValue(policy.GetMaximumInterval()) < 0 {
		return serviceerror.NewInvalidArgument("MaximumInterval cannot be negative on retry policy.")
	}
	if timestamp.DurationValue(policy.GetMaximumInterval()) > 0 && timestamp.DurationValue(policy.GetMaximumInterval()) < timestamp.DurationValue(policy.GetInitialInterval()) {
		return serviceerror.NewInvalidArgument("MaximumInterval cannot be less than InitialInterval on retry policy.")
	}
	if policy.GetMaximumAttempts() < 0 {
		return serviceerror.NewInvalidArgument("MaximumAttempts cannot be negative on retry policy.")
	}

	for _, nrt := range policy.NonRetryableErrorTypes {
		if strings.HasPrefix(nrt, TimeoutFailureTypePrefix) {
			timeoutTypeValue := nrt[len(TimeoutFailureTypePrefix):]
			timeoutType, ok := enumspb.TimeoutType_value[timeoutTypeValue]
			if !ok || enumspb.TimeoutType(timeoutType) == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid timeout type value: %v.", timeoutTypeValue))
			}
		}
	}

	return nil
}

func GetDefaultRetryPolicyConfigOptions() map[string]interface{} {
	return map[string]interface{}{
		initialIntervalInSecondsConfigKey:   int(defaultInitialInterval.Seconds()),
		maximumIntervalCoefficientConfigKey: defaultMaximumIntervalCoefficient,
		backoffCoefficientConfigKey:         defaultBackoffCoefficient,
		maximumAttemptsConfigKey:            defaultMaximumAttempts,
	}
}

func FromConfigToDefaultRetrySettings(options map[string]interface{}) DefaultRetrySettings {
	defaultSettings := DefaultRetrySettings{
		InitialInterval:            defaultInitialInterval,
		MaximumIntervalCoefficient: defaultMaximumIntervalCoefficient,
		BackoffCoefficient:         defaultBackoffCoefficient,
		MaximumAttempts:            defaultMaximumAttempts,
	}

	if seconds, ok := options[initialIntervalInSecondsConfigKey]; ok {
		defaultSettings.InitialInterval = time.Duration(
			number.NewNumber(
				seconds,
			).GetIntOrDefault(int(defaultInitialInterval.Nanoseconds())),
		) * time.Second
	}

	if coefficient, ok := options[maximumIntervalCoefficientConfigKey]; ok {
		defaultSettings.MaximumIntervalCoefficient = number.NewNumber(
			coefficient,
		).GetFloatOrDefault(defaultMaximumIntervalCoefficient)
	}

	if coefficient, ok := options[backoffCoefficientConfigKey]; ok {
		defaultSettings.BackoffCoefficient = number.NewNumber(
			coefficient,
		).GetFloatOrDefault(defaultBackoffCoefficient)
	}

	if attempts, ok := options[maximumAttemptsConfigKey]; ok {
		defaultSettings.MaximumAttempts = int32(number.NewNumber(
			attempts,
		).GetIntOrDefault(defaultMaximumAttempts))
	}

	return defaultSettings
}

// CreateHistoryStartWorkflowRequest create a start workflow request for history
func CreateHistoryStartWorkflowRequest(
	namespaceID string,
	startRequest *workflowservice.StartWorkflowExecutionRequest,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	now time.Time,
) *historyservice.StartWorkflowExecutionRequest {
	histRequest := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:              namespaceID,
		StartRequest:             startRequest,
		ContinueAsNewInitiator:   enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		Attempt:                  1,
		ParentExecutionInfo:      parentExecutionInfo,
		FirstWorkflowTaskBackoff: backoff.GetBackoffForNextScheduleNonNegative(startRequest.GetCronSchedule(), now, now),
	}

	if timestamp.DurationValue(startRequest.GetWorkflowExecutionTimeout()) > 0 {
		deadline := now.Add(timestamp.DurationValue(startRequest.GetWorkflowExecutionTimeout()))
		histRequest.WorkflowExecutionExpirationTime = timestamp.TimePtr(deadline.Round(time.Millisecond))
	}

	if len(startRequest.CronSchedule) != 0 {
		histRequest.ContinueAsNewInitiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
	}

	return histRequest
}

// CheckEventBlobSizeLimit checks if a blob data exceeds limits. It logs a warning if it exceeds warnLimit,
// and return ErrBlobSizeExceedsLimit if it exceeds errorLimit.
func CheckEventBlobSizeLimit(
	actualSize int,
	warnLimit int,
	errorLimit int,
	namespace string,
	workflowID string,
	runID string,
	metricsHandler metrics.MetricsHandler,
	logger log.Logger,
	blobSizeViolationOperationTag tag.ZapTag,
) error {

	metricsHandler.Histogram(metrics.EventBlobSize.GetMetricName(), metrics.EventBlobSize.GetMetricUnit()).Record(int64(actualSize))
	if actualSize > warnLimit {
		if logger != nil {
			logger.Warn("Blob data size exceeds the warning limit.",
				tag.WorkflowNamespace(namespace),
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

func GetPayloadsMapSize(data map[string]*commonpb.Payloads) int {
	size := 0
	for key, payloads := range data {
		size += len(key)
		size += payloads.Size()
	}

	return size
}

// OverrideWorkflowRunTimeout override the run timeout according to execution timeout
func OverrideWorkflowRunTimeout(
	workflowRunTimeout time.Duration,
	workflowExecutionTimeout time.Duration,
) time.Duration {

	if workflowExecutionTimeout == 0 {
		return workflowRunTimeout
	} else if workflowRunTimeout == 0 {
		return workflowExecutionTimeout
	}
	return util.Min(workflowRunTimeout, workflowExecutionTimeout)
}

// OverrideWorkflowTaskTimeout override the workflow task timeout according to default timeout or max timeout
func OverrideWorkflowTaskTimeout(
	namespace string,
	taskStartToCloseTimeout time.Duration,
	workflowRunTimeout time.Duration,
	getDefaultTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter,
) time.Duration {

	if taskStartToCloseTimeout == 0 {
		taskStartToCloseTimeout = getDefaultTimeoutFunc(namespace)
	}

	taskStartToCloseTimeout = util.Min(taskStartToCloseTimeout, MaxWorkflowTaskStartToCloseTimeout)

	if workflowRunTimeout == 0 {
		return taskStartToCloseTimeout
	}

	return util.Min(taskStartToCloseTimeout, workflowRunTimeout)
}

// CloneProto is a generic typed version of proto.Clone from gogoproto.
func CloneProto[T proto.Message](v T) T {
	return proto.Clone(v).(T)
}
