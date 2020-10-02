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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

	replicationServiceBusyInitialInterval    = 2 * time.Second
	replicationServiceBusyMaxInterval        = 10 * time.Second
	replicationServiceBusyExpirationInterval = 30 * time.Second

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

// CreateTaskProcessingRetryPolicy creates a retry policy for task processing
func CreateTaskProcessingRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryTaskProcessingInitialInterval)
	policy.SetMaximumInterval(retryTaskProcessingMaxInterval)
	policy.SetMaximumAttempts(retryTaskProcessingMaxAttempts)

	return policy
}

// CreateReplicationServiceBusyRetryPolicy creates a retry policy to handle replication service busy
func CreateReplicationServiceBusyRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(replicationServiceBusyInitialInterval)
	policy.SetMaximumInterval(replicationServiceBusyMaxInterval)
	policy.SetExpirationInterval(replicationServiceBusyExpirationInterval)

	return policy
}

// IsPersistenceTransientError checks if the error is a transient persistence error
func IsPersistenceTransientError(err error) bool {
	switch err.(type) {
	case *serviceerror.Internal,
		*serviceerror.ResourceExhausted:
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
	switch err.(type) {
	case *serviceerror.NotFound,
		*serviceerror.InvalidArgument,
		*serviceerror.NamespaceNotActive,
		*serviceerror.WorkflowExecutionAlreadyStarted,
		*serviceerror.DeadlineExceeded,
		*serviceerror.CancellationAlreadyRequested:
		return true
	}

	return false
}

// IsDeadlineExceeded checks if the error is context timeout error
func IsDeadlineExceeded(err error) bool {
	var deadlineExceededSvcErr *serviceerror.DeadlineExceeded
	return errors.As(err, &deadlineExceededSvcErr) || errors.Is(err, context.DeadlineExceeded)
}

// IsWhitelistServiceTransientError checks if the error is a transient error.
func IsWhitelistServiceTransientError(err error) bool {
	switch err.(type) {
	case *serviceerror.Internal,
		*serviceerror.ResourceExhausted,
		*serviceerrors.ShardOwnershipLost,
		*serviceerror.Unavailable:
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

// WorkflowIDToHistoryShard is used to map namespaceID-workflowID pair to a shardID
func WorkflowIDToHistoryShard(namespaceID, workflowID string, numberOfShards int) int {
	idBytes := []byte(namespaceID + "_" + workflowID)
	hash := farm.Fingerprint32(idBytes)
	return int(hash%uint32(numberOfShards)) + 1 // ShardID starts with 1
}

// PrettyPrintHistory prints history in human readable format
func PrettyPrintHistory(history *historypb.History, logger log.Logger) {
	fmt.Println("******************************************")
	fmt.Println("History", proto.MarshalTextString(history))
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
		WorkflowTaskInfo:           historyResponse.WorkflowTaskInfo,
		WorkflowExecutionTaskQueue: historyResponse.WorkflowExecutionTaskQueue,
		EventStoreVersion:          historyResponse.EventStoreVersion,
		BranchToken:                historyResponse.BranchToken,
		ScheduledTime:              historyResponse.ScheduledTime,
		StartedTime:                historyResponse.StartedTime,
		Queries:                    historyResponse.Queries,
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

// MinTime returns the smaller of two given time.Time
func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
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

// MinTime returns the smaller of two given time.Time
func MaxTime(a, b time.Time) time.Time {
	if a.After(b) {
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

	initialIntervalInSeconds, ok := options[initialIntervalInSecondsConfigKey]
	if ok {
		defaultSettings.InitialInterval = time.Duration(initialIntervalInSeconds.(int)) * time.Second
	}

	maximumIntervalCoefficient, ok := options[maximumIntervalCoefficientConfigKey]
	if ok {
		defaultSettings.MaximumIntervalCoefficient = maximumIntervalCoefficient.(float64)
	}

	backoffCoefficient, ok := options[backoffCoefficientConfigKey]
	if ok {
		defaultSettings.BackoffCoefficient = backoffCoefficient.(float64)
	}

	maximumAttempts, ok := options[maximumAttemptsConfigKey]
	if ok {
		defaultSettings.MaximumAttempts = int32(maximumAttempts.(int))
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
	namespaceID string,
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
				tag.WorkflowNamespaceID(namespaceID),
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

// IsJustOrderByClause return true is query start with order by
func IsJustOrderByClause(clause string) bool {
	whereClause := strings.TrimSpace(clause)
	whereClause = strings.ToLower(whereClause)
	return strings.HasPrefix(whereClause, "order by")
}

// ConvertIndexedValueTypeToProtoType takes fieldType as interface{} and convert to IndexedValueType.
// Because different implementation of dynamic config client may lead to different types
func ConvertIndexedValueTypeToProtoType(fieldType interface{}, logger log.Logger) enumspb.IndexedValueType {
	switch t := fieldType.(type) {
	case float64:
		return enumspb.IndexedValueType(t)
	case int:
		return enumspb.IndexedValueType(t)
	case string:
		if ivt, ok := enumspb.IndexedValueType_value[t]; ok {
			return enumspb.IndexedValueType(ivt)
		}
	case enumspb.IndexedValueType:
		return t
	}

	// Unknown fieldType, please make sure dynamic config return correct value type
	logger.Error("unknown index value type", tag.Value(fieldType), tag.ValueType(fieldType))
	return fieldType.(enumspb.IndexedValueType) // it will panic and been captured by logger
}

// DeserializeSearchAttributeValue takes json encoded search attribute value and it's type as input, then
// unmarshal the value into a concrete type and return the value
func DeserializeSearchAttributeValue(value *commonpb.Payload, valueType enumspb.IndexedValueType) (interface{}, error) {
	switch valueType {
	case enumspb.INDEXED_VALUE_TYPE_STRING, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		var val string
		if err := payload.Decode(value, &val); err != nil {
			var listVal []string
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_INT:
		var val int64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []int64
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		var val float64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []float64
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		var val bool
		if err := payload.Decode(value, &val); err != nil {
			var listVal []bool
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		var val time.Time
		if err := payload.Decode(value, &val); err != nil {
			var listVal []time.Time
			err = payload.Decode(value, &listVal)
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

func GetPayloadsMapSize(data map[string]*commonpb.Payloads) int {
	size := 0
	for key, payloads := range data {
		size += len(key)
		size += payloads.Size()
	}

	return size
}

// GetWorkflowExecutionTimeout gets the default allowed execution timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowExecutionTimeout(
	namespace string,
	requestedTimeout time.Duration,
	getDefaultTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter,
	getMaxAllowedTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter) time.Duration {

	if requestedTimeout == 0 {
		requestedTimeout = getDefaultTimeoutFunc(namespace)
	}

	return timestamp.MinDuration(
		requestedTimeout,
		getMaxAllowedTimeoutFunc(namespace),
	)
}

// GetWorkflowRunTimeout gets the default allowed run timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowRunTimeout(
	namespace string,
	requestedTimeout time.Duration,
	executionTimeout time.Duration,
	getDefaultTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter,
	getMaxAllowedTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter) time.Duration {

	if requestedTimeout == 0 {
		requestedTimeout = getDefaultTimeoutFunc(namespace)
	}

	return timestamp.MinDuration(
		timestamp.MinDuration(
			requestedTimeout,
			executionTimeout,
		),
		getMaxAllowedTimeoutFunc(namespace),
	)
}

// GetWorkflowTaskTimeout gets the default allowed execution timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowTaskTimeout(
	namespace string,
	requestedTimeout time.Duration,
	getDefaultTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter) time.Duration {

	if requestedTimeout == 0 {
		return getDefaultTimeoutFunc(namespace)
	}

	return requestedTimeout
}
