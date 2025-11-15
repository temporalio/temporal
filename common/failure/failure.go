package failure

import (
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/util"
)

const (
	failureSourceServer = "Server"
)

func NewServerFailure(message string, nonRetryable bool) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: nonRetryable,
		}},
	}

	return f
}

func NewResetWorkflowFailure(message string, lastHeartbeatDetails *commonpb.Payloads) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{
			LastHeartbeatDetails: lastHeartbeatDetails,
		}},
	}

	return f
}

func NewTimeoutFailure(message string, timeoutType enumspb.TimeoutType) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		Source:  failureSourceServer,
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: timeoutType,
		}},
	}

	return f
}

func Truncate(f *failurepb.Failure, maxSize int) *failurepb.Failure {
	return TruncateWithDepth(f, maxSize, 20)
}

func TruncateWithDepth(f *failurepb.Failure, maxSize, maxDepth int) *failurepb.Failure {
	if f == nil {
		return nil
	}

	// note that bytes are given to earlier calls first, so call in order of importance
	trunc := func(s string) string {
		s = util.TruncateUTF8(s, maxSize)
		maxSize -= len(s)
		if s != "" {
			maxSize -= 4 // account for proto overhead
		}
		return s
	}

	newFailure := &failurepb.Failure{}

	// Keep failure info for ApplicationFailureInfo and for ServerFailureInfo to persist NonRetryable flag.
	if i := f.GetApplicationFailureInfo(); i != nil {
		newFailure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: i.NonRetryable,
			Type:         trunc(i.Type),
		}}
		maxSize -= 8 // account for proto overhead
	} else if i := f.GetServerFailureInfo(); i != nil {
		newFailure.FailureInfo = &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: i.NonRetryable,
		}}
		maxSize -= 4 // account for proto overhead
	}

	newFailure.Source = trunc(f.Source)
	newFailure.Message = trunc(f.Message)
	newFailure.StackTrace = trunc(f.StackTrace)
	if f.Cause != nil && maxSize > 4 && maxDepth > 0 {
		newFailure.Cause = TruncateWithDepth(f.Cause, maxSize-4, maxDepth-1)
	}

	return newFailure
}

// IsRetryable determines if a failure is retryable based on its type and non-retryable types list.
func IsRetryable(failure *failurepb.Failure, nonRetryableTypes []string) bool {
	if failure == nil {
		return true
	}

	if failure.GetTerminatedFailureInfo() != nil || failure.GetCanceledFailureInfo() != nil {
		return false
	}

	if failure.GetTimeoutFailureInfo() != nil {
		timeoutType := failure.GetTimeoutFailureInfo().GetTimeoutType()
		if timeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
			timeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT {
			return !slices.Contains(
				nonRetryableTypes,
				retrypolicy.TimeoutFailureTypePrefix+timeoutType.String(),
			)
		}

		return false
	}

	if failure.GetServerFailureInfo() != nil {
		return !failure.GetServerFailureInfo().GetNonRetryable()
	}

	if failure.GetApplicationFailureInfo() != nil {
		if failure.GetApplicationFailureInfo().GetNonRetryable() {
			return false
		}

		return !slices.Contains(
			nonRetryableTypes,
			failure.GetApplicationFailureInfo().GetType(),
		)
	}
	return true
}
