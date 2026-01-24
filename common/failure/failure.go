package failure

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/util"
)

const (
	failureSourceServer = "Server"
)

func NewServerFailure(message string, nonRetryable bool) *failurepb.Failure {
	f := failurepb.Failure_builder{
		Message: message,
		ServerFailureInfo: failurepb.ServerFailureInfo_builder{
			NonRetryable: nonRetryable,
		}.Build(),
	}.Build()

	return f
}

func NewResetWorkflowFailure(message string, lastHeartbeatDetails *commonpb.Payloads) *failurepb.Failure {
	f := failurepb.Failure_builder{
		Message: message,
		ResetWorkflowFailureInfo: failurepb.ResetWorkflowFailureInfo_builder{
			LastHeartbeatDetails: lastHeartbeatDetails,
		}.Build(),
	}.Build()

	return f
}

func NewTimeoutFailure(message string, timeoutType enumspb.TimeoutType) *failurepb.Failure {
	f := failurepb.Failure_builder{
		Message: message,
		Source:  failureSourceServer,
		TimeoutFailureInfo: failurepb.TimeoutFailureInfo_builder{
			TimeoutType: timeoutType,
		}.Build(),
	}.Build()

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
		newFailure.SetApplicationFailureInfo(failurepb.ApplicationFailureInfo_builder{
			NonRetryable: i.GetNonRetryable(),
			Type:         trunc(i.GetType()),
		}.Build())
		maxSize -= 8 // account for proto overhead
	} else if i := f.GetServerFailureInfo(); i != nil {
		newFailure.SetServerFailureInfo(failurepb.ServerFailureInfo_builder{
			NonRetryable: i.GetNonRetryable(),
		}.Build())
		maxSize -= 4 // account for proto overhead
	}

	newFailure.SetSource(trunc(f.GetSource()))
	newFailure.SetMessage(trunc(f.GetMessage()))
	newFailure.SetStackTrace(trunc(f.GetStackTrace()))
	if f.HasCause() && maxSize > 4 && maxDepth > 0 {
		newFailure.SetCause(TruncateWithDepth(f.GetCause(), maxSize-4, maxDepth-1))
	}

	return newFailure
}
