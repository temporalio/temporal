package caller

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"

	notificationservicepb "go.temporal.io/api/notificationservice/v1"
)

// ReceivedCallback is the data received by the worker callback.
type ReceivedCallback struct {
	Input   *notificationservicepb.OnCompleteHandlerRequest
	Options nexus.StartOperationOptions
}

// In-memory cache of all worker callbacks received.
// BUG: Not threadsafe, tests cannot be ran in parallel, etc.
var receivedCallbacks []*ReceivedCallback

func recordWorkerCallbackCalled(input *notificationservicepb.OnCompleteHandlerRequest, opts nexus.StartOperationOptions) {
	rc := &ReceivedCallback{
		Input:   input,
		Options: opts,
	}
	receivedCallbacks = append(receivedCallbacks, rc)
}

func ResetTimesWorkerCallbackCalled() {
	receivedCallbacks = nil
}

func TimesWorkerCallbackCalled() int {
	return len(receivedCallbacks)
}

func MustGetWorkerCallbackResult(idx int) *ReceivedCallback {
	return receivedCallbacks[idx]
}

const NexusNotificationServiceName = "temporal.notificationservice.v1.NotificationService"

const OnCompleteOperationName = "OnComplete"

// OnCompletionCallContext is user-supplied data provided at the callsite.
type OnCompleteCallContext struct {
	Message string
}

// Completion handler using the raw service contract for the NotificationService (the nexuspb.OnCompleteHandlerInput) parameter.
var completionHandler = nexus.NewSyncOperation(
	OnCompleteOperationName,
	func(ctx context.Context, input *notificationservicepb.OnCompleteHandlerRequest, options nexus.StartOperationOptions) (*notificationservicepb.OnCompleteHandlerResponse, error) {
		recordWorkerCallbackCalled(input, options)
		return &notificationservicepb.OnCompleteHandlerResponse{}, nil
	})

// TODO: `nexus.NewSyncOperation` is old and busted. Use `temporalnexus.NewTemporalOperation(...)`` instead.
