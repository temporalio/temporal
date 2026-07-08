package caller

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
)

// ReceivedCallback is the data received by the worker callback.
type ReceivedCallback struct {
	Input   *nexuspb.OnCompleteHandlerInput
	Options nexus.StartOperationOptions
}

// In-memory cache of all worker callbacks received.
// BUG: Not threadsafe, tests cannot be ran in parallel, etc.
var receivedCallbacks []*ReceivedCallback

func recordWorkerCallbackCalled(input *nexuspb.OnCompleteHandlerInput, opts nexus.StartOperationOptions) {
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

const NexusCompletionServiceName = "temporal.nexus.v1.CompletionService"

const OnCompleteOperationName = "OnComplete"

// OnCompletionCallContext is user-supplied data provided at the callsite.
type OnCompleteCallContext struct {
	Message string
}

// UnitTypeHack is a hack to define a function that doesn't return a value.
// Is there something like this already exposed from the Go SDK?
type UnitTypeHack struct{}

var completionHandler = nexus.NewSyncOperation(
	OnCompleteOperationName,
	func(ctx context.Context, input *nexuspb.OnCompleteHandlerInput, options nexus.StartOperationOptions) (UnitTypeHack, error) {
		recordWorkerCallbackCalled(input, options)
		return UnitTypeHack{}, nil
	})

// TODO: `nexus.NewSyncOperation` is old and busted. Use `temporalnexus.NewTemporalOperation(...)`` instead.
