package caller

import (
	"context"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
)

var timesWorkerCallbackCalled atomic.Int64

func recordWorkerCallbackCalled() {
	timesWorkerCallbackCalled.Add(1)
}

func ResetTimesWorkerCallbackCalled() {
	timesWorkerCallbackCalled.Store(0)
}

func TimesWorkerCallbackCalled() int {
	return int(timesWorkerCallbackCalled.Load())
}

const NexusCompletionServiceName = "temporal.nexus.v1.CompletionService"

const OnCompleteOperationName = "OnComplete"

// OnCompletionCallContext is user-supplied data provided at the callsite.
type OnCompleteCallContext struct {
	Message string
}

type OnCompleteContext struct {
	// BUG: This should be defined in the SDK.
	CallContext any // User data supplied at the callsite.
}

type OnCompleteCompletion struct {
	// BUG: This should be defined in the SDK
	Success bool // Whether or not the operation failed.
}

type OnCompleteOperationInput struct {
	// BUG: This should be defined in the SDK.
	Context OnCompleteContext
	Result  OnCompleteCompletion
}

// UnitTypeHack is a hack to define a function that doesn't return a value.
// Is there something like this already exposed from the Go SDK?
type UnitTypeHack struct{}

var completionHandler = nexus.NewSyncOperation(
	OnCompleteOperationName,
	func(ctx context.Context, input OnCompleteOperationInput, options nexus.StartOperationOptions) (UnitTypeHack, error) {
		recordWorkerCallbackCalled()
		return UnitTypeHack{}, nil
	})
