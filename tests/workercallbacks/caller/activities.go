package caller

import (
	"context"
	"log"
	"sync/atomic"

	"go.temporal.io/server/tests/workercallbacks/handler"
)

const OnAddOperationCompleteCallbackActivityName = "OnAddOperationCompleteCallbackActivityName"

// timesCalled counts how many times onAddOperationCompleteCallbackActivity has run. It is process
// global because the activity is registered on a worker, not constructed per-test; tests read it
// via TimesCalled and should reset it via ResetTimesCalled before exercising the callback.
var timesCalled atomic.Int32

// TimesCalled reports how many times the worker-callback activity has been invoked.
func TimesCalled() int {
	return int(timesCalled.Load())
}

// ResetTimesCalled resets the invocation counter. Call before triggering the callback.
func ResetTimesCalled() {
	timesCalled.Store(0)
}

// onAddOperationCompleteCallbackActivity is the "worker callback", that will be invoked
// whenever the handler's AddOperation is completed.
//
// TODO(chrsmith): The input type should take another parameter, so we can pass more context
// about the source Nexus operation and request. Not just the output.
func onAddOperationCompleteCallbackActivity(ctx context.Context, input handler.AddOutput) error {
	timesCalled.Add(1)
	log.Printf("onAddOperationCompleteCallbackActivity called!")
	return nil
}
