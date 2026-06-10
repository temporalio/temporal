package caller

import (
	"context"

	"go.temporal.io/server/tests/workercallbacks/handler"
)

const OnAddOperationCompleteCallbackActivityName = "OnAddOperationCompleteCallbackActivityName"

// onAddOperationCompleteCallbackActivity is the "worker callback", that will be invoked
// whenever the handler's AddOperation is completed.
//
// TODO(chrsmith): The input type should take another parameter, so we can pass more context
// about the source Nexus operation and request. Not just the output.
func onAddOperationCompleteCallbackActivity(ctx context.Context, input handler.AddOutput) error {
	panic("If you see this in the error logs, that's a good thing :) ")
}
