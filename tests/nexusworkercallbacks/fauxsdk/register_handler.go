package fauxsdk

import (
	"context"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"

	"go.temporal.io/sdk/worker"
)

// CompletionContext is the user-friendly type for fetching source call context when
// executing a worker callback.
type CompletionContext struct {
	context.Context

	rawCallCtx *commonpb.Payload
}

// Completion is a discriminated untion with the result of the source operation.
type Completion struct {
	Success *commonpb.Payload
	Failure *failurepb.Failure
}

// CompletionHandlerFn is the user-facing and friendlier signature for a completion handler.
type CompletionHandlerFn[T any] func(
	ctx CompletionContext,
	completion Completion,
) error

// UnitTypeHack is a hack to define a function that doesn't return a value.
// Is there something like this already exposed from the Go SDK?
type UnitTypeHack struct{}

// SetCompletionHandler takes care of registering a new implementation of the CompletionService,
// and wiring the supplied handler to its OnComplete operation.
//
// This is also where the SDK's veneer for converting the nexuspb.OnCompleteHandlerInput proto
// into a more user-friendly signature.
func SetCompletionHandler[T any](
	w worker.Worker,
	completionFn CompletionHandlerFn[T],
) error {
	completionSvc := nexus.NewService("temporal.nexus.v1.CompletionService")

	// Define the OnComplete operation for the CompletionService for the worker and task queue.
	// This wraps the user-supplied completion handler for ergonomics.
	completionHandler := nexus.NewSyncOperation(
		"OnComplete",
		func(ctx context.Context, input *nexuspb.OnCompleteHandlerInput, options nexus.StartOperationOptions) (UnitTypeHack, error) {
			// Convert the OnCompleteHandlerInput into the SDK-types that are the parmaeters to the
			// user-defined completion handler.
			compCtx := CompletionContext{
				Context:    ctx,
				rawCallCtx: input.GetSourceContext(),
			}
			completion := Completion{
				Success: input.GetOutcome().GetSuccess(),
				Failure: input.GetOutcome().GetFailure(),
			}

			// Call the user's handler.
			err := completionFn(compCtx, completion)

			return UnitTypeHack{}, err
		})

	err := completionSvc.Register(completionHandler)
	if err != nil {
		return fmt.Errorf("registering Nexus service: %w", err)
	}

	w.RegisterNexusService(completionSvc)
	return nil
}
