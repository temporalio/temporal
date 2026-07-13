package fauxsdk

import (
	"context"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	notificationservicepb "go.temporal.io/api/notificationservice/v1"

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

// SetCompletionHandler takes care of registering a new implementation of the NotificationService,
// and wiring the supplied handler to its OnComplete operation.
//
// This is also where the SDK's veneer for converting the nexuspb.OnCompleteHandlerInput proto
// into a more user-friendly signature.
func SetCompletionHandler[T any](
	w worker.Worker,
	completionFn CompletionHandlerFn[T],
) error {
	notificationSvc := nexus.NewService("temporal.notificationservice.v1.NotificationService")

	// Define the OnComplete operation for the NotificationService for the worker and task queue.
	// This wraps the user-supplied completion handler for ergonomics.
	completionHandler := nexus.NewSyncOperation(
		"OnComplete",
		func(ctx context.Context, input *notificationservicepb.OnCompleteHandlerRequest, options nexus.StartOperationOptions) (*notificationservicepb.OnCompleteHandlerResponse, error) {
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

			return &notificationservicepb.OnCompleteHandlerResponse{}, err
		})

	err := notificationSvc.Register(completionHandler)
	if err != nil {
		return fmt.Errorf("registering Nexus service: %w", err)
	}

	w.RegisterNexusService(notificationSvc)
	return nil
}
