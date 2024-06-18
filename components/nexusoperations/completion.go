// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func handleSuccessfulOperationResult(
	node *hsm.Node,
	operation Operation,
	result *commonpb.Payload,
	completionSource CompletionSource,
) (hsm.TransitionOutput, error) {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
		// We must assign to this property, linter doesn't like this.
		// nolint:revive
		e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: eventID,
				Result:           result,
			},
		}
	})
	return TransitionSucceeded.Apply(operation, EventSucceeded{
		Time:             event.EventTime.AsTime(),
		Node:             node,
		CompletionSource: completionSource,
	})
}

func handleUnsuccessfulOperationError(
	node *hsm.Node,
	operation Operation,
	opFailedError *nexus.UnsuccessfulOperationError,
	completionSource CompletionSource,
) (hsm.TransitionOutput, error) {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	switch opFailedError.State { // nolint:exhaustive
	case nexus.OperationStateFailed:
		event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
			// We must assign to this property, linter doesn't like this.
			// nolint:revive
			e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
					Failure: nexusOperationFailure(
						operation,
						eventID,
						commonnexus.UnsuccessfulOperationErrorToTemporalFailure(opFailedError),
					),
					ScheduledEventId: eventID,
				},
			}
		})

		return TransitionFailed.Apply(operation, EventFailed{
			Time:             event.EventTime.AsTime(),
			Attributes:       event.GetNexusOperationFailedEventAttributes(),
			Node:             node,
			CompletionSource: completionSource,
		})
	case nexus.OperationStateCanceled:
		event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, func(e *historypb.HistoryEvent) {
			// We must assign to this property, linter doesn't like this.
			// nolint:revive
			e.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
					Failure: nexusOperationFailure(
						operation,
						eventID,
						commonnexus.UnsuccessfulOperationErrorToTemporalFailure(opFailedError),
					),
					ScheduledEventId: eventID,
				},
			}
		})

		return TransitionCanceled.Apply(operation, EventCanceled{
			Time:             event.EventTime.AsTime(),
			Node:             node,
			CompletionSource: completionSource,
		})
	default:
		// Both the Nexus Client and CompletionHandler reject invalid states, but just in case, we return this as a
		// transition error.
		return hsm.TransitionOutput{}, fmt.Errorf("unexpected operation state: %v", opFailedError.State) // nolint:goerr113
	}
}

func CompletionHandler(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	result *commonpb.Payload,
	opFailedError *nexus.UnsuccessfulOperationError,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return status.Errorf(codes.NotFound, "operation not found")
		}
		err := hsm.MachineTransition(node, func(operation Operation) (hsm.TransitionOutput, error) {
			if opFailedError != nil {
				return handleUnsuccessfulOperationError(node, operation, opFailedError, CompletionSourceCallback)
			}
			return handleSuccessfulOperationResult(node, operation, result, CompletionSourceCallback)
		})
		// TODO(bergundy): Remove this once the operation auto-deletes itself from the tree on completion.
		if errors.Is(err, hsm.ErrInvalidTransition) {
			return status.Errorf(codes.NotFound, "operation not found")
		}
		return err
	})
}
