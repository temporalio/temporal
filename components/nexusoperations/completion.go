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
	"go.temporal.io/api/serviceerror"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/types/known/timestamppb"
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
				RequestId:        operation.RequestId,
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
					RequestId:        operation.RequestId,
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
					RequestId:        operation.RequestId,
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
		return hsm.TransitionOutput{}, fmt.Errorf("unexpected operation state: %v", opFailedError.State)
	}
}

// Adds a NEXUS_OPERATION_STARTED history event and sets the operation state machine to NEXUS_OPERATION_STATE_STARTED.
// Necessary if the completion is received before the start response.
func fabricateStartedEventIfMissing(
	node *hsm.Node,
	requestID string,
	operationID string,
	startTime *timestamppb.Timestamp,
	links []*commonpb.Link,
) error {
	operation, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}

	if TransitionStarted.Possible(operation) {
		eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
		if err != nil {
			return err
		}

		operation.OperationId = operationID

		event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
					OperationId:      operationID,
					RequestId:        requestID,
				},
			}
			e.Links = links
			if startTime != nil {
				e.EventTime = startTime
			}
		})

		_, err = TransitionStarted.Apply(operation, EventStarted{
			Time:       event.EventTime.AsTime(),
			Node:       node,
			Attributes: event.GetNexusOperationStartedEventAttributes(),
		})

		return err
	}

	return nil
}

func CompletionHandler(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	requestID string,
	operationID string,
	startTime *timestamppb.Timestamp,
	links []*commonpb.Link,
	result *commonpb.Payload,
	opFailedError *nexus.UnsuccessfulOperationError,
) error {
	// The initial version of the completion token did not include a request ID.
	// Only retry Access without a run ID if the request ID is not empty.
	isRetryableNotFoundErr := requestID != ""
	err := env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return serviceerror.NewNotFound("operation not found")
		}
		if err := fabricateStartedEventIfMissing(node, requestID, operationID, startTime, links); err != nil {
			return err
		}
		err := hsm.MachineTransition(node, func(operation Operation) (hsm.TransitionOutput, error) {
			if requestID != "" && operation.RequestId != requestID {
				isRetryableNotFoundErr = false
				return hsm.TransitionOutput{}, serviceerror.NewNotFound("operation not found")
			}
			if opFailedError != nil {
				return handleUnsuccessfulOperationError(node, operation, opFailedError, CompletionSourceCallback)
			}
			return handleSuccessfulOperationResult(node, operation, result, CompletionSourceCallback)
		})
		// TODO(bergundy): Remove this once the operation auto-deletes itself from the tree on completion.
		if errors.Is(err, hsm.ErrInvalidTransition) {
			isRetryableNotFoundErr = false
			return serviceerror.NewNotFound("operation not found")
		}
		return err
	})
	if errors.As(err, new(*serviceerror.NotFound)) && isRetryableNotFoundErr && ref.WorkflowKey.RunID != "" {
		// Try again without a run ID in case the original run was reset.
		ref.WorkflowKey.RunID = ""
		return CompletionHandler(ctx, env, ref, requestID, operationID, startTime, links, result, opFailedError)
	}
	return err
}
