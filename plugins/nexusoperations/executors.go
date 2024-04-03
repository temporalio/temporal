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
	"net/http"
	"slices"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type ClientProvider func(queues.NamespaceIDAndDestination, *nexuspb.OutgoingServiceSpec) (*nexus.Client, error)

type ActiveExecutorOptions struct {
	fx.In

	Config                 *Config
	NamespaceRegistry      namespace.Registry
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
	ClientProvider         ClientProvider
}

func RegisterExecutor(
	registry *hsm.Registry,
	options ActiveExecutorOptions,
) error {
	exec := activeExecutor{options}
	if err := hsm.RegisterExecutor(registry, TaskTypeInvocation.ID, exec.executeInvocationTask); err != nil {
		return err
	}
	if err := hsm.RegisterExecutor(registry, TaskTypeBackoff.ID, exec.executeBackoffTask); err != nil {
		return err
	}
	return hsm.RegisterExecutor(registry, TaskTypeTimeout.ID, exec.executeTimeoutTask)
}

type activeExecutor struct {
	ActiveExecutorOptions
}

func (e activeExecutor) executeInvocationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task InvocationTask) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, e.Config.InvocationTaskTimeout(ns.Name().String()))
	defer cancel()

	service, err := e.NamespaceRegistry.NexusOutgoingService(namespace.ID(ref.WorkflowKey.NamespaceID), task.Destination)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			// The mapping doesn't exist. Assume the registry's NexusOutgoingService will readthrough to verify this
			// isn't due to propagation delay and this error indicates that the service mapping was removed and the task
			// should immediately fail.
			return queues.NewUnprocessableTaskError(fmt.Sprintf("cannot find service in namespace outgoing task mapping: %s", task.Destination))
		}
		return err
	}

	operationName, requestID, userHeader, input, err := e.loadOperationArgs(ctx, env, ref)
	if err != nil {
		return err
	}

	header := nexus.Header(userHeader)
	callbackURL := service.PublicCallbackUrl
	client, err := e.ClientProvider(queues.NamespaceIDAndDestination{NamespaceID: ref.WorkflowKey.GetNamespaceID(), Destination: task.Destination}, service)
	if err != nil {
		return err
	}

	token, err := e.CallbackTokenGenerator.Tokenize(&token.NexusOperationCompletion{
		WorkflowId:               ref.WorkflowKey.WorkflowID,
		RunId:                    ref.WorkflowKey.RunID,
		NamespaceId:              ref.WorkflowKey.NamespaceID,
		OperationRequestId:       requestID,
		Path:                     ref.StateMachineRef.Path,
		NamespaceFailoverVersion: 0, // TODO(bergundy): Figure out how to populate this
	})
	if err != nil {
		return fmt.Errorf("%w: %w", queues.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}
	rawResult, callErr := client.StartOperation(ctx, operationName, input, nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   requestID,
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
		},
	})
	var result *nexus.ClientStartOperationResult[*commonpb.Payload]
	if callErr == nil {
		if rawResult.Pending != nil {
			result = &nexus.ClientStartOperationResult[*commonpb.Payload]{
				Pending: &nexus.OperationHandle[*commonpb.Payload]{
					Operation: rawResult.Pending.Operation,
					ID:        rawResult.Pending.ID,
				},
			}
		} else {
			var payload *commonpb.Payload
			err := rawResult.Successful.Consume(&payload)
			if err != nil {
				callErr = err
			} else {
				// TODO(bergundy): Limit payload size.
				result = &nexus.ClientStartOperationResult[*commonpb.Payload]{
					Successful: payload,
				}
			}
		}
	}

	return e.saveResult(ctx, env, ref, result, callErr)
}

func (e activeExecutor) loadOperationArgs(ctx context.Context, env hsm.Environment, ref hsm.Ref) (operationName string, requestID string, header map[string]string, payload *commonpb.Payload, err error) {
	var eventToken []byte
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		if err := checkParentIsRunning(node); err != nil {
			return err
		}
		operation, err := hsm.MachineData[Operation](node)
		if err != nil {
			return err
		}
		operationName = operation.Operation
		requestID = operation.RequestId
		eventToken = operation.ScheduledEventToken
		event, err := node.LoadHistoryEvent(ctx, eventToken)
		if err != nil {
			return nil
		}
		payload = event.GetNexusOperationScheduledEventAttributes().GetInput()
		header = event.GetNexusOperationScheduledEventAttributes().GetHeader()
		return nil
	})
	return
}

func (e activeExecutor) saveResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, result *nexus.ClientStartOperationResult[*commonpb.Payload], callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := checkParentIsRunning(node); err != nil {
			return err
		}
		return hsm.MachineTransition(node, func(operation Operation) (hsm.TransitionOutput, error) {
			if callErr != nil {
				return e.handleStartOperationError(env, node, operation, callErr)
			}
			eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
			if err != nil {
				return hsm.TransitionOutput{}, err
			}
			if result.Pending != nil {
				// Handler has indicated that the operation will complete asynchronously. Mark the operation as started
				// to allow it to complete via callback.
				event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
					// nolint:revive
					e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
							ScheduledEventId: eventID,
							OperationId:      result.Pending.ID,
						},
					}
				})
				if err != nil {
					return hsm.TransitionOutput{}, err
				}
				return TransitionStarted.Apply(operation, EventStarted{
					Time:       env.Now(),
					Node:       node,
					Attributes: event.GetNexusOperationStartedEventAttributes(),
				})
			}
			// Operation completed synchronously. Store the result and update the state machine.
			node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
				//nolint:revive
				e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
					NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
						ScheduledEventId: eventID,
						Result:           result.Successful,
					},
				}
			})
			attemptTime := env.Now()
			return TransitionSucceeded.Apply(operation, EventSucceeded{
				AttemptTime: &attemptTime,
				Node:        node,
			})
		})
	})
}

func (e activeExecutor) handleStartOperationError(env hsm.Environment, node *hsm.Node, operation Operation, callErr error) (hsm.TransitionOutput, error) {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	var unexpectedResponseError *nexus.UnexpectedResponseError
	var opFailedError *nexus.UnsuccessfulOperationError
	if errors.As(callErr, &opFailedError) {
		switch opFailedError.State {
		case nexus.OperationStateFailed:
			node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
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
				AttemptFailure: &AttemptFailure{
					Time: env.Now(),
					Err:  callErr,
				},
				Node: node,
			})
		case nexus.OperationStateCanceled:
			node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, func(e *historypb.HistoryEvent) {
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
				AttemptFailure: &AttemptFailure{
					Time: env.Now(),
					Err:  callErr,
				},
				Node: node,
			})
		default:
			// The client would return this as UnexpectedResponseError but we don't rely on that here, treat it as a retryable failure.
			// Fall through to the AttemptFailed transition.
			callErr = fmt.Errorf("unexpected operation state returned from handler: %v", opFailedError.State) // nolint:goerr113
		}
	} else if errors.As(callErr, &unexpectedResponseError) {
		response := unexpectedResponseError.Response
		if response.StatusCode >= 400 && response.StatusCode < 500 && !slices.Contains(retryable4xxErrorTypes, response.StatusCode) {
			// The StartOperation request got an unexpected response that is not retryable, fail the operation.
			node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
				// nolint:revive
				e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
					NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
						Failure: nexusOperationFailure(
							operation,
							eventID,
							&failurepb.Failure{
								Message: unexpectedResponseError.Message,
								FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
									ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
										NonRetryable: true,
									},
								},
							},
						),
						ScheduledEventId: eventID,
					},
				}
			})

			return TransitionFailed.Apply(operation, EventFailed{
				AttemptFailure: &AttemptFailure{
					Time: env.Now(),
					Err:  callErr,
				},
				Node: node,
			})
		}
		// Fall through to the AttemptFailed transition.
	} else if errors.Is(err, ErrResponseBodyTooLarge) {
		// Following practices from workflow task completion payload size limit enforcement, we do not retry this
		// operation if the response body is too large.
		return TransitionFailed.Apply(operation, EventFailed{
			AttemptFailure: &AttemptFailure{
				Time: env.Now(),
				Err:  callErr,
			},
			Node: node,
		})
	}
	return TransitionAttemptFailed.Apply(operation, EventAttemptFailed{
		AttemptFailure: AttemptFailure{
			Time: env.Now(),
			Err:  callErr,
		},
		Node: node,
	})
}

func (e activeExecutor) executeBackoffTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task BackoffTask) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := checkParentIsRunning(node); err != nil {
			return err
		}
		return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
			return TransitionRescheduled.Apply(op, EventRescheduled{
				Node: node,
			})
		})
	})
}

func (e activeExecutor) executeTimeoutTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task TimeoutTask) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := task.Validate(node); err != nil {
			return err
		}
		if err := checkParentIsRunning(node); err != nil {
			return err
		}
		return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
			eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
			if err != nil {
				return hsm.TransitionOutput{}, err
			}
			node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, func(e *historypb.HistoryEvent) {
				// nolint:revive
				e.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
					NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
						Failure: nexusOperationFailure(
							op,
							eventID,
							&failurepb.Failure{
								Message: "operation timed out",
								FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
									TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
										TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
									},
								},
							},
						),
						ScheduledEventId: eventID,
					},
				}
			})

			return TransitionTimedOut.Apply(op, EventTimedOut{
				Node: node,
			})
		})
	})
}

func nexusOperationFailure(operation Operation, scheduledEventID int64, cause *failurepb.Failure) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "nexus operation completed unsuccessfully",
		FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
			NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
				OperationId:      operation.OperationId,
				Operation:        operation.Operation,
				Service:          operation.Service,
				ScheduledEventId: scheduledEventID,
			},
		},
		Cause: cause,
	}
}

// checkParentIsRunning checks that the parent node is running if the operation is attached to a workflow execution.
func checkParentIsRunning(node *hsm.Node) error {
	if node.Parent != nil {
		execution, err := hsm.MachineData[interface{ IsWorkflowExecutionRunning() bool }](node.Parent)
		if err != nil {
			return err
		}
		if !execution.IsWorkflowExecutionRunning() {
			return consts.ErrWorkflowCompleted
		}
	}
	return nil
}
