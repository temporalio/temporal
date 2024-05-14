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
	"strings"
	"text/template"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type ClientProvider func(ctx context.Context, key queues.NamespaceIDAndDestination, service string) (*nexus.Client, error)

// EndpointChecker checks if an endpoint exists, should return serviceerror.NotFound if the endpoint does not exist or
// any other error to indicate lookup has failed.
type EndpointChecker func(ctx context.Context, namespaceName, endpointName string) error

type ActiveExecutorOptions struct {
	fx.In

	Config                 *Config
	NamespaceRegistry      namespace.Registry
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
	ClientProvider         ClientProvider
	EndpointChecker        EndpointChecker
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
	if err := hsm.RegisterExecutor(registry, TaskTypeTimeout.ID, exec.executeTimeoutTask); err != nil {
		return err
	}
	if err := hsm.RegisterExecutor(registry, TaskTypeCancelation.ID, exec.executeCancelationTask); err != nil {
		return err
	}
	return hsm.RegisterExecutor(registry, TaskTypeCancelationBackoff.ID, exec.executeCancelationBackoffTask)
}

type activeExecutor struct {
	ActiveExecutorOptions
}

func (e activeExecutor) executeInvocationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task InvocationTask) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}
	if err := e.EndpointChecker(ctx, ns.Name().String(), task.Destination); err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			// The endpoint is not registered, immediately fail the invocation.
			return e.saveResult(ctx, env, ref, nil, &nexus.UnexpectedResponseError{
				Message: "endpoint not registered",
				Response: &http.Response{
					StatusCode: http.StatusNotFound,
				},
			})
		}
		return err
	}

	args, err := e.loadOperationArgs(ctx, env, ref)
	if err != nil {
		return fmt.Errorf("failed to load operation args: %w", err)
	}

	header := nexus.Header(args.header)
	if e.Config.CallbackURLTemplate() == "unset" {
		return serviceerror.NewInternal(fmt.Sprintf("dynamic config %q is unset", CallbackURLTemplate.Key().String()))
	}
	// TODO(bergundy): Consider caching this template.
	callbackURLTemplate, err := template.New("NexusCallbackURL").Parse(e.Config.CallbackURLTemplate())
	if err != nil {
		return fmt.Errorf("failed to parse callback URL template: %w", err)
	}
	builder := &strings.Builder{}
	err = callbackURLTemplate.Execute(builder, struct{ NamespaceName, NamespaceID string }{
		NamespaceName: ns.Name().String(),
		NamespaceID:   ns.ID().String(),
	})
	if err != nil {
		return fmt.Errorf("failed to format callback URL: %w", err)
	}
	callbackURL := builder.String()

	client, err := e.ClientProvider(
		ctx,
		queues.NamespaceIDAndDestination{
			NamespaceID: ref.WorkflowKey.GetNamespaceID(),
			Destination: task.Destination,
		},
		args.service,
	)
	if err != nil {
		return fmt.Errorf("failed to get a client: %w", err)
	}

	smRef := common.CloneProto(ref.StateMachineRef)
	// Reset the machine transition count to 0 so it is ignored in the completion staleness check.
	// This is to account for either the operation transitioning to STARTED state after a successful call but also to
	// account for the task timing out before we get a successful result and a transition to BACKING_OFF.
	smRef.MachineTransitionCount = 0

	token, err := e.CallbackTokenGenerator.Tokenize(&token.NexusOperationCompletion{
		NamespaceId: ref.WorkflowKey.NamespaceID,
		WorkflowId:  ref.WorkflowKey.WorkflowID,
		RunId:       ref.WorkflowKey.RunID,
		Ref:         smRef,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", queues.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.Destination),
	)
	defer cancel()

	rawResult, callErr := client.StartOperation(callCtx, args.operation, args.payload, nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   args.requestID,
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

	if err := e.saveResult(ctx, env, ref, result, callErr); err != nil {
		return fmt.Errorf("failed to save result: %w", err)
	}
	return nil
}

type startArgs struct {
	service                  string
	operation                string
	requestID                string
	header                   map[string]string
	payload                  *commonpb.Payload
	namespaceFailoverVersion int64
}

func (e activeExecutor) loadOperationArgs(ctx context.Context, env hsm.Environment, ref hsm.Ref) (args startArgs, err error) {
	var eventToken []byte
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		if err := checkParentIsRunning(node); err != nil {
			return err
		}
		operation, err := hsm.MachineData[Operation](node)
		if err != nil {
			return err
		}

		args.service = operation.Service
		args.operation = operation.Operation
		args.requestID = operation.RequestId
		eventToken = operation.ScheduledEventToken
		event, err := node.LoadHistoryEvent(ctx, eventToken)
		if err != nil {
			return nil
		}
		args.payload = event.GetNexusOperationScheduledEventAttributes().GetInput()
		args.header = event.GetNexusOperationScheduledEventAttributes().GetNexusHeader()
		args.namespaceFailoverVersion = event.Version
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
				return TransitionStarted.Apply(operation, EventStarted{
					Time:       env.Now(),
					Node:       node,
					Attributes: event.GetNexusOperationStartedEventAttributes(),
				})
			}
			// Operation completed synchronously. Store the result and update the state machine.
			attemptTime := env.Now()
			return handleSuccessfulOperationResult(node, operation, result.Successful, &attemptTime)
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
		attemptTime := env.Now()
		return handleUnsuccessfulOperationError(node, operation, opFailedError, &attemptTime)
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

func (e activeExecutor) executeCancelationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task CancelationTask) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}
	if err := e.EndpointChecker(ctx, ns.Name().String(), task.Destination); err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			// The endpoint is not registered, immediately fail the invocation.
			return e.saveCancelationResult(ctx, env, ref, &nexus.UnexpectedResponseError{
				Message: "endpoint not registered",
				Response: &http.Response{
					StatusCode: http.StatusNotFound,
				},
			})
		}
		return err
	}

	args, err := e.loadArgsForCancelation(ctx, env, ref)
	if err != nil {
		return fmt.Errorf("failed to load args: %w", err)
	}
	client, err := e.ClientProvider(
		ctx,
		queues.NamespaceIDAndDestination{
			NamespaceID: ref.WorkflowKey.NamespaceID,
			Destination: task.Destination,
		},
		args.service,
	)
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}
	handle, err := client.NewHandle(args.operation, args.operationID)
	if err != nil {
		return fmt.Errorf("failed to get handle for operation: %w", err)
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.Destination),
	)
	defer cancel()

	callErr := handle.Cancel(callCtx, nexus.CancelOperationOptions{})
	return e.saveCancelationResult(ctx, env, ref, callErr)
}

type cancelArgs struct {
	service, operation, operationID string
}

// loadArgsForCancelation loads state from the operation state machine that's the parent of the cancelation machine the
// given reference is pointing to.
func (e activeExecutor) loadArgsForCancelation(ctx context.Context, env hsm.Environment, ref hsm.Ref) (args cancelArgs, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(n *hsm.Node) error {
		if err := checkParentIsRunning(n.Parent); err != nil {
			return err
		}
		op, err := hsm.MachineData[Operation](n.Parent)
		if err != nil {
			return err
		}
		if !TransitionCanceled.Possible(op) {
			// Operation is already in a terminal state.
			return fmt.Errorf("%w: operation already in terminal state", consts.ErrStaleReference)
		}
		args.service = op.Service
		args.operation = op.Operation
		args.operationID = op.OperationId
		return nil
	})
	return
}

func (e activeExecutor) saveCancelationResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(n *hsm.Node) error {
		if err := checkParentIsRunning(n.Parent); err != nil {
			return err
		}
		return hsm.MachineTransition(n, func(c Cancelation) (hsm.TransitionOutput, error) {
			if callErr != nil {
				var unexpectedResponseErr *nexus.UnexpectedResponseError
				if errors.As(callErr, &unexpectedResponseErr) {
					response := unexpectedResponseErr.Response
					if response.StatusCode >= 400 && response.StatusCode < 500 && !slices.Contains(retryable4xxErrorTypes, response.StatusCode) {
						return TransitionCancelationFailed.Apply(c, EventCancelationFailed{
							Time: env.Now(),
							Err:  callErr,
							Node: n,
						})
					}
				}
				return TransitionCancelationAttemptFailed.Apply(c, EventCancelationAttemptFailed{
					Time: env.Now(),
					Err:  callErr,
					Node: n,
				})
			}
			// Cancelation request transmitted successfully.
			// The operation is not yet canceled and may ignore our request, the outcome will be known via the
			// completion callback.
			return TransitionCancelationSucceeded.Apply(c, EventCancelationSucceeded{
				Time: env.Now(),
				Node: n,
			})
		})
	})
}

func (e activeExecutor) executeCancelationBackoffTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task CancelationBackoffTask) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := checkParentIsRunning(node.Parent); err != nil {
			return err
		}
		return hsm.MachineTransition(node, func(c Cancelation) (hsm.TransitionOutput, error) {
			return TransitionCancelationRescheduled.Apply(c, EventCancelationRescheduled{
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
				Endpoint:         operation.Endpoint,
				Service:          operation.Service,
				Operation:        operation.Operation,
				OperationId:      operation.OperationId,
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
