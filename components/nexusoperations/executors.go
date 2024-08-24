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
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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

// ClientProvider provides a nexus client for a given endpoint.
type ClientProvider func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexus.Client, error)

type TaskExecutorOptions struct {
	fx.In

	Config                 *Config
	NamespaceRegistry      namespace.Registry
	MetricsHandler         metrics.Handler
	Logger                 log.Logger
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
	ClientProvider         ClientProvider
	EndpointRegistry       commonnexus.EndpointRegistry
}

func RegisterExecutor(
	registry *hsm.Registry,
	options TaskExecutorOptions,
) error {
	exec := taskExecutor{options}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		exec.executeInvocationTask,
	); err != nil {
		return err
	}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeBackoffTask,
	); err != nil {
		return err
	}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeTimeoutTask,
	); err != nil {
		return err
	}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		exec.executeCancelationTask,
	); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(
		registry,
		exec.executeCancelationBackoffTask,
	)
}

type taskExecutor struct {
	TaskExecutorOptions
}

func (e taskExecutor) executeInvocationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task InvocationTask) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	args, err := e.loadOperationArgs(ctx, ns, env, ref)
	if err != nil {
		return fmt.Errorf("failed to load operation args: %w", err)
	}

	// This happens when we accept the ScheduleNexusOperation command when the endpoint is not found in the registry as
	// indicated by the EndpointNotFoundAlwaysNonRetryable dynamic config.
	if args.endpointID == "" {
		return e.saveResult(ctx, env, ref, nil, &nexus.UnexpectedResponseError{
			Message: "endpoint not registered",
			Response: &http.Response{
				StatusCode: http.StatusNotFound,
			},
		})
	}

	endpoint, err := e.lookupEndpoint(ctx, namespace.ID(ref.WorkflowKey.NamespaceID), args.endpointID, args.endpointName)
	if err != nil {
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
		ref.WorkflowKey.GetNamespaceID(),
		endpoint,
		args.service,
	)
	if err != nil {
		return fmt.Errorf("failed to get a client: %w", err)
	}

	smRef := common.CloneProto(ref.StateMachineRef)
	// Unset the machine last update versioned transition so it is ignored in the completion staleness check.
	// This is to account for either the operation transitioning to STARTED state after a successful call but also to
	// account for the task timing out before we get a successful result and a transition to BACKING_OFF.
	smRef.MachineLastUpdateVersionedTransition = nil
	smRef.MachineTransitionCount = 0

	token, err := e.CallbackTokenGenerator.Tokenize(&token.NexusOperationCompletion{
		NamespaceId: ref.WorkflowKey.NamespaceID,
		WorkflowId:  ref.WorkflowKey.WorkflowID,
		RunId:       ref.WorkflowKey.RunID,
		Ref:         smRef,
		RequestId:   args.requestID,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", queues.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}

	nexusLink, err := ConvertLinkWorkflowEventToNexusLink(args.workflowEventLink)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.EndpointName),
	)
	defer cancel()

	// Make the call and record metrics.
	startTime := time.Now()
	rawResult, callErr := client.StartOperation(callCtx, args.operation, args.payload, nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   args.requestID,
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
		},
		Links: []nexus.Link{nexusLink},
	})

	methodTag := metrics.NexusMethodTag("StartOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	outcomeTag := metrics.OutcomeTag(startCallOutcomeTag(callCtx, rawResult, callErr))
	OutboundRequestCounter.With(e.MetricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeTag)
	OutboundRequestLatency.With(e.MetricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, outcomeTag)

	var result *nexus.ClientStartOperationResult[*commonpb.Payload]
	if callErr == nil {
		if rawResult.Pending != nil {
			result = &nexus.ClientStartOperationResult[*commonpb.Payload]{
				Pending: &nexus.OperationHandle[*commonpb.Payload]{
					Operation: rawResult.Pending.Operation,
					ID:        rawResult.Pending.ID,
				},
				Links: rawResult.Links,
			}
		} else {
			var payload *commonpb.Payload
			err := rawResult.Successful.Consume(&payload)
			if err != nil {
				callErr = err
			} else if payload.Size() > e.Config.PayloadSizeLimit(ns.Name().String()) {
				callErr = ErrResponseBodyTooLarge
			} else {
				result = &nexus.ClientStartOperationResult[*commonpb.Payload]{
					Successful: payload,
				}
			}
		}
	}

	if callErr != nil {
		e.Logger.Error("Nexus StartOperation request failed", tag.Error(callErr))
	}

	err = e.saveResult(ctx, env, ref, result, callErr)

	if callErr != nil && isDestinationDown(callErr) {
		err = queues.NewDestinationDownError(callErr.Error(), err)
	}

	return err
}

type startArgs struct {
	service                  string
	operation                string
	requestID                string
	endpointName             string
	endpointID               string
	header                   map[string]string
	payload                  *commonpb.Payload
	workflowEventLink        *commonpb.Link_WorkflowEvent
	namespaceFailoverVersion int64
}

func (e taskExecutor) loadOperationArgs(
	ctx context.Context,
	ns *namespace.Namespace,
	env hsm.Environment,
	ref hsm.Ref,
) (args startArgs, err error) {
	var eventToken []byte
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		operation, err := hsm.MachineData[Operation](node)
		if err != nil {
			return err
		}

		args.endpointName = operation.Endpoint
		args.endpointID = operation.EndpointId
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
		args.workflowEventLink = &commonpb.Link_WorkflowEvent{
			Namespace:  ns.Name().String(),
			WorkflowId: ref.WorkflowKey.WorkflowID,
			RunId:      ref.WorkflowKey.RunID,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   event.GetEventId(),
					EventType: event.GetEventType(),
				},
			},
		}
		args.namespaceFailoverVersion = event.Version
		return nil
	})
	return
}

func (e taskExecutor) saveResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, result *nexus.ClientStartOperationResult[*commonpb.Payload], callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
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
				var links []*commonpb.Link
				for _, nexusLink := range result.Links {
					switch nexusLink.Type {
					case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
						link, err := ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
						if err != nil {
							// silently ignore for now
							continue
						}
						links = append(links, &commonpb.Link{
							Variant: &commonpb.Link_WorkflowEvent_{
								WorkflowEvent: link,
							},
						})
					default:
						// If the link data type is unsupported, just ignore it for now.
						e.Logger.Error(fmt.Sprintf("invalid link data type: %q", nexusLink.Type))
					}
				}
				// Handler has indicated that the operation will complete asynchronously. Mark the operation as started
				// to allow it to complete via callback.
				event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
					// nolint:revive // We must mutate here even if the linter doesn't like it.
					e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
							ScheduledEventId: eventID,
							OperationId:      result.Pending.ID,
							RequestId:        operation.RequestId,
						},
					}
					// nolint:revive // We must mutate here even if the linter doesn't like it.
					e.Links = links
				})
				return TransitionStarted.Apply(operation, EventStarted{
					Time:       env.Now(),
					Node:       node,
					Attributes: event.GetNexusOperationStartedEventAttributes(),
				})
			}
			// Operation completed synchronously. Store the result and update the state machine.
			return handleSuccessfulOperationResult(node, operation, result.Successful, CompletionSourceResponse)
		})
	})
}

func (e taskExecutor) handleStartOperationError(env hsm.Environment, node *hsm.Node, operation Operation, callErr error) (hsm.TransitionOutput, error) {
	var unexpectedResponseError *nexus.UnexpectedResponseError
	var opFailedError *nexus.UnsuccessfulOperationError

	if errors.As(callErr, &opFailedError) {
		return handleUnsuccessfulOperationError(node, operation, opFailedError, CompletionSourceResponse)
	} else if errors.As(callErr, &unexpectedResponseError) {
		if !isRetryableHTTPResponse(unexpectedResponseError.Response) {
			// The StartOperation request got an unexpected response that is not retryable, fail the operation.
			return handleNonRetryableStartOperationError(env, node, operation, unexpectedResponseError.Message)
		}
		// Fall through to the AttemptFailed transition.
	} else if errors.Is(callErr, ErrResponseBodyTooLarge) {
		// Following practices from workflow task completion payload size limit enforcement, we do not retry this
		// operation if the response body is too large.
		return handleNonRetryableStartOperationError(env, node, operation, callErr.Error())
	}
	return TransitionAttemptFailed.Apply(operation, EventAttemptFailed{
		Time:        env.Now(),
		Err:         callErr,
		Node:        node,
		RetryPolicy: e.Config.RetryPolicy(),
	})
}

func handleNonRetryableStartOperationError(env hsm.Environment, node *hsm.Node, operation Operation, message string) (hsm.TransitionOutput, error) {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	attrs := &historypb.NexusOperationFailedEventAttributes{
		Failure: nexusOperationFailure(
			operation,
			eventID,
			&failurepb.Failure{
				Message: message,
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: true,
					},
				},
			},
		),
		ScheduledEventId: eventID,
		RequestId:        operation.RequestId,
	}
	node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
		// nolint:revive // We must mutate here even if the linter doesn't like it.
		e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
			NexusOperationFailedEventAttributes: attrs,
		}
	})

	return TransitionFailed.Apply(operation, EventFailed{
		Time:             env.Now(),
		Attributes:       attrs,
		CompletionSource: CompletionSourceResponse,
		Node:             node,
	})
}

func (e taskExecutor) executeBackoffTask(env hsm.Environment, node *hsm.Node, task BackoffTask) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(op, EventRescheduled{
			Node: node,
		})
	})
}

func (e taskExecutor) executeTimeoutTask(env hsm.Environment, node *hsm.Node, task TimeoutTask) error {
	if err := task.Validate(node); err != nil {
		return err
	}
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
		if err != nil {
			return hsm.TransitionOutput{}, err
		}
		node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, func(e *historypb.HistoryEvent) {
			// nolint:revive // We must mutate here even if the linter doesn't like it.
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
					RequestId:        op.RequestId,
				},
			}
		})

		return TransitionTimedOut.Apply(op, EventTimedOut{
			Node: node,
		})
	})
}

func (e taskExecutor) executeCancelationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task CancelationTask) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	args, err := e.loadArgsForCancelation(ctx, env, ref)
	if err != nil {
		return fmt.Errorf("failed to load args: %w", err)
	}

	endpoint, err := e.lookupEndpoint(ctx, namespace.ID(ref.WorkflowKey.NamespaceID), args.endpointID, args.endpointName)
	if err != nil {
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

	client, err := e.ClientProvider(
		ctx,
		ref.WorkflowKey.NamespaceID,
		endpoint,
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
		e.Config.RequestTimeout(ns.Name().String(), task.EndpointName),
	)
	defer cancel()

	// Make the call and record metrics.
	startTime := time.Now()
	callErr := handle.Cancel(callCtx, nexus.CancelOperationOptions{})

	methodTag := metrics.NexusMethodTag("CancelOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	statusCodeTag := metrics.OutcomeTag(cancelCallOutcomeTag(callCtx, callErr))
	OutboundRequestCounter.With(e.MetricsHandler).Record(1, namespaceTag, destTag, methodTag, statusCodeTag)
	OutboundRequestLatency.With(e.MetricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, statusCodeTag)

	if callErr != nil {
		e.Logger.Error("Nexus CancelOperation request failed", tag.Error(callErr))
	}

	err = e.saveCancelationResult(ctx, env, ref, callErr)

	if callErr != nil && isDestinationDown(callErr) {
		err = queues.NewDestinationDownError(callErr.Error(), err)
	}

	return err
}

type cancelArgs struct {
	service, operation, operationID, endpointID, endpointName string
}

// loadArgsForCancelation loads state from the operation state machine that's the parent of the cancelation machine the
// given reference is pointing to.
func (e taskExecutor) loadArgsForCancelation(ctx context.Context, env hsm.Environment, ref hsm.Ref) (args cancelArgs, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(n *hsm.Node) error {
		if err := n.CheckRunning(); err != nil {
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
		args.endpointID = op.EndpointId
		args.endpointName = op.Endpoint
		return nil
	})
	return
}

func (e taskExecutor) saveCancelationResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(n *hsm.Node) error {
		if err := n.CheckRunning(); err != nil {
			return err
		}
		return hsm.MachineTransition(n, func(c Cancelation) (hsm.TransitionOutput, error) {
			if callErr != nil {
				var unexpectedResponseErr *nexus.UnexpectedResponseError
				if errors.As(callErr, &unexpectedResponseErr) {
					if !isRetryableHTTPResponse(unexpectedResponseErr.Response) {
						return TransitionCancelationFailed.Apply(c, EventCancelationFailed{
							Time: env.Now(),
							Err:  callErr,
							Node: n,
						})
					}
				}
				return TransitionCancelationAttemptFailed.Apply(c, EventCancelationAttemptFailed{
					Time:        env.Now(),
					Err:         callErr,
					Node:        n,
					RetryPolicy: e.Config.RetryPolicy(),
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

func (e taskExecutor) executeCancelationBackoffTask(env hsm.Environment, node *hsm.Node, task CancelationBackoffTask) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(c Cancelation) (hsm.TransitionOutput, error) {
		return TransitionCancelationRescheduled.Apply(c, EventCancelationRescheduled{
			Node: node,
		})
	})
}

func (e taskExecutor) extractEndpointNameFromOperation(ctx context.Context, env hsm.Environment, ref hsm.Ref, getOpNode func(*hsm.Node) *hsm.Node) (endpoint string, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(n *hsm.Node) error {
		op, err := hsm.MachineData[Operation](getOpNode(n))
		if err != nil {
			return err
		}
		endpoint = op.Endpoint
		return nil
	})
	return
}

// lookupEndpint gets an endpoint from the registry, preferring to look up by ID and falling back to name lookup.
// The fallback is a temporary workaround for not implementing endpoint replication, and endpoint ID being a UUID set by
// the system. We try to get the endpoint by name to support cases where an operator manually created an endpoint with
// the same name in two replicas.
func (e taskExecutor) lookupEndpoint(ctx context.Context, namespaceID namespace.ID, endpointID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	entry, err := e.EndpointRegistry.GetByID(ctx, endpointID)
	if err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			return e.EndpointRegistry.GetByName(ctx, namespaceID, endpointName)
		}
		return nil, err
	}
	return entry, nil
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

func startCallOutcomeTag(callCtx context.Context, result *nexus.ClientStartOperationResult[*nexus.LazyValue], callErr error) string {
	var unexpectedResponseError *nexus.UnexpectedResponseError
	var opFailedError *nexus.UnsuccessfulOperationError

	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if errors.As(callErr, &opFailedError) {
			return "operation-unsuccessful:" + string(opFailedError.State)
		} else if errors.As(callErr, &unexpectedResponseError) {
			return fmt.Sprintf("request-error:%d", unexpectedResponseError.Response.StatusCode)
		}
		return "unknown-error"
	}
	if result.Pending != nil {
		return "pending"
	}
	return "successful"
}

func cancelCallOutcomeTag(callCtx context.Context, callErr error) string {
	var unexpectedResponseError *nexus.UnexpectedResponseError
	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if errors.As(callErr, &unexpectedResponseError) {
			return fmt.Sprintf("request-error:%d", unexpectedResponseError.Response.StatusCode)
		}
		return "unknown-error"
	}
	return "successful"
}

func isRetryableHTTPResponse(response *http.Response) bool {
	return response.StatusCode >= 500 || slices.Contains(retryable4xxErrorTypes, response.StatusCode)
}

func isDestinationDown(err error) bool {
	var unexpectedErr *nexus.UnexpectedResponseError
	var opFailedErr *nexus.UnsuccessfulOperationError
	if errors.As(err, &opFailedErr) {
		return false
	}
	if errors.As(err, &unexpectedErr) {
		return isRetryableHTTPResponse(unexpectedErr.Response)
	}
	if errors.Is(err, ErrResponseBodyTooLarge) {
		return false
	}
	return true
}
