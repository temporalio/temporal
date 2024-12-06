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

var ErrOperationTimeoutBelowMin = errors.New("remaining operation timeout is less than required minimum")

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
		handlerError := nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
		return e.saveResult(ctx, env, ref, nil, handlerError)
	}

	endpoint, err := e.lookupEndpoint(ctx, namespace.ID(ref.WorkflowKey.NamespaceID), args.endpointID, args.endpointName)
	if err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			// The endpoint is not registered, immediately fail the invocation.
			handlerError := nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return e.saveResult(ctx, env, ref, nil, handlerError)
		}
		return err
	}

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

	// Set MachineTransitionCount to 0 since older server versions, which had logic that considers references with
	// non-zero MachineTransitionCount as "non-concurrent" references, and would fail validation of the reference if the
	// Operation machine has transitioned.
	// TODO(bergundy): Remove this before the 1.27 release.
	smRef := common.CloneProto(ref.StateMachineRef)
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

	header := nexus.Header(args.header)
	callTimeout := e.Config.RequestTimeout(ns.Name().String(), task.EndpointName)
	if args.scheduleToCloseTimeout > 0 {
		opTimeout := args.scheduleToCloseTimeout - time.Since(args.scheduledTime)
		callTimeout = min(callTimeout, opTimeout)
		if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeoutHeader == "" {
			if header == nil {
				header = make(nexus.Header, 1)
			}
			header[nexus.HeaderOperationTimeout] = opTimeout.String()
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	startTime := time.Now()
	var rawResult *nexus.ClientStartOperationResult[*nexus.LazyValue]
	var callErr error
	if callTimeout < e.Config.MinOperationTimeout(ns.Name().String()) {
		callErr = ErrOperationTimeoutBelowMin
	} else {
		rawResult, callErr = client.StartOperation(callCtx, args.operation, args.payload, nexus.StartOperationOptions{
			Header:      header,
			CallbackURL: callbackURL,
			RequestID:   args.requestID,
			CallbackHeader: nexus.Header{
				commonnexus.CallbackTokenHeader: token,
			},
			Links: []nexus.Link{args.nexusLink},
		})
	}

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
	scheduledTime            time.Time
	scheduleToCloseTimeout   time.Duration
	header                   map[string]string
	payload                  *commonpb.Payload
	nexusLink                nexus.Link
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
		args.scheduledTime = event.EventTime.AsTime()
		args.scheduleToCloseTimeout = event.GetNexusOperationScheduledEventAttributes().GetScheduleToCloseTimeout().AsDuration()
		args.payload = event.GetNexusOperationScheduledEventAttributes().GetInput()
		args.header = event.GetNexusOperationScheduledEventAttributes().GetNexusHeader()
		args.nexusLink = ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
			Namespace:  ns.Name().String(),
			WorkflowId: ref.WorkflowKey.WorkflowID,
			RunId:      ref.WorkflowKey.RunID,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   event.GetEventId(),
					EventType: event.GetEventType(),
				},
			},
		})
		args.namespaceFailoverVersion = event.Version
		return nil
	})
	return
}

func (e taskExecutor) saveResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, result *nexus.ClientStartOperationResult[*commonpb.Payload], callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
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
							// TODO(rodrigozhou): links are non-essential for the execution of the workflow,
							// so ignoring the error for now; we will revisit how to handle these errors later.
							e.Logger.Error(
								fmt.Sprintf("failed to parse link to %q: %s", nexusLink.Type, nexusLink.URL),
								tag.Error(err),
							)
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
	var handlerError *nexus.HandlerError
	var opFailedError *nexus.UnsuccessfulOperationError

	if errors.As(callErr, &opFailedError) {
		return handleUnsuccessfulOperationError(node, operation, opFailedError, CompletionSourceResponse)
	} else if errors.As(callErr, &handlerError) {
		if !isRetryableHandlerError(handlerError.Type) {
			// The StartOperation request got an unexpected response that is not retryable, fail the operation.
			// Although Failure is nullable, Nexus SDK is expected to always populate this field
			return handleNonRetryableStartOperationError(env, node, operation, handlerError.Failure.Message)
		}
		// Fall through to the AttemptFailed transition.
	} else if errors.Is(callErr, ErrResponseBodyTooLarge) {
		// Following practices from workflow task completion payload size limit enforcement, we do not retry this
		// operation if the response body is too large.
		return handleNonRetryableStartOperationError(env, node, operation, callErr.Error())
	} else if errors.Is(callErr, ErrOperationTimeoutBelowMin) {
		// Operation timeout is not retryable
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
	return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(op, EventRescheduled{
			Node: node,
		})
	})
}

func (e taskExecutor) executeTimeoutTask(env hsm.Environment, node *hsm.Node, task TimeoutTask) error {
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
			handlerError := nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")

			// The endpoint is not registered, immediately fail the invocation.
			return e.saveCancelationResult(ctx, env, ref, handlerError)
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

	callTimeout := e.Config.RequestTimeout(ns.Name().String(), task.EndpointName)
	if args.scheduleToCloseTimeout > 0 {
		opTimeout := args.scheduleToCloseTimeout - time.Since(args.scheduledTime)
		callTimeout = min(callTimeout, opTimeout)
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	var callErr error
	startTime := time.Now()
	if callTimeout < e.Config.MinOperationTimeout(ns.Name().String()) {
		callErr = ErrOperationTimeoutBelowMin
	} else {
		callErr = handle.Cancel(callCtx, nexus.CancelOperationOptions{})
	}

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
	scheduledTime                                             time.Time
	scheduleToCloseTimeout                                    time.Duration
}

// loadArgsForCancelation loads state from the operation state machine that's the parent of the cancelation machine the
// given reference is pointing to.
func (e taskExecutor) loadArgsForCancelation(ctx context.Context, env hsm.Environment, ref hsm.Ref) (args cancelArgs, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(n *hsm.Node) error {
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
		args.scheduledTime = op.ScheduledTime.AsTime()
		args.scheduleToCloseTimeout = op.ScheduleToCloseTimeout.AsDuration()
		return nil
	})
	return
}

func (e taskExecutor) saveCancelationResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(n *hsm.Node) error {
		return hsm.MachineTransition(n, func(c Cancelation) (hsm.TransitionOutput, error) {
			if callErr != nil {
				if errors.Is(callErr, ErrOperationTimeoutBelowMin) {
					return TransitionCancelationFailed.Apply(c, EventCancelationFailed{
						Time: env.Now(),
						Err:  callErr,
						Node: n,
					})
				}
				var handlerErr *nexus.HandlerError
				if errors.As(callErr, &handlerErr) {
					if !isRetryableHandlerError(handlerErr.Type) {
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
	return hsm.MachineTransition(node, func(c Cancelation) (hsm.TransitionOutput, error) {
		return TransitionCancelationRescheduled.Apply(c, EventCancelationRescheduled{
			Node: node,
		})
	})
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
	var handlerError *nexus.HandlerError
	var opFailedError *nexus.UnsuccessfulOperationError

	if callErr != nil {
		if errors.Is(callErr, ErrOperationTimeoutBelowMin) {
			return "operation-timeout"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if errors.As(callErr, &opFailedError) {
			return "operation-unsuccessful:" + string(opFailedError.State)
		} else if errors.As(callErr, &handlerError) {
			return "handler-error:" + string(handlerError.Type)
		}
		return "unknown-error"
	}
	if result.Pending != nil {
		return "pending"
	}
	return "successful"
}

func cancelCallOutcomeTag(callCtx context.Context, callErr error) string {
	var handlerErr *nexus.HandlerError
	if callErr != nil {
		if errors.Is(callErr, ErrOperationTimeoutBelowMin) {
			return "operation-timeout"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if errors.As(callErr, &handlerErr) {
			return "handler-error:" + string(handlerErr.Type)
		}
		return "unknown-error"
	}
	return "successful"
}

func isRetryableHandlerError(eType nexus.HandlerErrorType) bool {
	switch eType {
	case nexus.HandlerErrorTypeResourceExhausted,
		nexus.HandlerErrorTypeInternal,
		nexus.HandlerErrorTypeUnavailable,
		nexus.HandlerErrorTypeUpstreamTimeout:
		return true
	case nexus.HandlerErrorTypeBadRequest,
		nexus.HandlerErrorTypeUnauthenticated,
		nexus.HandlerErrorTypeUnauthorized,
		nexus.HandlerErrorTypeNotFound,
		nexus.HandlerErrorTypeNotImplemented:
		return false
	default:
		// Default to retryable in case other error types are added in the future.
		// It's better to retry than unexpectedly fail.
		return true
	}
}

func isDestinationDown(err error) bool {
	var handlerError *nexus.HandlerError
	var opFailedErr *nexus.UnsuccessfulOperationError
	if errors.As(err, &opFailedErr) {
		return false
	}
	if errors.As(err, &handlerError) {
		return isRetryableHandlerError(handlerError.Type)
	}
	if errors.Is(err, ErrResponseBodyTooLarge) {
		return false
	}
	if errors.Is(err, ErrOperationTimeoutBelowMin) {
		return false
	}
	return true
}
