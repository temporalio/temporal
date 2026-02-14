package nexusoperations

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

type operationTimeoutBelowMinError struct {
	timeoutType enumspb.TimeoutType
}

func (o *operationTimeoutBelowMinError) Error() string {
	return fmt.Sprintf("not enough time to execute another request before %s timeout", o.timeoutType.String())
}

var ErrInvalidOperationToken = errors.New("invalid operation token")
var errRequestTimedOut = errors.New("request timed out")
var errOpProcessorFailed = errors.New("nexus operation processor failed")

const maxDuration = time.Duration(1<<63 - 1)

// ClientProvider provides a nexus client for a given endpoint.
type ClientProvider func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error)

type TaskExecutorOptions struct {
	fx.In

	Config                 *Config
	NamespaceRegistry      namespace.Registry
	MetricsHandler         metrics.Handler
	Logger                 log.Logger
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
	ClientProvider         ClientProvider
	EndpointRegistry       commonnexus.EndpointRegistry
	HTTPTraceProvider      commonnexus.HTTPClientTraceProvider
	HistoryClient          resource.HistoryClient
	ChasmRegistry          *chasm.Registry
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
		exec.executeScheduleToCloseTimeoutTask,
	); err != nil {
		return err
	}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeScheduleToStartTimeoutTask,
	); err != nil {
		return err
	}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeStartToCloseTimeoutTask,
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

func buildCallbackURL(
	useSystemCallback bool,
	callbackTemplate string,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
) (string, error) {
	if endpoint == nil {
		return commonnexus.SystemCallbackURL, nil
	}
	target := endpoint.GetEndpoint().GetSpec().GetTarget().GetVariant()
	if !useSystemCallback {
		return buildCallbackFromTemplate(callbackTemplate, ns)
	}
	switch target.(type) {
	case *persistencespb.NexusEndpointTarget_Worker_:
		return commonnexus.SystemCallbackURL, nil
	case *persistencespb.NexusEndpointTarget_External_:
		return buildCallbackFromTemplate(callbackTemplate, ns)
	default:
		return "", fmt.Errorf("unknown endpoint target type: %T", target)
	}
}

func buildCallbackFromTemplate(callbackTemplate string, ns *namespace.Namespace) (string, error) {
	if callbackTemplate == "unset" {
		return "", serviceerror.NewInternalf("dynamic config %q is unset", CallbackURLTemplate.Key().String())
	}
	callbackURLTemplate, err := template.New("NexusCallbackURL").Parse(callbackTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse callback URL template: %w", err)
	}
	builder := &strings.Builder{}
	err = callbackURLTemplate.Execute(builder, struct{ NamespaceName, NamespaceID string }{
		NamespaceName: ns.Name().String(),
		NamespaceID:   ns.ID().String(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to format callback URL: %w", err)
	}
	return builder.String(), nil
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
	var endpoint *persistencespb.NexusEndpointEntry

	// Skip endpoint lookup for system-internal operations.
	if args.endpointName != commonnexus.SystemEndpoint {
		// This happens when we accept the ScheduleNexusOperation command when the endpoint is not found in the registry as
		// indicated by the EndpointNotFoundAlwaysNonRetryable dynamic config.
		// The config has been removed but we keep this check for backward compatibility.
		if args.endpointID == "" {
			handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return e.saveResult(ctx, env, ref, nil, handlerError)
		}

		endpoint, err = e.lookupEndpoint(ctx, namespace.ID(ref.WorkflowKey.NamespaceID), args.endpointID, args.endpointName)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				// The endpoint is not registered, immediately fail the invocation.
				handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
				return e.saveResult(ctx, env, ref, nil, handlerError)
			}
			return err
		}
	}

	callbackURL, err := buildCallbackURL(e.Config.UseSystemCallbackURL(), e.Config.CallbackURLTemplate(), ns, endpoint)
	if err != nil {
		return fmt.Errorf("failed to build callback URL: %w", err)
	}

	// Set MachineTransitionCount to 0 since older server versions, which had logic that considers references with
	// non-zero MachineTransitionCount as "non-concurrent" references, and would fail validation of the reference if the
	// Operation machine has transitioned.
	// TODO(bergundy): Remove this before the 1.27 release.
	smRef := common.CloneProto(ref.StateMachineRef)
	smRef.MachineTransitionCount = 0

	// Set ms VT to initial version because workflow may switch to a different branch.
	smRef.MutableStateVersionedTransition = smRef.MachineInitialVersionedTransition

	token, err := e.CallbackTokenGenerator.Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: ref.WorkflowKey.NamespaceID,
		WorkflowId:  ref.WorkflowKey.WorkflowID,
		RunId:       ref.WorkflowKey.RunID,
		Ref:         smRef,
		RequestId:   args.requestID,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", queueserrors.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}

	callTimeout := e.Config.RequestTimeout(ns.Name().String(), task.EndpointName)
	var timeoutType enumspb.TimeoutType
	// Adjust timeout based on remaining operation timeouts.
	// ScheduleToStart takes precedence over ScheduleToClose since it is already capped by it.
	if args.scheduleToStartTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToStartTimeout-time.Since(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
	} else if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-time.Since(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}
	// Inform the handler of the operation timeout via header.
	// StartToClose takes precedence over ScheduleToClose since it is already capped by it.
	opTimeout := maxDuration
	if args.startToCloseTimeout > 0 {
		opTimeout = args.startToCloseTimeout
	}
	if args.scheduleToCloseTimeout > 0 {
		opTimeout = min(args.scheduleToCloseTimeout-time.Since(args.scheduledTime), opTimeout)
	}
	header := nexus.Header(args.header)
	if header == nil {
		header = make(nexus.Header, 1) // It's most likely that we'll only be setting the new wire format header.
	}
	// Set the operation timeout header if not already set.
	if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeout != maxDuration && opTimeoutHeader == "" {
		header[nexus.HeaderOperationTimeout] = commonnexus.FormatDuration(opTimeout)
	}
	if e.Config.UseNewFailureWireFormat(ns.Name().String()) {
		// If this request is handled by a newer server that supports Nexus failure serialization, trigger that behavior.
		header.Set(nexusrpc.HeaderTemporalNexusFailureSupport, "true")
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	// Set this value on the parent context so that our custom HTTP caller can mutate it since we cannot access response headers directly.
	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})

	options := nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   args.requestID,
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
		},
		Links: []nexus.Link{args.nexusLink},
	}

	var result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	var callErr error
	var startTime time.Time
	if callTimeout < e.Config.MinRequestTimeout(ns.Name().String()) {
		startTime = time.Now()
		callErr = &operationTimeoutBelowMinError{timeoutType: timeoutType}
	} else if args.endpointName == commonnexus.SystemEndpoint {
		startTime = time.Now()
		result, callErr = e.startOnHistoryService(callCtx, ns, args, options)
	} else {
		client, err := e.ClientProvider(
			callCtx,
			ns.ID().String(),
			endpoint,
			args.service,
		)
		if err != nil {
			return fmt.Errorf("failed to get a client: %w", err)
		}

		if e.HTTPTraceProvider != nil {
			traceLogger := log.With(e.Logger,
				tag.Operation("StartOperation"),
				tag.WorkflowNamespace(ns.Name().String()),
				tag.RequestID(args.requestID),
				tag.NexusOperation(args.operation),
				tag.Endpoint(args.endpointName),
				tag.WorkflowID(ref.WorkflowKey.WorkflowID),
				tag.WorkflowRunID(ref.WorkflowKey.RunID),
				tag.AttemptStart(time.Now().UTC()),
				tag.Attempt(task.Attempt),
			)
			if trace := e.HTTPTraceProvider.NewTrace(task.Attempt, traceLogger); trace != nil {
				callCtx = httptrace.WithClientTrace(callCtx, trace)
			}
		}
		startTime = time.Now()
		result, callErr = e.startViaHTTP(callCtx, client, args, options)
	}

	if result != nil {
		tokenLimit := e.Config.MaxOperationTokenLength(ns.Name().String())
		if result.Pending != nil && len(result.Pending.Token) > tokenLimit {
			callErr = fmt.Errorf("%w: length exceeds allowed limit (%d/%d)", ErrInvalidOperationToken, len(result.Pending.Token), tokenLimit)
		} else if result.Successful != nil && result.Successful.Size() > e.Config.PayloadSizeLimit(ns.Name().String()) {
			callErr = ErrResponseBodyTooLarge
		}
	}
	failureSource := failureSourceFromContext(callCtx)

	methodTag := metrics.NexusMethodTag("StartOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(args.endpointName)
	}
	outcomeTag := metrics.OutcomeTag(startCallOutcomeTag(callCtx, result, callErr))
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	chasmnexus.OutboundRequestCounter.With(e.MetricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)
	chasmnexus.OutboundRequestLatency.With(e.MetricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)

	if callErr != nil {
		if failureSource == commonnexus.FailureSourceWorker || errors.As(callErr, new(*operationTimeoutBelowMinError)) {
			e.Logger.Debug("Nexus StartOperation request failed", tag.Error(callErr))
		} else {
			e.Logger.Error("Nexus StartOperation request failed", tag.Error(callErr))
		}
	}

	err = e.saveResult(ctx, env, ref, result, callErr)

	if callErr != nil && isDestinationDown(callErr) {
		err = queueserrors.NewDestinationDownError(callErr.Error(), err)
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
	scheduleToStartTimeout   time.Duration
	scheduleToCloseTimeout   time.Duration
	startToCloseTimeout      time.Duration
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
		args.scheduledTime = operation.ScheduledTime.AsTime()
		args.scheduleToCloseTimeout = operation.ScheduleToCloseTimeout.AsDuration()
		args.scheduleToStartTimeout = operation.ScheduleToStartTimeout.AsDuration()
		args.startToCloseTimeout = operation.StartToCloseTimeout.AsDuration()
		eventToken = operation.ScheduledEventToken
		event, err := node.LoadHistoryEvent(ctx, eventToken)
		if err != nil {
			return err
		}
		attrs := event.GetNexusOperationScheduledEventAttributes()
		args.payload = attrs.GetInput()
		args.header = attrs.GetNexusHeader()
		args.nexusLink = commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
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

// nolint:revive // (cognitive complexity) This function is long but the complexity is justified.
func (e taskExecutor) saveResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload], callErr error) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		operation, err := hsm.MachineData[Operation](node)
		if err != nil {
			return err
		}
		if callErr != nil {
			return e.handleStartOperationError(env, node, operation, callErr)
		}
		eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
		if err != nil {
			return err
		}

		var links []*commonpb.Link
		if result.Links != nil {
			for _, nexusLink := range result.Links {
				switch nexusLink.Type {
				case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
					link, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
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
		}

		if result.Pending != nil {
			// Handler has indicated that the operation will complete asynchronously. Mark the operation as started
			// to allow it to complete via callback.
			event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
				// nolint:revive // We must mutate here even if the linter doesn't like it.
				e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
					NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
						ScheduledEventId: eventID,
						OperationToken:   result.Pending.Token,
						// TODO(bergundy): Remove this fallback after the 1.27 release.
						OperationId: result.Pending.Token,
						RequestId:   operation.RequestId,
					},
				}
				// nolint:revive // We must mutate here even if the linter doesn't like it.
				e.Links = links
			})
			return hsm.MachineTransition(node, func(operation Operation) (hsm.TransitionOutput, error) {
				return TransitionStarted.Apply(operation, EventStarted{
					Time:       env.Now(),
					Node:       node,
					Attributes: event.GetNexusOperationStartedEventAttributes(),
				})
			})
		}
		// Operation completed synchronously. Store the result and update the state machine.
		return handleSuccessfulOperationResult(node, operation, result.Successful, links)
	})
}

func (e taskExecutor) handleStartOperationError(env hsm.Environment, node *hsm.Node, operation Operation, callErr error) error {
	var handlerErr *nexus.HandlerError
	var opErr *nexus.OperationError
	var opTimeoutBelowMinErr *operationTimeoutBelowMinError
	var serviceErr serviceerror.ServiceError

	switch {
	case errors.As(callErr, &serviceErr):
		if !common.IsRetryableRPCError(callErr) {
			return handleNonRetryableStartOperationError(node, operation, callErr)
		}
		// Fall through all uncaught errors to retryable
	case errors.As(callErr, &opErr):
		return handleOperationError(node, operation, opErr)
	case errors.As(callErr, &handlerErr) && !handlerErr.Retryable():
		// The StartOperation request got an unexpected response that is not retryable, fail the operation.
		// Although Failure is nullable, Nexus SDK is expected to always populate this field
		return handleNonRetryableStartOperationError(node, operation, handlerErr)
	case errors.Is(callErr, ErrResponseBodyTooLarge):
		// Following practices from workflow task completion payload size limit enforcement, we do not retry this
		// operation if the response body is too large.
		return handleNonRetryableStartOperationError(node, operation, callErr)
	case errors.Is(callErr, ErrInvalidOperationToken):
		// Following practices from workflow task completion payload size limit enforcement, we do not retry this
		// operation if the response's operation token is too large.
		return handleNonRetryableStartOperationError(node, operation, callErr)
	case errors.As(callErr, &opTimeoutBelowMinErr):
		// Not enough time to execute another request, resolve the operation with a timeout.
		return e.recordOperationTimeout(node, opTimeoutBelowMinErr.timeoutType)
	case errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled):
		// If timed out, we don't leak internal info to the user
		callErr = errRequestTimedOut
	default:
		// Fall through all uncaught errors to retryable
	}

	failure, err := callErrToFailure(callErr, true)
	if err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(operation Operation) (hsm.TransitionOutput, error) {
		return TransitionAttemptFailed.Apply(operation, EventAttemptFailed{
			Time:        env.Now(),
			Failure:     failure,
			Node:        node,
			RetryPolicy: e.Config.RetryPolicy(),
		})
	})
}

func handleNonRetryableStartOperationError(node *hsm.Node, operation Operation, callErr error) error {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return err
	}
	cause, err := callErrToFailure(callErr, false)
	if err != nil {
		return err
	}
	attrs := &historypb.NexusOperationFailedEventAttributes{
		Failure: createNexusOperationFailure(
			operation,
			eventID,
			cause,
		),
		ScheduledEventId: eventID,
		RequestId:        operation.RequestId,
	}
	event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
		// nolint:revive // We must mutate here even if the linter doesn't like it.
		e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
			NexusOperationFailedEventAttributes: attrs,
		}
	})

	return FailedEventDefinition{}.Apply(node.Parent, event)
}

func (e taskExecutor) executeBackoffTask(env hsm.Environment, node *hsm.Node, task BackoffTask) error {
	return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(op, EventRescheduled{
			Node: node,
		})
	})
}

func (e taskExecutor) executeScheduleToCloseTimeoutTask(env hsm.Environment, node *hsm.Node, task ScheduleToCloseTimeoutTask) error {
	return e.recordOperationTimeout(node, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
}

func (e taskExecutor) executeScheduleToStartTimeoutTask(env hsm.Environment, node *hsm.Node, task ScheduleToStartTimeoutTask) error {
	return e.recordOperationTimeout(node, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
}

func (e taskExecutor) executeStartToCloseTimeoutTask(env hsm.Environment, node *hsm.Node, task StartToCloseTimeoutTask) error {
	return e.recordOperationTimeout(node, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
}

func (e taskExecutor) recordOperationTimeout(node *hsm.Node, timeoutType enumspb.TimeoutType) error {
	return hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
		if err != nil {
			return hsm.TransitionOutput{}, err
		}
		node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, func(e *historypb.HistoryEvent) {
			// nolint:revive // We must mutate here even if the linter doesn't like it.
			e.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
					Failure: createNexusOperationFailure(
						op,
						eventID,
						&failurepb.Failure{
							Message: "operation timed out",
							FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
								TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
									TimeoutType: timeoutType,
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

	var endpoint *persistencespb.NexusEndpointEntry

	// Skip endpoint lookup for system-internal operations.
	if args.endpointName != commonnexus.SystemEndpoint {
		// This happens when we accept the ScheduleNexusOperation command when the endpoint is not found in the registry as
		// indicated by the EndpointNotFoundAlwaysNonRetryable dynamic config.
		// The config has been removed but we keep this check for backward compatibility.
		if args.endpointID == "" {
			handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return e.saveCancelationResult(ctx, env, ref, handlerError, args.scheduledEventID)
		}

		endpoint, err = e.lookupEndpoint(ctx, namespace.ID(ref.WorkflowKey.NamespaceID), args.endpointID, args.endpointName)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")

				// The endpoint is not registered, immediately fail the invocation.
				return e.saveCancelationResult(ctx, env, ref, handlerError, args.scheduledEventID)
			}
			return err
		}
	}

	callTimeout := e.Config.RequestTimeout(ns.Name().String(), task.EndpointName)
	var timeoutType enumspb.TimeoutType
	// Adjust timeout based on remaining operation timeouts.
	if args.startToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.startToCloseTimeout-time.Since(args.startedTime))
		timeoutType = enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	}
	if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-time.Since(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	// Set this value on the parent context so that our custom HTTP caller can mutate it since we cannot access response headers directly.
	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})

	var callErr error
	var startTime time.Time
	if callTimeout < e.Config.MinRequestTimeout(ns.Name().String()) {
		startTime = time.Now()
		callErr = &operationTimeoutBelowMinError{timeoutType: timeoutType}
	} else if args.endpointName == commonnexus.SystemEndpoint {
		startTime = time.Now()
		callErr = e.cancelOnHistoryService(callCtx, ns, args)
	} else {
		client, err := e.ClientProvider(
			callCtx,
			ref.WorkflowKey.NamespaceID,
			endpoint,
			args.service,
		)
		if err != nil {
			return fmt.Errorf("failed to get client: %w", err)
		}
		handle, err := client.NewOperationHandle(args.operation, args.token)
		if err != nil {
			return fmt.Errorf("failed to get handle for operation: %w", err)
		}

		if e.HTTPTraceProvider != nil {
			traceLogger := log.With(e.Logger,
				tag.Operation("CancelOperation"),
				tag.WorkflowNamespace(ns.Name().String()),
				tag.RequestID(args.requestID),
				tag.NexusOperation(args.operation),
				tag.Endpoint(args.endpointName),
				tag.WorkflowID(ref.WorkflowKey.WorkflowID),
				tag.WorkflowRunID(ref.WorkflowKey.RunID),
				tag.AttemptStart(time.Now().UTC()),
				tag.Attempt(task.Attempt),
			)
			if trace := e.HTTPTraceProvider.NewTrace(task.Attempt, traceLogger); trace != nil {
				callCtx = httptrace.WithClientTrace(callCtx, trace)
			}
		}

		startTime = time.Now()
		callErr = handle.Cancel(callCtx, nexus.CancelOperationOptions{Header: nexus.Header(args.headers)})
	}
	failureSource := failureSourceFromContext(callCtx)
	methodTag := metrics.NexusMethodTag("CancelOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(args.endpointName)
	}
	statusCodeTag := metrics.OutcomeTag(cancelCallOutcomeTag(callCtx, callErr))
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	chasmnexus.OutboundRequestCounter.With(e.MetricsHandler).Record(1, namespaceTag, destTag, methodTag, statusCodeTag, failureSourceTag)
	chasmnexus.OutboundRequestLatency.With(e.MetricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, statusCodeTag, failureSourceTag)

	if callErr != nil {
		if failureSource == commonnexus.FailureSourceWorker || errors.As(callErr, new(*operationTimeoutBelowMinError)) {
			e.Logger.Debug("Nexus CancelOperation request failed", tag.Error(callErr))
		} else {
			e.Logger.Error("Nexus CancelOperation request failed", tag.Error(callErr))
		}
	}

	err = e.saveCancelationResult(ctx, env, ref, callErr, args.scheduledEventID)

	if callErr != nil && isDestinationDown(callErr) {
		err = queueserrors.NewDestinationDownError(callErr.Error(), err)
	}

	return err
}

type cancelArgs struct {
	service, operation, token, endpointID, endpointName, requestID string
	scheduledTime                                                  time.Time
	startedTime                                                    time.Time
	startToCloseTimeout                                            time.Duration
	scheduleToCloseTimeout                                         time.Duration
	scheduledEventID                                               int64
	headers                                                        map[string]string
	payload                                                        *commonpb.Payload
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
		args.token = op.OperationToken
		args.endpointID = op.EndpointId
		args.endpointName = op.Endpoint
		args.requestID = op.RequestId
		args.scheduledTime = op.ScheduledTime.AsTime()
		args.startedTime = op.StartedTime.AsTime()
		args.scheduleToCloseTimeout = op.ScheduleToCloseTimeout.AsDuration()
		args.startToCloseTimeout = op.StartToCloseTimeout.AsDuration()
		args.scheduledEventID, err = hsm.EventIDFromToken(op.ScheduledEventToken)
		if err != nil {
			return err
		}
		// Load header from the scheduled event so we can propagate it to CancelOperation.
		event, err := n.Parent.LoadHistoryEvent(ctx, op.ScheduledEventToken)
		if err != nil {
			return err
		}
		if attrs := event.GetNexusOperationScheduledEventAttributes(); attrs != nil {
			args.headers = attrs.GetNexusHeader()
			args.payload = attrs.GetInput()
		}
		return nil
	})
	return
}

func (e taskExecutor) saveCancelationResult(ctx context.Context, env hsm.Environment, ref hsm.Ref, callErr error, scheduledEventID int64) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(n *hsm.Node) error {
		return hsm.MachineTransition(n, func(c Cancelation) (hsm.TransitionOutput, error) {
			if callErr != nil {
				var handlerErr *nexus.HandlerError
				var opTimeoutBelowMinErr *operationTimeoutBelowMinError
				isRetryable := !errors.As(callErr, &opTimeoutBelowMinErr) && (!errors.As(callErr, &handlerErr) || handlerErr.Retryable())
				failure, err := callErrToFailure(callErr, isRetryable)
				if err != nil {
					return hsm.TransitionOutput{}, err
				}
				if !isRetryable {
					if e.Config.RecordCancelRequestCompletionEvents() {
						n.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, func(e *historypb.HistoryEvent) {
							// nolint:revive // We must mutate here even if the linter doesn't like it.
							e.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestFailedEventAttributes{
								NexusOperationCancelRequestFailedEventAttributes: &historypb.NexusOperationCancelRequestFailedEventAttributes{
									ScheduledEventId: scheduledEventID,
									RequestedEventId: c.RequestedEventId,
									Failure:          failure,
								},
							}
							// nolint:revive // We must mutate here even if the linter doesn't like it.
							e.WorkerMayIgnore = true // For compatibility with older SDKs.
						})
					}
					return TransitionCancelationFailed.Apply(c, EventCancelationFailed{
						Time:    env.Now(),
						Failure: failure,
						Node:    n,
					})
				}
				return TransitionCancelationAttemptFailed.Apply(c, EventCancelationAttemptFailed{
					Time:        env.Now(),
					Failure:     failure,
					Node:        n,
					RetryPolicy: e.Config.RetryPolicy(),
				})
			}
			// Cancelation request transmitted successfully.
			// The operation is not yet canceled and may ignore our request, the outcome will be known via the
			// completion callback.
			if e.Config.RecordCancelRequestCompletionEvents() {
				n.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, func(e *historypb.HistoryEvent) {
					// nolint:revive // We must mutate here even if the linter doesn't like it.
					e.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
						NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{
							ScheduledEventId: scheduledEventID,
							RequestedEventId: c.RequestedEventId,
						},
					}
					// nolint:revive // We must mutate here even if the linter doesn't like it.
					e.WorkerMayIgnore = true // For compatibility with older SDKs.
				})
			}
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

func createNexusOperationFailure(operation Operation, scheduledEventID int64, cause *failurepb.Failure) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "nexus operation completed unsuccessfully",
		FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
			NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
				Endpoint:       operation.Endpoint,
				Service:        operation.Service,
				Operation:      operation.Operation,
				OperationToken: operation.OperationToken,
				// TODO(bergundy): This field is deprecated, remove it after the 1.27 release.
				OperationId:      operation.OperationToken,
				ScheduledEventId: scheduledEventID,
			},
		},
		Cause: cause,
	}
}

func startCallOutcomeTag(callCtx context.Context, result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload], callErr error) string {

	if callErr != nil {
		var opTimeoutBelowMinErr *operationTimeoutBelowMinError
		if errors.As(callErr, &opTimeoutBelowMinErr) {
			return "operation-timeout"
		}
		if errors.Is(callErr, ErrInvalidOperationToken) {
			return "invalid-operation-token"
		}
		if errors.Is(callErr, errOpProcessorFailed) {
			return "operation-processor-failed"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		var serviceErr serviceerror.ServiceError
		if errors.As(callErr, &serviceErr) {
			return "service-error:" + strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1)
		}
		var opFailedError *nexus.OperationError
		if errors.As(callErr, &opFailedError) {
			return "operation-unsuccessful:" + string(opFailedError.State)
		}
		var handlerError *nexus.HandlerError
		if errors.As(callErr, &handlerError) {
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
	if callErr != nil {
		if errors.Is(callErr, errOpProcessorFailed) {
			return "operation-processor-failed"
		}
		var opTimeoutBelowMinErr *operationTimeoutBelowMinError
		if errors.As(callErr, &opTimeoutBelowMinErr) {
			return "operation-timeout"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		var handlerErr *nexus.HandlerError
		if errors.As(callErr, &handlerErr) {
			return "handler-error:" + string(handlerErr.Type)
		}
		var serviceErr serviceerror.ServiceError
		if errors.As(callErr, &serviceErr) {
			return "service-error:" + strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1)
		}
		return "unknown-error"
	}
	return "successful"
}

func isDestinationDown(err error) bool {
	var serviceErr serviceerror.ServiceError
	// For the system endpoint, we don't even consider the destination down since it's internal.
	if errors.As(err, &serviceErr) {
		return false
	}
	var opFailedErr *nexus.OperationError
	if errors.As(err, &opFailedErr) {
		return false
	}
	var handlerError *nexus.HandlerError
	if errors.As(err, &handlerError) {
		return handlerError.Retryable()
	}
	if errors.Is(err, errOpProcessorFailed) {
		return false
	}
	if errors.Is(err, ErrResponseBodyTooLarge) {
		return false
	}
	if errors.Is(err, ErrInvalidOperationToken) {
		return false
	}
	var opTimeoutBelowMinErr *operationTimeoutBelowMinError
	return !errors.As(err, &opTimeoutBelowMinErr)
}

func callErrToFailure(callErr error, retryable bool) (*failurepb.Failure, error) {
	var serviceErr serviceerror.ServiceError
	if errors.As(callErr, &serviceErr) {
		return &failurepb.Failure{
			Message: fmt.Sprintf("%s: %s", strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1), serviceErr.Error()),
			FailureInfo: &failurepb.Failure_ServerFailureInfo{
				ServerFailureInfo: &failurepb.ServerFailureInfo{
					NonRetryable: !retryable,
				},
			},
		}, nil
	}
	var handlerErr *nexus.HandlerError
	if errors.As(callErr, &handlerErr) {
		var nf nexus.Failure
		if handlerErr.OriginalFailure != nil {
			nf = *handlerErr.OriginalFailure
		} else {
			var err error
			nf, err = nexusrpc.DefaultFailureConverter().ErrorToFailure(handlerErr)
			if err != nil {
				return nil, err
			}
		}
		f, err := commonnexus.NexusFailureToTemporalFailure(nf)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	return &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "CallError",
				NonRetryable: !retryable,
			},
		},
	}, nil
}

func failureSourceFromContext(ctx context.Context) string {
	ctxVal := ctx.Value(commonnexus.FailureSourceContextKey)
	if ctxVal == nil {
		return ""
	}
	val, ok := ctxVal.(*atomic.Value)
	if !ok {
		return ""
	}
	src := val.Load()
	if src == nil {
		return ""
	}
	source, ok := src.(string)
	if !ok {
		return ""
	}
	return source
}

func (e taskExecutor) startOnHistoryService(
	ctx context.Context,
	ns *namespace.Namespace,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	protoLinks := commonnexus.ConvertLinksToProto(options.Links)
	res, err := e.ChasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace: ns,
		RequestID: args.requestID,
		Links:     []nexus.Link{args.nexusLink},
		// Indicate that the input payload can be overwritten with defaults populated.
		ReserializeInputPayload: true,
	}, args.service, args.operation, args.payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}
	resp, err := e.HistoryClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(e.Config.NumHistoryShards),
		// NOTE: Header is not allowed for system operations.
		Request: &nexuspb.StartOperationRequest{
			Service:        args.service,
			Operation:      args.operation,
			Payload:        res.ReserializedInputPayload,
			RequestId:      args.requestID,
			Callback:       options.CallbackURL,
			CallbackHeader: options.CallbackHeader,
			Links:          protoLinks,
		},
	})
	if err != nil {
		return nil, err
	}

	// Convert the response back to the expected format
	result := &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{}

	switch v := resp.GetResponse().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		result.Links = commonnexus.ConvertLinksFromProto(v.SyncSuccess.GetLinks())
		result.Successful = v.SyncSuccess.Payload
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		result.Links = commonnexus.ConvertLinksFromProto(v.AsyncSuccess.GetLinks())
		result.Pending = &nexusrpc.OperationHandle[*commonpb.Payload]{
			Operation: args.operation,
			Token:     v.AsyncSuccess.GetOperationToken(),
			// Ignore the private client field, it's not needed here.
		}
	case *nexuspb.StartOperationResponse_Failure:
		state := nexus.OperationStateFailed
		if v.Failure.GetCanceledFailureInfo() != nil {
			state = nexus.OperationStateCanceled
		}
		nexusFailure, convErr := commonnexus.TemporalFailureToNexusFailure(v.Failure)
		if convErr != nil {
			e.Logger.Error("failed to convert temporal failure to nexus failure", tag.Error(convErr), tag.RequestID(args.requestID))
			he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error (request ID: %s)", args.requestID)
			he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
			return nil, he
		}
		return nil, &nexus.OperationError{
			State:           state,
			Cause:           &nexus.FailureError{Failure: nexusFailure},
			OriginalFailure: &nexusFailure,
		}
	default:
		e.Logger.Error(fmt.Sprintf("unexpected response variant type: %T", v), tag.RequestID(args.requestID))
		he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error (request ID: %s)", args.requestID)
		he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
		return nil, he
	}

	return result, nil
}

func (e taskExecutor) cancelOnHistoryService(
	ctx context.Context,
	ns *namespace.Namespace,
	args cancelArgs,
) error {
	res, err := e.ChasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace: ns,
		RequestID: args.requestID,
		// Links are not needed for cancelation.
	}, args.service, args.operation, args.payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}

	_, err = e.HistoryClient.CancelNexusOperation(ctx, &historyservice.CancelNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(e.Config.NumHistoryShards),
		Request: &nexuspb.CancelOperationRequest{
			Service:        args.service,
			Operation:      args.operation,
			OperationToken: args.token,
		},
	})
	return err
}

func (e taskExecutor) startViaHTTP(
	ctx context.Context,
	client *nexusrpc.HTTPClient,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	rawResult, callErr := client.StartOperation(ctx, args.operation, args.payload, options)

	var result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	if callErr == nil {
		if rawResult.Pending != nil {
			result = &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{
				Pending: &nexusrpc.OperationHandle[*commonpb.Payload]{
					Operation: rawResult.Pending.Operation,
					Token:     rawResult.Pending.Token,
				},
				Links: rawResult.Links,
			}
		} else {
			var payload *commonpb.Payload
			err := rawResult.Successful.Consume(&payload)
			if err != nil {
				callErr = err
			} else {
				result = &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{
					Successful: payload,
					Links:      rawResult.Links,
				}
			}
		}
	}
	return result, callErr
}
