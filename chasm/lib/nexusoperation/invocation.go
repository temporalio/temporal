package nexusoperation

import (
	"context"
	"fmt"
	"net/http/httptrace"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
)

// startArgs holds the arguments needed to start a Nexus operation invocation.
type startArgs struct {
	service                string
	operation              string
	requestID              string
	endpointName           string
	endpointID             string
	currentTime            time.Time
	scheduledTime          time.Time
	scheduleToStartTimeout time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	header                 map[string]string
	payload                *commonpb.Payload
	nexusLinks             []nexus.Link
	serializedRef          []byte
}

// invocationTraceContext captures per-call contextual information needed to set up HTTP tracing.
type invocationTraceContext struct {
	operationTag  string // "StartOperation" or "CancelOperation"
	namespaceName string
	requestID     string
	operation     string
	endpointName  string
	workflowID    string
	runID         string
	attemptStart  time.Time
	attempt       int32
}

type invocation interface {
	Start(
		ctx context.Context,
		args startArgs,
		options nexus.StartOperationOptions,
	) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error)
	Cancel(
		ctx context.Context,
		args cancelArgs,
		options nexus.CancelOperationOptions,
	) error
}

type invocationTimeout struct {
	timeoutType enumspb.TimeoutType
}

func (i *invocationTimeout) Start(
	_ context.Context,
	_ startArgs,
	_ nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	return nil, &operationTimeoutBelowMinError{timeoutType: i.timeoutType}
}

func (i *invocationTimeout) Cancel(
	_ context.Context,
	_ cancelArgs,
	_ nexus.CancelOperationOptions,
) error {
	return &operationTimeoutBelowMinError{timeoutType: i.timeoutType}
}

type invocationHTTP struct {
	client      *nexusrpc.HTTPClient
	clientTrace *httptrace.ClientTrace
}

func (b *nexusTaskHandlerBase) newInvocationHTTP(
	ctx context.Context,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	service string,
	traceCtx invocationTraceContext,
) (*invocationHTTP, error) {
	client, err := b.clientProvider(ctx, ns.ID().String(), endpoint, service)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to get a client: %v", err)
	}
	var clientTrace *httptrace.ClientTrace
	if b.httpTraceProvider != nil {
		traceLogger := log.With(b.logger,
			tag.Operation(traceCtx.operationTag),
			tag.WorkflowNamespace(traceCtx.namespaceName),
			tag.RequestID(traceCtx.requestID),
			tag.NexusOperation(traceCtx.operation),
			tag.Endpoint(traceCtx.endpointName),
			tag.WorkflowID(traceCtx.workflowID),
			tag.WorkflowRunID(traceCtx.runID),
			tag.AttemptStart(traceCtx.attemptStart),
			tag.Attempt(traceCtx.attempt),
		)
		clientTrace = b.httpTraceProvider.NewTrace(traceCtx.attempt, traceLogger)
	}
	return &invocationHTTP{client: client, clientTrace: clientTrace}, nil
}

func (i *invocationHTTP) Start(
	ctx context.Context,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	if i.clientTrace != nil {
		ctx = httptrace.WithClientTrace(ctx, i.clientTrace)
	}
	rawResult, callErr := i.client.StartOperation(ctx, args.operation, args.payload, options)

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

func (i *invocationHTTP) Cancel(
	ctx context.Context,
	args cancelArgs,
	options nexus.CancelOperationOptions,
) error {
	if i.clientTrace != nil {
		ctx = httptrace.WithClientTrace(ctx, i.clientTrace)
	}
	handle, err := i.client.NewOperationHandle(args.operation, args.token)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to get handle for operation: %v", err)
	}
	return handle.Cancel(ctx, options)
}

type invocationSystem struct {
	ns            *namespace.Namespace
	chasmRegistry *chasm.Registry
	historyClient resource.HistoryClient
	config        *Config
	logger        log.Logger
}

func (b *nexusTaskHandlerBase) newInvocationSystem(
	ns *namespace.Namespace,
) *invocationSystem {
	return &invocationSystem{
		ns:            ns,
		chasmRegistry: b.chasmRegistry,
		historyClient: b.historyClient,
		config:        b.config,
		logger:        b.logger,
	}
}

func (i *invocationSystem) Start(
	ctx context.Context,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	protoLinks := commonnexus.ConvertLinksToProto(options.Links)
	res, err := i.chasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace:               i.ns,
		RequestID:               args.requestID,
		Links:                   args.nexusLinks,
		ReserializeInputPayload: true,
	}, args.service, args.operation, args.payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}
	resp, err := i.historyClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: i.ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(i.config.NumHistoryShards),
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
		}
	case *nexuspb.StartOperationResponse_Failure:
		state := nexus.OperationStateFailed
		if v.Failure.GetCanceledFailureInfo() != nil {
			state = nexus.OperationStateCanceled
		}
		nexusFailure, convErr := commonnexus.TemporalFailureToNexusFailure(v.Failure)
		if convErr != nil {
			i.logger.Error("failed to convert temporal failure to nexus failure", tag.Error(convErr), tag.RequestID(args.requestID))
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
		i.logger.Error(fmt.Sprintf("unexpected response variant type: %T", v), tag.RequestID(args.requestID))
		he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error (request ID: %s)", args.requestID)
		he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
		return nil, he
	}

	return result, nil
}

func (i *invocationSystem) Cancel(
	ctx context.Context,
	args cancelArgs,
	_ nexus.CancelOperationOptions,
) error {
	res, err := i.chasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace: i.ns,
		RequestID: args.requestID,
		// Links are not needed for cancellation.
	}, args.service, args.operation, args.payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}

	_, err = i.historyClient.CancelNexusOperation(ctx, &historyservice.CancelNexusOperationRequest{
		NamespaceId: i.ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(i.config.NumHistoryShards),
		Request: &nexuspb.CancelOperationRequest{
			Service:        args.service,
			Operation:      args.operation,
			OperationToken: args.token,
		},
	})
	return err
}
