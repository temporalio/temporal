// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

// Generic Nexus context that is not bound to a specific operation.
// Includes fields extracted from an incoming Nexus request before being handled by the Nexus HTTP handler.
type nexusContext struct {
	requestStartTime time.Time
	apiName          string
	namespaceName    string
	taskQueue        string
	claims           *authorization.Claims
}

// Context for a specific Nexus operation, includes a resolved namespace, and a bound metrics handler and logger.
type operationContext struct {
	nexusContext
	namespace      *namespace.Namespace
	metricsHandler metrics.Handler
	logger         log.Logger
	auth           *authorization.Policy
}

// Panic handler and metrics recording function.
// Used as a deferred statement in Nexus handler methods.
func (c *operationContext) capturePanicAndRecordMetrics(errPtr *error) {
	recovered := recover() //nolint:revive
	if recovered != nil {
		err, ok := recovered.(error)
		if !ok {
			err = fmt.Errorf("panic: %v", recovered) //nolint:goerr113
		}

		st := string(debug.Stack())

		c.logger.Error("Panic captured", tag.SysStackTrace(st), tag.Error(err))
		*errPtr = err
	}

	c.metricsHandler.Counter(metrics.NexusRequests.GetMetricName()).Record(1)
	c.metricsHandler.Histogram(metrics.NexusLatencyHistogram.GetMetricName(), metrics.Milliseconds).Record(time.Since(c.requestStartTime).Milliseconds())
}

func (c *operationContext) interceptRequest(ctx context.Context, req *nexuspb.Request) (*matchingservice.DispatchNexusTaskRequest, error) {
	request := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: c.namespace.ID().String(),
		TaskQueue:   &taskqueue.TaskQueue{Name: c.taskQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL},
		Request:     req,
	}

	err := c.auth.Authorize(ctx, c.claims, &authorization.CallTarget{
		APIName:   c.apiName,
		Namespace: c.namespace.Name().String(),
		Request:   request,
	})
	if err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.NexusOutcomeTag("unauthorized"))
		return nil, adaptAuthorizeError(err)
	}
	// TODO: Redirect if current cluster is passive for this namespace.
	return request, nil
}

// Key to extract a nexusContext object from a context.Context.
type nexusContextKey struct{}

// A Nexus Handler implementation.
// Dispatches Nexus requests as Nexus tasks to workers via matching.
type nexusHandler struct {
	nexus.UnimplementedHandler
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
	matchingClient    matchingservice.MatchingServiceClient
	auth              *authorization.Interceptor
}

// Extracts a nexusContext from the given ctx and returns an operationContext with tagged metrics and logging.
// Resolves the context's namespace name to a registered Namespace.
func (h *nexusHandler) getOperationContext(ctx context.Context, method string) (*operationContext, error) {
	nc, ok := ctx.Value(nexusContextKey{}).(nexusContext)
	if !ok {
		return nil, errors.New("no nexus context set on context") //nolint:goerr113
	}
	oc := operationContext{nexusContext: nc, auth: h.auth}
	oc.metricsHandler = h.metricsHandler.WithTags(
		metrics.NamespaceTag(nc.namespaceName),
		metrics.NexusMethodTag(method),
		// default to internal error unless overridden by handler
		metrics.NexusOutcomeTag("internal_error"),
	)
	var err error
	if oc.namespace, err = h.namespaceRegistry.GetNamespace(namespace.Name(nc.namespaceName)); err != nil {
		oc.metricsHandler.Counter(metrics.NexusRequests.GetMetricName()).Record(
			1,
			metrics.NexusOutcomeTag("namespace_not_found"),
		)

		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace not found: %q", nc.namespaceName)
	}
	oc.logger = log.With(h.logger, tag.Operation(method), tag.WorkflowNamespace(nc.namespaceName))
	return &oc, nil
}

// StartOperation implements the nexus.Handler interface.
func (h *nexusHandler) StartOperation(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (result nexus.HandlerStartOperationResult[any], retErr error) {
	oc, err := h.getOperationContext(ctx, "StartOperation")
	if err != nil {
		return nil, err
	}
	defer oc.capturePanicAndRecordMetrics(&retErr)

	startOperationRequest := nexuspb.StartOperationRequest{
		Operation: operation,
		Callback:  options.CallbackURL,
		RequestId: options.RequestID,
	}
	request, err := oc.interceptRequest(ctx, &nexuspb.Request{
		Header: options.Header,
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &startOperationRequest,
		},
	})
	if err != nil {
		return nil, err
	}

	// Transform nexus Content to temporal Payload with common/nexus PayloadSerializer.
	if err = input.Consume(&startOperationRequest.Payload); err != nil {
		oc.logger.Warn("invalid input", tag.Error(err))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
	}
	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		// TODO: check for deadline error and convert to downstream timeout handler error.
		return nil, err
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("handler_error"))
		return nil, &nexus.HandlerError{
			Type:    convertNexusHandlerError(nexus.HandlerErrorType(t.HandlerError.GetErrorType())),
			Failure: commonnexus.ProtoFailureToNexusFailure(t.HandlerError.GetFailure()),
		}
	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t := t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("sync_success"))
			return &nexus.HandlerStartOperationResultSync[any]{
				Value: t.SyncSuccess.GetPayload(),
			}, nil
		case *nexuspb.StartOperationResponse_AsyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("async_success"))
			return &nexus.HandlerStartOperationResultAsync{
				OperationID: t.AsyncSuccess.GetOperationId(),
			}, nil
		case *nexuspb.StartOperationResponse_OperationError:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("operation_error"))
			return nil, &nexus.UnsuccessfulOperationError{
				State:   nexus.OperationState(t.OperationError.GetOperationState()),
				Failure: *commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
			}
		}
	}
	return nil, errors.New("unhandled response outcome") //nolint:goerr113
}
func (h *nexusHandler) CancelOperation(ctx context.Context, operation, id string, options nexus.CancelOperationOptions) (retErr error) {
	oc, err := h.getOperationContext(ctx, "CancelOperation")
	if err != nil {
		return err
	}
	defer oc.capturePanicAndRecordMetrics(&retErr)

	request, err := oc.interceptRequest(ctx, &nexuspb.Request{
		Header: options.Header,
		Variant: &nexuspb.Request_CancelOperation{
			CancelOperation: &nexuspb.CancelOperationRequest{
				Operation:   operation,
				OperationId: id,
			},
		},
	})
	if err != nil {
		return err
	}

	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		// TODO: check for deadline error and convert to downstream timeout handler error.
		return err
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("handler_error"))
		return &nexus.HandlerError{
			Type:    convertNexusHandlerError(nexus.HandlerErrorType(t.HandlerError.GetErrorType())),
			Failure: commonnexus.ProtoFailureToNexusFailure(t.HandlerError.GetFailure()),
		}
	case *matchingservice.DispatchNexusTaskResponse_Response:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.NexusOutcomeTag("success"))
		return nil
	}
	return errors.New("unhandled response outcome") //nolint:goerr113
}

// convertNexusHandlerError converts any 5xx user handler error to a downsream error.
func convertNexusHandlerError(t nexus.HandlerErrorType) nexus.HandlerErrorType {
	switch t {
	case nexus.HandlerErrorTypeDownstreamTimeout,
		nexus.HandlerErrorTypeUnauthenticated,
		nexus.HandlerErrorTypeForbidden,
		nexus.HandlerErrorTypeBadRequest,
		nexus.HandlerErrorTypeNotFound,
		nexus.HandlerErrorTypeNotImplemented:
		return t
	}
	return nexus.HandlerErrorTypeDownstreamError
}

func adaptAuthorizeError(err error) error {
	// Authorize err is either an explicitly set reason, or a generic "Request unauthorized." message.
	var permissionDeniedError *serviceerror.PermissionDenied
	if errors.As(err, &permissionDeniedError) && permissionDeniedError.Reason != "" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeForbidden, "permission denied: %s", permissionDeniedError.Reason)
	}
	return nexus.HandlerErrorf(nexus.HandlerErrorTypeForbidden, "permission denied")
}
