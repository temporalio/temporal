package interceptor

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const truncatedSuffix = "... <truncated>"

type ServiceErrorInterceptor struct {
	maxMessageLength dynamicconfig.IntPropertyFn

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewServiceErrorInterceptor(
	maxMessageLength dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *ServiceErrorInterceptor {
	return &ServiceErrorInterceptor{
		maxMessageLength: maxMessageLength,

		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

func (i *ServiceErrorInterceptor) Intercept(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := i.capturePanicHandler(ctx, req, handler)

	return resp, i.transformError(err)
}

func (i *ServiceErrorInterceptor) InterceptNexus(
	ctx context.Context,
	in interceptornexus.InterceptorInput,
	next interceptornexus.HandlerFunc,
) (any, error) {
	resp, err := i.capturePanicHandlerNexus(ctx, in, next)
	if ie, ok := errors.AsType[*interceptornexus.InterceptorError](err); ok {
		ie.Err = i.transformNexusError(ie.Err)
		return resp, ie
	}
	return resp, i.transformNexusError(err)
}

func (i *ServiceErrorInterceptor) transformError(err error) error {
	if err == nil {
		return nil
	}
	var deserializationError *serialization.DeserializationError
	var serializationError *serialization.SerializationError
	// convert serialization errors to be captured as serviceerrors across gRPC calls
	if errors.As(err, &deserializationError) || errors.As(err, &serializationError) {
		err = serviceerror.NewDataLoss(err.Error())
	}

	// truncate message length if needed
	maxLength := i.maxMessageLength()
	st := serviceerror.ToStatus(err)
	if len(st.Message()) > maxLength {
		p := st.Proto()
		p.Message = util.TruncateUTF8(p.Message, maxLength-len(truncatedSuffix)) + truncatedSuffix
		st = status.FromProto(p)
	}
	return st.Err()
}

func (i *ServiceErrorInterceptor) capturePanicHandler(
	ctx context.Context,
	req any,
	handler grpc.UnaryHandler,
) (_ any, retError error) {
	defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)
	return handler(ctx, req)
}

func (i *ServiceErrorInterceptor) capturePanicHandlerNexus(
	ctx context.Context,
	in interceptornexus.InterceptorInput,
	next interceptornexus.HandlerFunc,
) (_ any, retError error) {
	logTags := []tag.Tag{
		tag.Operation(in.MethodName()),
		tag.WorkflowNamespace(in.NamespaceName()),
	}
	if endpointName := in.EndpointName(); endpointName != "" {
		logTags = append(logTags, tag.Endpoint(endpointName))
	}
	if operationName := in.OperationName(); operationName != "" {
		logTags = append(logTags, tag.NexusOperation(operationName))
	}
	switch input := in.(type) {
	case interceptornexus.StartOpInput:
		logTags = append(logTags, tag.NexusStageHandlerInbound, tag.RequestID(input.StartOperationOptions.RequestID))
	case interceptornexus.CancelOpInput:
		logTags = append(logTags, tag.NexusStageHandlerInbound)
	case interceptornexus.CompleteOpInput:
		logTags = append(logTags, tag.NexusStageCallerInbound)
		if input.Completion != nil && input.Completion.GetRequestId() != "" {
			logTags = append(logTags, tag.RequestID(input.Completion.GetRequestId()))
		}
	default:
	}
	defer metrics.CapturePanic(log.With(i.logger, logTags...), i.metricsHandler, &retError)
	return next(ctx, in)
}

// transformNexusError only normalizes gRPC-shaped errors. Nexus-native errors
// are returned as-is to preserve existing mappings.
func (i *ServiceErrorInterceptor) transformNexusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return err
	}
	if _, ok := errors.AsType[*nexus.OperationError](err); ok {
		return err
	}
	return i.transformError(err)
}
