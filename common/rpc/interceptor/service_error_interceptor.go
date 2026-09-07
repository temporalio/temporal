package interceptor

import (
	"context"
	"errors"

	nexusrpc "github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
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
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	resp, err := i.capturePanicHandlerNexus(ctx, in, next)
	if ie, ok := errors.AsType[*nexus.InterceptorError](err); ok {
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
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (_ any, retError error) {
	defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)
	return next(ctx, in)
}

// transformNexusError only normalizes gRPC-shaped errors. Nexus-native errors
// are returned as-is to preserve existing mappings.
func (i *ServiceErrorInterceptor) transformNexusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*nexusrpc.HandlerError](err); ok {
		return err
	}
	if _, ok := errors.AsType[*nexusrpc.OperationError](err); ok {
		return err
	}
	return i.transformError(err)
}
