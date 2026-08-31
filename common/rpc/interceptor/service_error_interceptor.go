package interceptor

import (
	"context"
	"errors"

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

// InterceptNexus is a no-op: unlike the gRPC path, every error reaching the Nexus
// chain is already converted to a *nexus.HandlerError/*nexus.OperationError at its
// origin (see commonnexus.ConvertGRPCError call sites in nexus_handler.go and the
// other Nexus interceptors), so there's nothing left for transformError to do. This
// method exists only so ServiceErrorInterceptor keeps its chain position for parity
// with the gRPC ordering.
func (i *ServiceErrorInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return next(ctx, in)
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
