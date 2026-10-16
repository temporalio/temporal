package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
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
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := i.capturePanicHandler(ctx, req, info, handler)

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

	return resp, st.Err()
}

func (i *ServiceErrorInterceptor) capturePanicHandler(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (_ any, retError error) {
	logger := i.logger
	if info != nil {
		logger = log.With(logger, logtags.NexusStage(info.FullMethod)...)
	}
	defer metrics.CapturePanic(logger, i.metricsHandler, &retError)
	return handler(ctx, req)
}
