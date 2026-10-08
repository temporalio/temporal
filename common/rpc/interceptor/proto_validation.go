//go:generate go run ../../../cmd/tools/genrpcvalidationdispatch -out .

package interceptor

import (
	"context"
	"errors"
	"reflect"

	"buf.build/go/protovalidate"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ProtoValidationInterceptor struct {
	validator      protovalidate.Validator
	metricsHandler metrics.Handler
	responses      map[string]protoValidationResponse
}

var _ grpc.UnaryServerInterceptor = (*ProtoValidationInterceptor)(nil).Intercept

func NewProtoValidationInterceptor(logger log.Logger, metricsHandler metrics.Handler) (*ProtoValidationInterceptor, error) {
	validator, err := newProtoValidator(protoValidationMessages()...)
	if err != nil {
		return nil, err
	}
	responses := make(map[string]protoValidationResponse)
	for method, message := range protoValidationResponses() {
		responses[method] = protoValidationResponse{
			messageType: reflect.TypeOf(message),
			logger:      log.NewThrottledLogger(log.With(logger, tag.Operation(method)), func() float64 { return 1 }),
		}
	}
	return &ProtoValidationInterceptor{validator: validator, metricsHandler: metricsHandler, responses: responses}, nil
}

func (i *ProtoValidationInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if message, ok := req.(proto.Message); ok {
		if err := i.validate(message); err != nil {
			return nil, protoValidationRequestError(err)
		}
	}
	response, err := handler(ctx, req)
	if err == nil {
		i.reportResponse(info.FullMethod, response)
	}
	return response, err
}

func protoValidationRequestError(err error) error {
	failure, ok := errors.AsType[*protovalidate.ValidationError](err)
	if !ok {
		return serviceerror.NewInternal("protobuf request validation failed")
	}
	details := new(errdetails.BadRequest)
	for _, item := range failure.Violations {
		details.FieldViolations = append(details.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field: protovalidate.FieldPathString(item.Proto.GetField()), Reason: item.Proto.GetRuleId(), Description: item.Proto.GetMessage(),
		})
	}
	st, encodeErr := status.New(codes.InvalidArgument, err.Error()).WithDetails(details)
	if encodeErr != nil {
		return serviceerror.NewInternal("encode protobuf validation details failed")
	}
	return serviceerror.FromStatus(st)
}
