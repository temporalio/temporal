//go:generate go run ../../../cmd/tools/genrpcvalidationdispatch -out .

package interceptor

import (
	"context"
	"errors"

	"buf.build/go/protovalidate"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ProtoValidationInterceptor struct {
	logger    log.Logger
	validator protovalidate.Validator
}

var _ grpc.UnaryServerInterceptor = (*ProtoValidationInterceptor)(nil).Intercept

func NewProtoValidationInterceptor(logger log.Logger) (*ProtoValidationInterceptor, error) {
	validator, err := protovalidate.New(protovalidate.WithFailFast())
	if err != nil {
		return nil, err
	}
	return &ProtoValidationInterceptor{logger: logger, validator: validator}, nil
}

func (i *ProtoValidationInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if message, ok := req.(proto.Message); ok {
		if err := i.validate(message); err != nil {
			if _, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
				return nil, serviceerror.NewInvalidArgument(err.Error())
			}
			return nil, serviceerror.NewInternal(err.Error())
		}
	}
	response, err := handler(ctx, req)
	if err == nil {
		if message, ok := response.(proto.Message); ok {
			if err := i.validate(message); err != nil {
				softassert.Fail(i.logger, "RPC response failed protobuf validation", tag.Operation(info.FullMethod), tag.Error(err))
			}
		}
	}
	return response, err
}
