//go:build test_dep

package interceptor

import (
	"context"
	"reflect"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/grpc"
)

const nilResponseMessage = "gRPC handler returned nil response without error"

// NewNonNilResponseInterceptor returns a test-only check for successful nil responses.
func NewNonNilResponseInterceptor(
	logger log.Logger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		softassert.That(logger, err != nil || !isNil(resp), nilResponseMessage, tag.Operation(info.FullMethod))
		return resp, err
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}

	reflectedValue := reflect.ValueOf(value)
	return reflectedValue.Kind() == reflect.Pointer && reflectedValue.IsNil()
}
