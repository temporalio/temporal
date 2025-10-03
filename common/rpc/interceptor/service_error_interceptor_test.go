package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UnaryHandler func(ctx context.Context, req any) (any, error)

type (
	// Unimplemented represents unimplemented error.
	ErrorWithoutStatus struct {
		Message string
	}
)

func (e *ErrorWithoutStatus) Error() string {
	return e.Message
}

func createTestServiceErrorInterceptor() *ServiceErrorInterceptor {
	logger := log.NewNoopLogger()
	requestErrorHandler := NewRequestErrorHandler(logger, dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
	metricsHandler := metrics.NoopMetricsHandler
	namespaceRegistry := namespace.NewMockRegistry(nil)
	return NewServiceErrorInterceptor(requestErrorHandler, metricsHandler, namespaceRegistry)
}

// Error returns string message.
func TestServiceErrorInterceptorUnknown(t *testing.T) {
	interceptor := createTestServiceErrorInterceptor()

	_, err := interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid argument")
		})

	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			errWithoutStatus := &ErrorWithoutStatus{
				Message: "unknown error without status",
			}
			return nil, errWithoutStatus
		})

	assert.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
}

func TestServiceErrorInterceptorSer(t *testing.T) {
	interceptor := createTestServiceErrorInterceptor()
	serErrors := []error{
		serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
		serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
	}
	for _, inErr := range serErrors {
		_, err := interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{},
			func(_ context.Context, _ any) (any, error) {
				return nil, inErr
			})
		assert.Equal(t, serviceerror.ToStatus(err).Code(), codes.DataLoss)
	}
}
