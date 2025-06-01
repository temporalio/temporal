package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/serialization"
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

// Error returns string message.
func TestServiceErrorInterceptorUnknown(t *testing.T) {

	_, err := ServiceErrorInterceptor(context.Background(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid argument")
		})

	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = ServiceErrorInterceptor(context.Background(), nil, nil,
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
	serErrors := []error{
		serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
		serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
	}
	for _, inErr := range serErrors {
		_, err := ServiceErrorInterceptor(context.Background(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, inErr
			})
		assert.Equal(t, serviceerror.ToStatus(err).Code(), codes.DataLoss)
	}
}
