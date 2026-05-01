package interceptor

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

	_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid argument")
		})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = ServiceErrorInterceptor(t.Context(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			errWithoutStatus := &ErrorWithoutStatus{
				Message: "unknown error without status",
			}
			return nil, errWithoutStatus
		})

	require.Error(t, err)
	require.Equal(t, codes.Unknown, status.Code(err))
}

func TestServiceErrorInterceptorSer(t *testing.T) {
	serErrors := []error{
		serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
		serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
	}
	for _, inErr := range serErrors {
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, inErr
			})
		require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
	}
}

func TestServiceErrorInterceptorTruncation(t *testing.T) {
	t.Run("nil error is not affected", func(t *testing.T) {
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return "ok", nil
			})
		require.NoError(t, err)
	})

	t.Run("short message is not truncated", func(t *testing.T) {
		msg := "short error"
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.Equal(t, msg, st.Message())
	})

	t.Run("message at exact limit is not truncated", func(t *testing.T) {
		msg := strings.Repeat("a", maxMessageLength)
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.Equal(t, msg, st.Message())
	})

	t.Run("message over limit is truncated", func(t *testing.T) {
		msg := strings.Repeat("a", maxMessageLength+100)
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.LessOrEqual(t, len(st.Message()), maxMessageLength)
		require.True(t, strings.HasSuffix(st.Message(), truncatedSuffix))
	})

	t.Run("truncation preserves error code", func(t *testing.T) {
		msg := strings.Repeat("x", maxMessageLength+500)
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewNotFound(msg)
			})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("truncation respects multi-byte UTF-8 boundary", func(t *testing.T) {
		// Fill up to near the limit with multi-byte characters (3 bytes each for '€')
		// then push over the limit so truncation must split within the repeated chars.
		euroCount := maxMessageLength / len("€") // each '€' is 3 bytes
		msg := strings.Repeat("€", euroCount+100)
		_, err := ServiceErrorInterceptor(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.LessOrEqual(t, len(st.Message()), maxMessageLength)
		require.True(t, strings.HasSuffix(st.Message(), truncatedSuffix))
		// Verify the truncated body (without suffix) is valid UTF-8 by checking
		// that no partial rune was left behind — the full message should be valid.
		require.True(t, strings.HasSuffix(st.Message(), truncatedSuffix))
		body := strings.TrimSuffix(st.Message(), truncatedSuffix)
		// Every character in body should be '€' (no partial runes).
		for _, r := range body {
			require.Equal(t, '€', r)
		}
	})
}
