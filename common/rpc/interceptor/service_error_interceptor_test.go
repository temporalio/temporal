package interceptor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testMaxMessageLength = 4000

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
	ctrl := gomock.NewController(t)
	interceptor := NewServiceErrorInterceptor(
		dynamicconfig.GetIntPropertyFn(testMaxMessageLength),
		metrics.NewMockHandler(ctrl),
		log.NewTestLogger(),
	)

	_, err := interceptor.Intercept(t.Context(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid argument")
		})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = interceptor.Intercept(t.Context(), nil, nil,
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
	ctrl := gomock.NewController(t)
	interceptor := NewServiceErrorInterceptor(
		dynamicconfig.GetIntPropertyFn(testMaxMessageLength),
		metrics.NewMockHandler(ctrl),
		log.NewTestLogger(),
	)
	serErrors := []error{
		serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
		serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, nil),
	}
	for _, inErr := range serErrors {
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, inErr
			})
		require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
	}
}

func TestServiceErrorInterceptorTruncation(t *testing.T) {
	ctrl := gomock.NewController(t)
	interceptor := NewServiceErrorInterceptor(
		dynamicconfig.GetIntPropertyFn(testMaxMessageLength),
		metrics.NewMockHandler(ctrl),
		log.NewTestLogger(),
	)

	t.Run("nil error is not affected", func(t *testing.T) {
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return "ok", nil
			})
		require.NoError(t, err)
	})

	t.Run("short message is not truncated", func(t *testing.T) {
		msg := "short error"
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.Equal(t, msg, st.Message())
	})

	t.Run("message at exact limit is not truncated", func(t *testing.T) {
		msg := strings.Repeat("a", testMaxMessageLength)
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.Equal(t, msg, st.Message())
	})

	t.Run("message over limit is truncated", func(t *testing.T) {
		msg := strings.Repeat("a", testMaxMessageLength+100)
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.LessOrEqual(t, len(st.Message()), testMaxMessageLength)
		require.True(t, strings.HasSuffix(st.Message(), truncatedSuffix))
	})

	t.Run("truncation preserves error code", func(t *testing.T) {
		msg := strings.Repeat("x", testMaxMessageLength+500)
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewNotFound(msg)
			})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("truncation respects multi-byte UTF-8 boundary", func(t *testing.T) {
		// Fill up to near the limit with multi-byte characters (3 bytes each for '€')
		// then push over the limit so truncation must split within the repeated chars.
		euroCount := testMaxMessageLength / len("€") // each '€' is 3 bytes
		msg := strings.Repeat("€", euroCount+100)
		_, err := interceptor.Intercept(t.Context(), nil, nil,
			func(_ context.Context, _ any) (any, error) {
				return nil, serviceerror.NewInternal(msg)
			})
		require.Error(t, err)
		st := status.Convert(err)
		require.LessOrEqual(t, len(st.Message()), testMaxMessageLength)
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

func TestServiceErrorInterceptorPanic(t *testing.T) {
	testCases := []struct {
		name       string
		panicObj   any
		errMessage string
	}{
		{
			name:       "panic with error is converted to internal error",
			panicObj:   errors.New("panic error message"),
			errMessage: "panic error message",
		},
		{
			name:       "panic with non-error value is converted to internal error",
			panicObj:   "something went wrong",
			errMessage: "panic: something went wrong",
		},
		{
			name:       "panic with service error is still converted to internal error",
			panicObj:   serviceerror.NewNotFound("not found message"),
			errMessage: "not found message",
		},
		{
			name:       "captured panic message is truncated",
			panicObj:   errors.New(strings.Repeat("a", testMaxMessageLength+100)),
			errMessage: strings.Repeat("a", testMaxMessageLength-len(truncatedSuffix)) + truncatedSuffix,
		},
		{
			name:       "no panic does not log",
			panicObj:   nil,
			errMessage: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metricsHandlerMock := metrics.NewMockHandler(ctrl)
			loggerMock := log.NewMockLogger(ctrl)
			interceptor := NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(testMaxMessageLength),
				metricsHandlerMock,
				loggerMock,
			)

			var loggedTags []tag.Tag
			if tc.panicObj != nil {
				counterMock := metrics.NewMockCounterIface(ctrl)
				counterMock.EXPECT().Record(int64(1))
				metricsHandlerMock.EXPECT().Counter(metrics.ServicePanic.Name()).Return(counterMock)
				loggerMock.EXPECT().Error("Panic is captured", gomock.Any(), gomock.Any()).
					Do(func(_ string, tags ...tag.Tag) {
						loggedTags = tags
					}).
					Times(1)
			}

			resp, err := interceptor.Intercept(t.Context(), nil, nil,
				func(_ context.Context, _ any) (any, error) {
					if tc.panicObj != nil {
						panic(tc.panicObj)
					}
					return "ok", nil
				})

			if tc.panicObj != nil {
				expectedMessage := fmt.Sprintf(
					"rpc error: code = Internal desc = %s",
					tc.errMessage,
				)
				require.Nil(t, resp)
				require.Error(t, err)
				require.Equal(t, codes.Internal, status.Code(err))
				require.Equal(t, expectedMessage, err.Error())

				// Logs contains the stack trace
				tagsByKey := make(map[string]tag.Tag, len(loggedTags))
				for _, tg := range loggedTags {
					tagsByKey[tg.Key()] = tg
				}
				require.Contains(t, tagsByKey, "sys-stack-trace")
				require.Contains(t, tagsByKey["sys-stack-trace"].Value(), "service_error_interceptor_test.go")
				require.Contains(t, tagsByKey, "error")
			} else {
				require.Equal(t, "ok", resp)
				require.NoError(t, err)
			}
		})
	}
}
