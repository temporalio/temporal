//go:build test_dep

package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAddNonNilResponseInterceptor(t *testing.T) {
	const fullMethod = "/test.Service/Test"

	handlerResp := &emptypb.Empty{}
	handlerErr := errors.New("handler failed")
	tests := []struct {
		name          string
		handler       grpc.UnaryHandler
		expectFailure bool
		expectedResp  any
		expectedErr   error
	}{
		{
			name: "non-nil response",
			handler: func(context.Context, any) (any, error) {
				return handlerResp, nil
			},
			expectedResp: handlerResp,
		},
		{
			name: "nil response with error",
			handler: func(context.Context, any) (any, error) {
				return nil, handlerErr
			},
			expectedErr: handlerErr,
		},
		{
			name: "nil response",
			handler: func(context.Context, any) (any, error) {
				return nil, nil
			},
			expectFailure: true,
		},
		{
			name: "typed nil response",
			handler: func(context.Context, any) (any, error) {
				var resp *emptypb.Empty
				return resp, nil
			},
			expectFailure: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := log.NewMockLogger(gomock.NewController(t))
			if test.expectFailure {
				logger.EXPECT().Error(
					"failed assertion: "+nilResponseMessage,
					tag.FailedAssertion,
					tag.Operation(fullMethod),
				)
			}

			interceptors := AddNonNilResponseInterceptor(nil, logger)
			require.Len(t, interceptors, 1)

			resp, err := interceptors[0](
				t.Context(),
				nil,
				&grpc.UnaryServerInfo{FullMethod: fullMethod},
				test.handler,
			)
			if test.expectFailure || test.expectedResp == nil {
				require.Nil(t, resp)
			} else {
				require.Same(t, test.expectedResp, resp)
			}
			if test.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.expectedErr, err)
			}
		})
	}
}
