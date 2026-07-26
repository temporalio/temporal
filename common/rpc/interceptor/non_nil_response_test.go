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

func TestNewNonNilResponseInterceptor(t *testing.T) {
	const fullMethod = "/test.Service/Test"

	handlerResp := &emptypb.Empty{}
	handlerErr := errors.New("handler failed")
	tests := []struct {
		name          string
		response      any
		err           error
		expectFailure bool
	}{
		{
			name:     "non-nil response",
			response: handlerResp,
		},
		{
			name: "nil response with error",
			err:  handlerErr,
		},
		{
			name:          "nil response",
			expectFailure: true,
		},
		{
			name:          "typed nil response",
			response:      (*emptypb.Empty)(nil),
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

			interceptor := NewNonNilResponseInterceptor(logger)

			resp, err := interceptor(
				t.Context(),
				nil,
				&grpc.UnaryServerInfo{FullMethod: fullMethod},
				func(context.Context, any) (any, error) {
					return test.response, test.err
				},
			)
			require.Equal(t, test.response, resp)
			require.Equal(t, test.err, err)
		})
	}
}
