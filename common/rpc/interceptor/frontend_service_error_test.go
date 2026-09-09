package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/rpctest"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestFrontendServiceErrorInterceptor(t *testing.T) {
	tests := []struct {
		name            string
		handlerErr      error
		configureStream func(s *rpctest.MockServerTransportStream)
		verifyFn        func(t *testing.T, err error, stream *rpctest.MockServerTransportStream)
		expectLogErr    string
	}{
		{
			name:       "Passthrough",
			handlerErr: nil,
			verifyFn: func(t *testing.T, err error, _ *rpctest.MockServerTransportStream) {
				require.NoError(t, err)
			},
		},
		{
			name:       "Mask ShardOwnershipLost",
			handlerErr: serviceerrors.NewShardOwnershipLost("owner-host", "current-host"),
			verifyFn: func(t *testing.T, err error, _ *rpctest.MockServerTransportStream) {
				require.Error(t, err)

				var unavail *serviceerror.Unavailable
				require.ErrorAs(t, err, &unavail)
				assert.Contains(t, unavail.Error(), "shard unavailable")
			},
		},
		{
			name:       "Mask DataLoss",
			handlerErr: serviceerror.NewDataLoss("..."),
			verifyFn: func(t *testing.T, err error, _ *rpctest.MockServerTransportStream) {
				require.Error(t, err)

				var unavail *serviceerror.Unavailable
				require.ErrorAs(t, err, &unavail)
				assert.Equal(t, "internal history service error", unavail.Error())
			},
		},
		{
			name: "Set ResourceExhaustedHeaders",
			handlerErr: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
			verifyFn: func(t *testing.T, err error, s *rpctest.MockServerTransportStream) {
				require.Error(t, err)

				hdr := s.CapturedHeaders()
				require.NotNil(t, hdr)
				assert.Equal(t, []string{
					enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String()},
					hdr.Get(ResourceExhaustedCauseHeader))
				assert.Equal(t, []string{
					enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM.String()},
					hdr.Get(ResourceExhaustedScopeHeader))
			},
		},
		{
			name:       "Set ResourceExhaustedHeaders Failure",
			handlerErr: serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "rate limit exceeded"),
			configureStream: func(s *rpctest.MockServerTransportStream) {
				s.SetHeaderFunc = func(md metadata.MD) error { return errors.New("injected header failure") }
			},
			expectLogErr: "Failed to add Resource-Exhausted headers to response",
			verifyFn: func(t *testing.T, err error, _ *rpctest.MockServerTransportStream) {
				require.Error(t, err)

				var re *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &re)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, re.Cause)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			method := "/test/method"

			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			if tc.expectLogErr != "" {
				tl.Expect(testlogger.Error, tc.expectLogErr, tag.Operation("method"))
			}

			stream := rpctest.NewMockServerTransportStream(method)
			if tc.configureStream != nil {
				tc.configureStream(stream)
			}
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

			var interceptorFn = NewFrontendServiceErrorInterceptor(tl)
			info := &grpc.UnaryServerInfo{FullMethod: method}
			_, err := interceptorFn(ctx, nil, info,
				func(_ context.Context, _ any) (any, error) {
					return nil, tc.handlerErr
				})

			tc.verifyFn(t, err, stream)
		})
	}
}
