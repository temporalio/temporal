package interceptor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/rpctest"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestResourceExhaustedRetryDelaySurvivesFrontendErrorInterceptors(t *testing.T) {
	t.Parallel()

	const maxMessageLength = 80
	const namespaceName = "test-namespace"
	const method = "/test/method"
	for _, tc := range []struct {
		name    string
		message string
		trunc   bool
	}{
		{name: "short message", message: "namespace rate limit exceeded"},
		{name: "long message", message: strings.Repeat("long resource exhausted message ", 10), trunc: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseErr := addRetryInfo(t, &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: tc.message,
			}, 2*time.Second)
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespace(namespace.Name(namespaceName)).Return(&namespace.Namespace{}, nil)
			maskInterceptor := NewMaskInternalErrorDetailsInterceptor(
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true), registry, log.NewNoopLogger())
			serviceErrorInterceptor := NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(maxMessageLength), metrics.NoopMetricsHandler, log.NewNoopLogger())
			frontendServiceErrorInterceptor := NewFrontendServiceErrorInterceptor(log.NewNoopLogger())
			stream := rpctest.NewMockServerTransportStream(method)
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
			request := &workflowservice.StartWorkflowExecutionRequest{Namespace: namespaceName}
			info := &grpc.UnaryServerInfo{FullMethod: method}

			_, err := maskInterceptor.Intercept(ctx, request, info, func(ctx context.Context, req any) (any, error) {
				return serviceErrorInterceptor.Intercept(ctx, req, info, func(ctx context.Context, req any) (any, error) {
					return frontendServiceErrorInterceptor(ctx, req, info, func(context.Context, any) (any, error) {
						return nil, baseErr
					})
				})
			})
			st, ok := status.FromError(err)
			require.True(t, ok)
			if tc.trunc {
				require.Len(t, st.Message(), maxMessageLength)
				require.True(t, strings.HasSuffix(st.Message(), truncatedSuffix))
			} else {
				require.Equal(t, tc.message, st.Message())
			}
			details := st.Details()
			require.Len(t, details, 2)
			require.IsType(t, &errordetailspb.ResourceExhaustedFailure{}, details[0])
			protorequire.ProtoEqual(t, &errordetailspb.ResourceExhaustedFailure{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			}, details[0].(*errordetailspb.ResourceExhaustedFailure))
			require.IsType(t, &errdetails.RetryInfo{}, details[1])
			protorequire.ProtoEqual(t, &errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)},
				details[1].(*errdetails.RetryInfo))
			require.Equal(t, []string{enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String()}, stream.CapturedHeaders().Get(ResourceExhaustedCauseHeader))
			require.Equal(t, []string{enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE.String()}, stream.CapturedHeaders().Get(ResourceExhaustedScopeHeader))

			clientErr := serviceerror.FromStatus(st)
			resourceExhaustedErr, ok := errors.AsType[*serviceerror.ResourceExhausted](clientErr)
			require.True(t, ok)
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhaustedErr.Cause)
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhaustedErr.Scope)
			retryInfo := retryInfoDetail(clientErr)
			require.NotNil(t, retryInfo)
			require.Equal(t, 2*time.Second, retryInfo.GetRetryDelay().AsDuration())
		})
	}
}

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
