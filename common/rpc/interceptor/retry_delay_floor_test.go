package interceptor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func retryInfoDetail(err error) *errdetails.RetryInfo {
	for _, detail := range serviceerror.ToStatus(err).Details() {
		if retryInfo, ok := detail.(*errdetails.RetryInfo); ok {
			return retryInfo
		}
	}
	return nil
}

func addRetryInfo(t testing.TB, base *serviceerror.ResourceExhausted, delay time.Duration) *serviceerror.ResourceExhausted {
	t.Helper()
	st, err := base.Status().WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	require.NoError(t, err)
	floored, ok := serviceerror.FromStatus(st).(*serviceerror.ResourceExhausted)
	require.True(t, ok)
	return floored
}

func TestRetryDelayFloorInterceptorIntercept(t *testing.T) {
	t.Parallel()

	clonedRateLimitError := *ErrNamespaceRateLimitServerBusy
	for _, tc := range []struct {
		name            string
		handlerErr      error
		floor           time.Duration
		wantDelay       time.Duration
		wantSameError   bool
		wantResponse    any
		wantMetric      bool
		matchedSentinel *serviceerror.ResourceExhausted
	}{
		{
			name:            "namespace rate limit",
			handlerErr:      ErrNamespaceRateLimitServerBusy,
			floor:           2 * time.Second,
			wantDelay:       2 * time.Second,
			wantMetric:      true,
			matchedSentinel: ErrNamespaceRateLimitServerBusy,
		},
		{
			name:            "concurrent request limit",
			handlerErr:      ErrNamespaceCountLimitServerBusy,
			floor:           2 * time.Second,
			wantDelay:       2 * time.Second,
			wantMetric:      true,
			matchedSentinel: ErrNamespaceCountLimitServerBusy,
		},
		{
			name:            "wrapped sentinel",
			handlerErr:      fmt.Errorf("wrapped limiter error: %w", ErrNamespaceRateLimitServerBusy),
			floor:           2 * time.Second,
			wantDelay:       2 * time.Second,
			wantMetric:      true,
			matchedSentinel: ErrNamespaceRateLimitServerBusy,
		},
		{
			name:            "zero floor",
			handlerErr:      ErrNamespaceRateLimitServerBusy,
			wantSameError:   true,
			matchedSentinel: ErrNamespaceRateLimitServerBusy,
		},
		{
			name:          "non-matching resource exhausted with same cause",
			handlerErr:    &clonedRateLimitError,
			floor:         2 * time.Second,
			wantSameError: true,
		},
		{
			name:         "handler success",
			floor:        2 * time.Second,
			wantResponse: "response",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const namespaceName = "test-namespace"
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			if tc.matchedSentinel != nil {
				registry.EXPECT().GetNamespace(namespace.Name(namespaceName)).Return(nil, nil)
			}
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			interceptor := NewRetryDelayFloorInterceptor(
				registry,
				func(string) time.Duration { return tc.floor },
				mh,
				ErrNamespaceRateLimitServerBusy,
				ErrNamespaceCountLimitServerBusy,
			)
			request := &workflowservice.StartWorkflowExecutionRequest{Namespace: namespaceName}
			response, err := interceptor.Intercept(context.Background(), request,
				&grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"},
				func(context.Context, any) (any, error) { return tc.wantResponse, tc.handlerErr },
			)

			require.Equal(t, tc.wantResponse, response)
			if tc.wantSameError {
				require.Same(t, tc.handlerErr, err)
				require.Nil(t, retryInfoDetail(err))
			} else if tc.wantDelay > 0 {
				resourceExhaustedErr, ok := errors.AsType[*serviceerror.ResourceExhausted](err)
				require.True(t, ok)
				require.Equal(t, tc.matchedSentinel.Cause, resourceExhaustedErr.Cause)
				require.Equal(t, tc.matchedSentinel.Scope, resourceExhaustedErr.Scope)
				require.Equal(t, tc.matchedSentinel.Message, resourceExhaustedErr.Message)
				details := serviceerror.ToStatus(err).Proto().GetDetails()
				require.Len(t, details, 2)
				require.True(t, details[0].MessageIs(&errordetailspb.ResourceExhaustedFailure{}))
				retryInfo := retryInfoDetail(err)
				require.NotNil(t, retryInfo)
				require.Equal(t, tc.wantDelay, retryInfo.GetRetryDelay().AsDuration())
			} else {
				require.NoError(t, err)
			}

			recordings := capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name())
			if tc.wantMetric {
				require.Len(t, recordings, 1)
				require.Equal(t, map[string]string{
					"namespace": namespaceName,
					"operation": "StartWorkflowExecution",
				}, recordings[0].Tags)
			} else {
				require.Empty(t, recordings)
			}
		})
	}
}
