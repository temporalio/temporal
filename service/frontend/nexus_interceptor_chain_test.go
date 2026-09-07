package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	rpcinterceptor "go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNexusChainPreservesNativeErrors(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		outcome      string
		wrapError    bool
		assertErrors func(*testing.T, error, bool)
	}{
		{
			name: "operation error",
			err: &nexus.OperationError{
				Message: "operation failed",
				State:   nexus.OperationStateFailed,
				Cause:   errors.New("worker failure"),
			},
			outcome:   "operation_error",
			wrapError: true,
			assertErrors: func(t *testing.T, err error, _ bool) {
				var operationErr *nexus.OperationError
				require.ErrorAs(t, err, &operationErr)
				require.Equal(t, "operation failed", operationErr.Message)

				convertedErr := convertInterceptorError(err)
				require.ErrorAs(t, convertedErr, &operationErr)
				require.Equal(t, nexus.OperationStateFailed, operationErr.State)
				require.Equal(t, "operation failed", operationErr.Message)
			},
		},
		{
			name:      "handler error",
			err:       nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input"),
			outcome:   "handler_error",
			wrapError: true,
			assertErrors: func(t *testing.T, err error, _ bool) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				require.Equal(t, "invalid input", handlerErr.Message)

				require.ErrorAs(t, convertInterceptorError(err), &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
			},
		},
		{
			name:      "bare handler error",
			err:       nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input"),
			outcome:   "internal_error",
			wrapError: false,
			assertErrors: func(t *testing.T, err error, _ bool) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				require.Equal(t, "invalid input", handlerErr.Message)
			},
		},
		{
			name:      "internal gRPC error",
			err:       status.Error(codes.Internal, "worker failure"),
			outcome:   "internal_error",
			wrapError: true,
			assertErrors: func(t *testing.T, err error, maskErrors bool) {
				require.Equal(t, codes.Internal, status.Code(err))
				if maskErrors {
					require.NotContains(t, err.Error(), "worker failure")
				} else {
					require.ErrorContains(t, err, "worker failure")
				}

				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, convertInterceptorError(err), &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, "internal error", handlerErr.Message)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, maskErrors := range []bool{false, true} {
				t.Run(fmt.Sprintf("mask errors=%t", maskErrors), func(t *testing.T) {
					t.Parallel()
					metricsHandler := metricstest.NewCaptureHandler()
					capture := metricsHandler.StartCapture()
					defer metricsHandler.StopCapture(capture)

					chainedHandler := newTestNexusInterceptorChain(metricsHandler, maskErrors, tc.err, tc.outcome, tc.wrapError)
					_, err := chainedHandler(context.Background(), newTestNexusStartInput())

					rawErr := err
					if tc.wrapError {
						var interceptorErr *interceptornexus.InterceptorError
						require.ErrorAs(t, err, &interceptorErr)
						require.Equal(t, tc.outcome, interceptorErr.Outcome)
						rawErr = interceptorErr.Err
					}
					tc.assertErrors(t, rawErr, maskErrors)

					snapshot := capture.Snapshot()
					require.Len(t, snapshot[metrics.NexusRequests.Name()], 1)
					require.Equal(t, tc.outcome, snapshot[metrics.NexusRequests.Name()][0].Tags["outcome"])
				})
			}
		})
	}
}

func newTestNexusInterceptorChain(
	metricsHandler metrics.Handler,
	maskErrors bool,
	terminalErr error,
	outcome string,
	wrapError bool,
) interceptornexus.HandlerFunc {
	telemetry := rpcinterceptor.NewTelemetryInterceptor(nil, metricsHandler, log.NewNoopLogger(), nil, nil)
	mask := rpcinterceptor.NewMaskInternalErrorDetailsInterceptor(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(maskErrors),
		nil,
		log.NewNoopLogger(),
	)
	serviceErrors := rpcinterceptor.NewServiceErrorInterceptor(
		dynamicconfig.GetIntPropertyFn(4000),
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
	frontendServiceErrors := rpcinterceptor.NewFrontendServiceErrorInterceptorWrapper(log.NewNoopLogger())

	return interceptornexus.ChainInterceptors(
		func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			if !wrapError {
				return nil, terminalErr
			}
			return nil, &interceptornexus.InterceptorError{Err: terminalErr, Outcome: outcome}
		},
		[]interceptornexus.Interceptor{
			telemetry.InterceptNexusOutermost,
			mask.InterceptNexus,
			serviceErrors.InterceptNexus,
			frontendServiceErrors.InterceptNexus,
		},
	)
}

func newTestNexusStartInput() interceptornexus.StartOpInput {
	return interceptornexus.NewStartOpInput(
		"s",
		"o",
		testNamespace,
		time.Now(),
		nexus.StartOperationOptions{},
		nil,
		interceptornexus.ForwardingInfo{},
		interceptornexus.RequestMetadata{NamespaceEntry: testOperationContext().namespace},
	)
}
