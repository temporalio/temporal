package grpcfaultstest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
)

const fullMethod = "/test.Service/Method"

func newInterceptor(t *testing.T, source *failpoints.Source) grpc.UnaryServerInterceptor {
	t.Helper()
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(source, func(name namespace.Name) namespace.ID {
		if name == "namespace-name" {
			return "namespace-id"
		}
		return ""
	}, primitives.FrontendService))
	require.NotNil(t, interceptor)
	return interceptor
}

func TestNewGenerator_BuggifyByService(t *testing.T) {
	t.Parallel()
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	for service, wantBuggify := range map[primitives.ServiceName]bool{
		primitives.FrontendService:         false,
		primitives.InternalFrontendService: true,
		primitives.HistoryService:          true,
		primitives.MatchingService:         true,
		primitives.WorkerService:           true,
	} {
		source := failpoints.NewEnabledSource()
		require.NoError(t, buggify.For(source.Failpoints()).Set(buggify.Config{
			Seed: 1, Activation: 1, Fire: 1, Error: "Unavailable",
		}))
		interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(source, nil, service))
		_, err := interceptor(context.Background(), "request", info, handler)
		require.Equal(t, wantBuggify, err != nil, service)

		// Registered failpoints apply to every service.
		setFailpoint(source, failpoint.GRPCKey(fullMethod), func(fp *failpoint.Failpoint[error]) {
			fp.Return(errors.New("registered"), failpoint.Always)
		})
		_, err = interceptor(context.Background(), "request", info, handler)
		require.EqualError(t, err, "registered", service)
	}
}

func setFailpoint(source *failpoints.Source, key failpoint.Key, configure func(*failpoint.Failpoint[error])) {
	fp := failpoint.NewFailpoint[error](key)
	configure(fp)
	source.Failpoints().Set(key, fp)
}

func TestUnaryServerInterceptor_NoFailpoints(t *testing.T) {
	t.Parallel()

	interceptor := newInterceptor(t, failpoints.NewEnabledSource())
	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: fullMethod},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
}

func TestUnaryServerInterceptor_BeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	tests := []struct {
		name    string
		key     failpoint.Key
		request any
		wantErr error
	}{
		{name: "scoped to request namespace", key: failpoint.GRPCKey(fullMethod).ForScope("namespace-id"), request: request, wantErr: injectedErr},
		{name: "scoped to other namespace", key: failpoint.GRPCKey(fullMethod).ForScope("other"), request: request, wantErr: nil},
		{name: "unscoped", key: failpoint.GRPCKey(fullMethod), request: request, wantErr: injectedErr},
		{name: "unscoped without request namespace", key: failpoint.GRPCKey(fullMethod), request: &matchingservice.AddWorkflowTaskRequest{}, wantErr: injectedErr},
		{name: "other method", key: failpoint.GRPCKey("/test.Service/Other"), request: request, wantErr: nil},
		{name: "any method", key: failpoint.AnyGRPCKey.ForScope("namespace-id"), request: request, wantErr: injectedErr},
		{
			name:    "namespace name resolved to ID",
			key:     failpoint.AnyGRPCKey.ForScope("namespace-id"),
			request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"},
			wantErr: injectedErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := failpoints.NewEnabledSource()
			setFailpoint(source, tc.key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWith(func(arg any) error {
					require.Equal(t, tc.request, arg)
					return injectedErr
				}, failpoint.Always)
			})
			handlerCalled := false

			response, err := newInterceptor(t, source)(
				context.Background(),
				tc.request,
				&grpc.UnaryServerInfo{FullMethod: fullMethod},
				func(context.Context, any) (any, error) {
					handlerCalled = true
					return "response", nil
				},
			)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, response)
				require.False(t, handlerCalled)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "response", response)
		})
	}
}

func TestUnaryServerInterceptor_AfterHandler(t *testing.T) {
	t.Parallel()

	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	for _, key := range []failpoint.Key{
		failpoint.GRPCResponseKey(fullMethod).ForScope("namespace-id"),
		failpoint.AnyGRPCResponseKey.ForScope("namespace-id"),
	} {
		t.Run(key.Name(), func(t *testing.T) {
			t.Parallel()
			source := failpoints.NewEnabledSource()
			var results []grpcfaults.Result
			setFailpoint(source, key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWith(func(arg any) error {
					result := arg.(grpcfaults.Result)
					results = append(results, result)
					if result.Error != nil {
						return injectedErr
					}
					return nil
				}, failpoint.Always)
			})
			interceptor := newInterceptor(t, source)
			info := &grpc.UnaryServerInfo{FullMethod: fullMethod}

			// A nil error keeps the result of the handler.
			response, err := interceptor(context.Background(), request, info, func(context.Context, any) (any, error) {
				return "response", nil
			})
			require.NoError(t, err)
			require.Equal(t, "response", response)

			// An error replaces it.
			handlerCalled := false
			response, err = interceptor(context.Background(), request, info, func(context.Context, any) (any, error) {
				handlerCalled = true
				return nil, handlerErr
			})
			require.ErrorIs(t, err, injectedErr)
			require.NotErrorIs(t, err, handlerErr)
			require.Nil(t, response)
			require.True(t, handlerCalled)

			require.Equal(t, []grpcfaults.Result{
				{Operation: fullMethod, Request: request, Response: "response"},
				{Operation: fullMethod, Request: request, Error: handlerErr},
			}, results)
		})
	}
}

func TestUnaryServerInterceptor_FailpointServiceExempt(t *testing.T) {
	t.Parallel()

	source := failpoints.NewEnabledSource()
	injectedErr := errors.New("injected")
	method := failpointpb.FailpointService_DisableAllFailpoints_FullMethodName
	for _, key := range []failpoint.Key{
		failpoint.AnyGRPCKey,
		failpoint.AnyGRPCResponseKey,
		failpoint.GRPCKey(method),
		failpoint.GRPCResponseKey(method),
	} {
		setFailpoint(source, key, func(fp *failpoint.Failpoint[error]) { fp.Return(injectedErr, failpoint.Always) })
	}
	interceptor := newInterceptor(t, source)
	handler := func(context.Context, any) (any, error) { return "response", nil }

	response, err := interceptor(context.Background(), &failpointpb.DisableAllFailpointsRequest{}, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	require.NoError(t, err)
	require.Equal(t, "response", response)

	// Other methods still fail.
	_, err = interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
	require.ErrorIs(t, err, injectedErr)
}
