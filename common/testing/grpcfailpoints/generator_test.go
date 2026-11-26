package grpcfailpoints_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/server/common/testing/grpcfailpoints"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"google.golang.org/grpc"
)

const fullMethod = "/test.Service/Method"

func newInterceptor(t *testing.T, source *failpoint.Registry) grpc.UnaryServerInterceptor {
	t.Helper()
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfailpoints.NewGenerator(source, failpointadapter.Namespaces{ID: func(name namespace.Name) (namespace.ID, bool) {
		if name == "namespace-name" {
			return "namespace-id", true
		}
		return "", false
	}}, false))
	require.NotNil(t, interceptor)
	return interceptor
}

func setFailpoint(source *failpoint.Registry, key failpoint.Key[error], configure func(*failpoint.Failpoint[error])) {
	fp := failpoint.NewFailpoint(key)
	configure(fp)
	source.Set(fp)
}

func TestNewGenerator_ExplicitOnly(t *testing.T) {
	t.Parallel()
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	for _, explicitOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicitOnly=%t", explicitOnly), func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			b := buggify.New(source)
			require.NoError(t, b.Set(buggify.Config{
				Seed: 1, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"}},
			}))
			interceptor := grpcfaults.UnaryServerInterceptor(grpcfailpoints.NewGenerator(source, failpointadapter.Namespaces{}, explicitOnly))
			_, err := interceptor(context.Background(), "request", info, handler)
			require.Equal(t, !explicitOnly, err != nil)

			// Registered failpoints apply with and without the policy.
			setFailpoint(source, serverfailpoints.GRPCKey(fullMethod), func(fp *failpoint.Failpoint[error]) {
				fp.Return(errors.New("registered"))
			})
			_, err = interceptor(context.Background(), "request", info, handler)
			require.EqualError(t, err, "registered")
		})
	}
}

func TestUnaryServerInterceptor_NoFailpoints(t *testing.T) {
	t.Parallel()

	interceptor := newInterceptor(t, failpoint.NewRegistry())
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
		key     failpoint.Key[error]
		request any
		wantErr error
	}{
		{name: "scoped to request namespace", key: serverfailpoints.GRPCKey(fullMethod).ForScope("namespace-id"), request: request, wantErr: injectedErr},
		{name: "scoped to other namespace", key: serverfailpoints.GRPCKey(fullMethod).ForScope("other"), request: request, wantErr: nil},
		{name: "unscoped", key: serverfailpoints.GRPCKey(fullMethod), request: request, wantErr: injectedErr},
		{name: "unscoped without request namespace", key: serverfailpoints.GRPCKey(fullMethod), request: &matchingservice.AddWorkflowTaskRequest{}, wantErr: injectedErr},
		{name: "other method", key: serverfailpoints.GRPCKey("/test.Service/Other"), request: request, wantErr: nil},
		{name: "any method", key: serverfailpoints.AnyGRPCKey.ForScope("namespace-id"), request: request, wantErr: injectedErr},
		{
			name:    "namespace name resolved to ID",
			key:     serverfailpoints.AnyGRPCKey.ForScope("namespace-id"),
			request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"},
			wantErr: injectedErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			setFailpoint(source, tc.key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWithArg(func(arg any) error {
					require.Equal(t, tc.request, arg)
					return injectedErr
				})
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
	for _, key := range []failpoint.Key[error]{
		serverfailpoints.GRPCResponseKey(fullMethod).ForScope("namespace-id"),
		serverfailpoints.AnyGRPCResponseKey.ForScope("namespace-id"),
	} {
		t.Run(key.Name(), func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			var results []*grpcfaults.Result
			setFailpoint(source, key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWithArg(func(arg any) error {
					result := arg.(*grpcfaults.Result)
					results = append(results, result)
					if result.Error != nil {
						return injectedErr
					}
					return nil
				})
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

			require.Equal(t, []*grpcfaults.Result{
				{Operation: fullMethod, Request: request, Response: "response"},
				{Operation: fullMethod, Request: request, Error: handlerErr},
			}, results)
		})
	}
}
