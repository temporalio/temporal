//go:build test_dep

package grpcfaultstest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpoint"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

func TestUnaryServerInterceptor_NoGenerator(t *testing.T) {
	t.Parallel()

	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testhooks.NewTestHooks()))
	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
}

func TestUnaryServerInterceptor_BeforeHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))
	handlerCalled := false

	response, err := interceptor(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return "response", nil
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.False(t, handlerCalled)
}

func TestUnaryServerInterceptor_AfterHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterResponseCallback(grpcfaults.Scope{NamespaceName: "namespace-name"}, func(context.Context, string, any, any, error) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Response: "replacement"}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))

	response, err := interceptor(
		context.Background(),
		&workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "replacement", response)
}

func TestUnaryServerInterceptor_HandlerError(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterResponseCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(_ context.Context, _ string, _, response any, err error) *grpcfaults.Outcome {
		require.Nil(t, response)
		require.ErrorIs(t, err, handlerErr)
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))
	handlerCalled := false

	response, err := interceptor(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, handlerErr
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.True(t, handlerCalled)
}

func TestNewCallbackGenerator_UnregisterRemovesHook(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
		return nil
	})
	_, ok := testhooks.Get(testHooks, testhooks.GRPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)

	unregister()
	_, ok = testhooks.Get(testHooks, testhooks.GRPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
}

func TestUnaryServerInterceptor_Failpoint(t *testing.T) {
	t.Parallel()

	const fullMethod = "/test.Service/Method"
	injectedErr := errors.New("injected")
	tests := []struct {
		name        string
		key         failpoint.Key
		namespaceID string
		wantErr     error
	}{
		{name: "scoped to request namespace", key: failpoint.GRPCKey(fullMethod).ForNamespace("namespace-id"), namespaceID: "namespace-id", wantErr: injectedErr},
		{name: "scoped to other namespace", key: failpoint.GRPCKey(fullMethod).ForNamespace("other"), namespaceID: "namespace-id", wantErr: nil},
		{name: "unscoped", key: failpoint.GRPCKey(fullMethod), namespaceID: "namespace-id", wantErr: injectedErr},
		{name: "unscoped without request namespace", key: failpoint.GRPCKey(fullMethod), namespaceID: "", wantErr: injectedErr},
		{name: "other method", key: failpoint.GRPCKey("/test.Service/Other"), namespaceID: "namespace-id", wantErr: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testHooks := testhooks.NewTestHooks()
			fp := failpoint.NewFailpoint[error](tc.key)
			fp.Return(injectedErr, failpoint.Always)
			testHooks.Failpoints().Set(tc.key, fp)
			interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))

			response, err := interceptor(
				context.Background(),
				&matchingservice.AddWorkflowTaskRequest{NamespaceId: tc.namespaceID},
				&grpc.UnaryServerInfo{FullMethod: fullMethod},
				func(context.Context, any) (any, error) {
					return "response", nil
				},
			)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, response)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "response", response)
		})
	}
}
