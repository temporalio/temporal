//go:build test_dep

package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

func TestGRPCUnaryServerInterceptor_NoGenerator(t *testing.T) {
	t.Parallel()

	interceptor := GRPCUnaryServerInterceptor(NewTestHookGenerator(testhooks.NewTestHooks()))
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

func TestGRPCUnaryServerInterceptor_BeforeHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	generator := NewTestRPCFaultGenerator(testHooks)
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		return true, nil, injectedErr
	})
	interceptor := GRPCUnaryServerInterceptor(NewTestHookGenerator(testHooks))
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

func TestGRPCUnaryServerInterceptor_AfterHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewTestRPCFaultGenerator(testHooks)
	generator.RegisterResponseCallback(RPCFaultScope{NamespaceName: "namespace-name"}, func(context.Context, string, any, any, error) (bool, any, error) {
		return true, "replacement", nil
	})
	interceptor := GRPCUnaryServerInterceptor(NewTestHookGenerator(testHooks))

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

func TestGRPCUnaryServerInterceptor_HandlerError(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	generator := NewTestRPCFaultGenerator(testHooks)
	generator.RegisterResponseCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(_ context.Context, _ string, _, response any, err error) (bool, any, error) {
		require.Nil(t, response)
		require.ErrorIs(t, err, handlerErr)
		return true, nil, injectedErr
	})
	interceptor := GRPCUnaryServerInterceptor(NewTestHookGenerator(testHooks))
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

func TestNewTestRPCFaultGenerator_UnregisterRemovesHook(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewTestRPCFaultGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		return false, nil, nil
	})
	_, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)

	unregister()
	_, ok = testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
}
