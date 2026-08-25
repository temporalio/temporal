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

type namespaceIDAndNameRequest struct {
	*matchingservice.AddWorkflowTaskRequest
	*workflowservice.UpdateNamespaceRequest
}

func TestGRPCUnaryServerInterceptor_NoGenerator(t *testing.T) {
	t.Parallel()

	interceptor := GRPCUnaryServerInterceptor(testhooks.NewTestHooks())
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
	testhooks.Set(
		testHooks,
		testhooks.RPCRequestFaultGeneratorByNamespaceID,
		func(context.Context, string, any) (bool, any, error) {
			return true, nil, injectedErr
		},
		namespace.ID("namespace-id"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)
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
	testhooks.Set(
		testHooks,
		testhooks.RPCResponseFaultGeneratorByNamespaceName,
		func(context.Context, string, any, any, error) (bool, any, error) {
			return true, "replacement", nil
		},
		namespace.Name("namespace-name"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)

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
	testhooks.Set(
		testHooks,
		testhooks.RPCResponseFaultGeneratorByNamespaceID,
		func(_ context.Context, _ string, _, response any, err error) (bool, any, error) {
			require.Nil(t, response)
			require.ErrorIs(t, err, handlerErr)
			return true, nil, injectedErr
		},
		namespace.ID("namespace-id"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)
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

func TestGRPCUnaryServerInterceptor_NamespaceMismatch(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	callbackCalled := false
	testhooks.Set(
		testHooks,
		testhooks.RPCRequestFaultGeneratorByNamespaceID,
		func(context.Context, string, any) (bool, any, error) {
			callbackCalled = true
			return true, nil, errors.New("injected")
		},
		namespace.ID("namespace-id"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)

	response, err := interceptor(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.False(t, callbackCalled)
}

func TestGRPCUnaryServerInterceptor_NamespaceIDPrecedence(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	callbackCalled := false
	testhooks.Set(
		testHooks,
		testhooks.RPCRequestFaultGeneratorByNamespaceName,
		func(context.Context, string, any) (bool, any, error) {
			callbackCalled = true
			return true, nil, errors.New("injected")
		},
		namespace.Name("namespace-name"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)

	response, err := interceptor(
		context.Background(),
		namespaceIDAndNameRequest{
			AddWorkflowTaskRequest: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			UpdateNamespaceRequest: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
		},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.False(t, callbackCalled)
}

func TestGRPCUnaryServerInterceptor_NamespaceLessRequest(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	callbackCalled := false
	testhooks.Set(
		testHooks,
		testhooks.RPCRequestFaultGeneratorByNamespaceName,
		func(context.Context, string, any) (bool, any, error) {
			callbackCalled = true
			return true, nil, errors.New("injected")
		},
		namespace.Name("namespace-name"),
	)
	interceptor := GRPCUnaryServerInterceptor(testHooks)

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
	require.False(t, callbackCalled)
}
