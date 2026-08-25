//go:build test_dep

package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/await"
)

type namespaceIDAndNameRequest struct {
	*matchingservice.AddWorkflowTaskRequest
	*workflowservice.UpdateNamespaceRequest
}

func TestRPCFaultGenerator_NamespaceBeforeGlobal(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	globalCalled := false
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		return true, "namespace response", nil
	})
	generator.RegisterRequestCallback(RPCFaultScope{}, func(context.Context, string, any) (bool, any, error) {
		globalCalled = true
		return true, "global response", nil
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)

	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "namespace response", response)
	require.False(t, globalCalled)
}

func TestRPCFaultGenerator_GlobalAfterNamespaceMiss(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	var calls []string
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, "namespace")
		return false, nil, nil
	})
	generator.RegisterRequestCallback(RPCFaultScope{}, func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, "global")
		return true, "response", nil
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)

	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "response", response)
	require.Equal(t, []string{"namespace", "global"}, calls)
}

func TestRPCFaultGenerator_NamespaceMismatch(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	callbackCalled := false
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		callbackCalled = true
		return true, nil, errors.New("injected")
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
	)

	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
	require.False(t, callbackCalled)
}

func TestRPCFaultGenerator_NamespaceIDPrecedence(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	callbackCalled := false
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceName: "namespace-name"}, func(context.Context, string, any) (bool, any, error) {
		callbackCalled = true
		return true, nil, errors.New("injected")
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		namespaceIDAndNameRequest{
			AddWorkflowTaskRequest: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			UpdateNamespaceRequest: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
		},
	)

	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
	require.False(t, callbackCalled)
}

func TestRPCFaultGenerator_NoCallbacks(t *testing.T) {
	t.Parallel()

	matched, response, err := NewRPCFaultGenerator().GenerateRequest(context.Background(), "/test.Service/Method", "request")

	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
}

func TestRPCFaultGenerator_GlobalScope(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	request := &struct{}{}
	response := &struct{}{}
	generator.RegisterRequestCallback(RPCFaultScope{}, func(context.Context, string, any) (bool, any, error) {
		return true, response, nil
	})
	generator.RegisterResponseCallback(RPCFaultScope{}, func(context.Context, string, any, any, error) (bool, any, error) {
		return true, response, nil
	})

	matched, actualResponse, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", request)
	require.NoError(t, err)
	require.True(t, matched)
	require.Same(t, response, actualResponse)

	matched, actualResponse, err = generator.GenerateResponse(context.Background(), "/test.Service/Method", request, nil, nil)
	require.NoError(t, err)
	require.True(t, matched)
	require.Same(t, response, actualResponse)
}

func TestRPCFaultGenerator_CallbackSeparationAndArguments(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	ctx := context.Background()
	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	response := &struct{}{}
	handlerErr := errors.New("handler")
	replacement := &struct{}{}
	injectedErr := errors.New("injected")
	requestCalls := 0
	responseCalls := 0
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(actualCtx context.Context, fullMethod string, actualRequest any) (bool, any, error) {
		requestCalls++
		require.Equal(t, ctx, actualCtx)
		require.Equal(t, "/test.Service/Method", fullMethod)
		require.Same(t, request, actualRequest)
		return false, nil, nil
	})
	generator.RegisterResponseCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(actualCtx context.Context, fullMethod string, actualRequest, actualResponse any, actualErr error) (bool, any, error) {
		responseCalls++
		require.Equal(t, ctx, actualCtx)
		require.Equal(t, "/test.Service/Method", fullMethod)
		require.Same(t, request, actualRequest)
		require.Same(t, response, actualResponse)
		require.ErrorIs(t, actualErr, handlerErr)
		return true, replacement, injectedErr
	})

	matched, actualResponse, actualErr := generator.GenerateRequest(ctx, "/test.Service/Method", request)
	require.NoError(t, actualErr)
	require.Nil(t, actualResponse)
	require.False(t, matched)
	require.Equal(t, 1, requestCalls)
	require.Equal(t, 0, responseCalls)

	matched, actualResponse, actualErr = generator.GenerateResponse(ctx, "/test.Service/Method", request, response, handlerErr)

	require.ErrorIs(t, actualErr, injectedErr)
	require.Same(t, replacement, actualResponse)
	require.True(t, matched)
	require.Equal(t, 1, requestCalls)
	require.Equal(t, 1, responseCalls)
}

func TestRPCFaultGenerator_FirstMatchWins(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	var calls []int
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 1)
		return false, nil, nil // no match!
	})
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 2)
		return true, "response", nil
	})
	generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 3)
		return true, "other response", nil // never reached!
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.True(t, matched)
	require.Equal(t, []int{1, 2}, calls)
}

func TestRPCFaultGenerator_UnregisterIsIdempotent(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		return true, "response", nil
	})

	unregister()
	unregister()
	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)

	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
}

func TestRPCFaultGenerator_UnregisterDuringGenerate(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	callbackStarted := make(chan struct{})
	continueCallback := make(chan struct{})
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		await.Snd(t, callbackStarted, struct{}{})
		await.Rcv(t, continueCallback)
		return true, "response", nil
	})
	type result struct {
		matched  bool
		response any
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		matched, response, err := generator.GenerateRequest(
			context.Background(),
			"/test.Service/Method",
			&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		)
		await.Snd(t, resultCh, result{matched: matched, response: response, err: err})
	}()

	await.Rcv(t, callbackStarted)
	unregister()
	await.Snd(t, continueCallback, struct{}{})
	generateResult := await.Rcv(t, resultCh)

	require.NoError(t, generateResult.err)
	require.Equal(t, "response", generateResult.response)
	require.True(t, generateResult.matched)

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
}

func TestRPCFaultGenerator_RegistersNamespaceIDAndNameAliases(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}, func(context.Context, string, any) (bool, any, error) {
		return true, "response", nil
	})

	matched, response, err := generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "response", response)
	matched, response, err = generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
	)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "response", response)

	unregister()
	matched, response, err = generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
	)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
	matched, response, err = generator.GenerateRequest(
		context.Background(),
		"/test.Service/Method",
		&workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
	)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, response)
}
