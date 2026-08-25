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

func TestRPCFaultGenerator_GenerateRequest(t *testing.T) {
	t.Parallel()

	type callback struct {
		name     string
		scope    RPCFaultScope
		matched  bool
		response any
		err      error
	}
	tests := []struct {
		name      string
		request   any
		callbacks []callback
		matched   bool
		response  any
		calls     []string
	}{
		{
			name:    "namespace before global",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, matched: true, response: "namespace response"},
				{name: "global", matched: true, response: "global response"},
			},
			matched:  true,
			response: "namespace response",
			calls:    []string{"namespace"},
		},
		{
			name:    "global after namespace miss",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}},
				{name: "global", matched: true, response: "response"},
			},
			matched:  true,
			response: "response",
			calls:    []string{"namespace", "global"},
		},
		{
			name:    "namespace mismatch",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, matched: true, err: errors.New("injected")},
			},
		},
		{
			name: "namespace ID precedence",
			request: namespaceIDAndNameRequest{
				AddWorkflowTaskRequest: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				UpdateNamespaceRequest: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
			},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceName: "namespace-name"}, matched: true, err: errors.New("injected")},
			},
		},
		{
			name:    "no callbacks",
			request: "request",
		},
		{
			name:    "first match wins",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "first", scope: RPCFaultScope{NamespaceID: "namespace-id"}}, // no match!
				{name: "second", scope: RPCFaultScope{NamespaceID: "namespace-id"}, matched: true, response: "response"},
				{name: "third", scope: RPCFaultScope{NamespaceID: "namespace-id"}, matched: true, response: "other response"}, // never reached!
			},
			matched:  true,
			response: "response",
			calls:    []string{"first", "second"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			generator := NewRPCFaultGenerator()
			var calls []string
			for _, callback := range test.callbacks {
				generator.RegisterRequestCallback(callback.scope, func(context.Context, string, any) (bool, any, error) {
					calls = append(calls, callback.name)
					return callback.matched, callback.response, callback.err
				})
			}

			matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", test.request)

			require.NoError(t, err)
			require.Equal(t, test.matched, matched)
			require.Equal(t, test.response, response)
			require.Equal(t, test.calls, calls)
		})
	}
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
