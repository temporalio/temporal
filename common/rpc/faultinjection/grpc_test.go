//go:build test_dep

package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
)

func TestNoCallbacks(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	NewRPCFaultGenerator(testHooks)
	_, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))

	require.False(t, ok)
}

func TestCallbackSeparationAndArguments(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewRPCFaultGenerator(testHooks)
	ctx := context.Background()
	request := &struct{}{}
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

	generateRequest, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)
	matched, actualResponse, actualErr := generateRequest(ctx, "/test.Service/Method", request)
	require.NoError(t, actualErr)
	require.Nil(t, actualResponse)
	require.False(t, matched)
	require.Equal(t, 1, requestCalls)
	require.Equal(t, 0, responseCalls)

	generateResponse, ok := testhooks.Get(testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)
	matched, actualResponse, actualErr = generateResponse(ctx, "/test.Service/Method", request, response, handlerErr)

	require.ErrorIs(t, actualErr, injectedErr)
	require.Same(t, replacement, actualResponse)
	require.True(t, matched)
	require.Equal(t, 1, requestCalls)
	require.Equal(t, 1, responseCalls)
}

func TestFirstMatchWins(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewRPCFaultGenerator(testHooks)
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

	generate, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)
	matched, response, err := generate(context.Background(), "/test.Service/Method", "request")

	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.True(t, matched)
	require.Equal(t, []int{1, 2}, calls)
}

func TestUnregisterIsIdempotent(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewRPCFaultGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id"}, func(context.Context, string, any) (bool, any, error) {
		return true, "response", nil
	})

	unregister()
	unregister()
	_, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))

	require.False(t, ok)
}

func TestUnregisterDuringGenerate(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewRPCFaultGenerator(testHooks)
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
	generate, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)
	go func() {
		matched, response, err := generate(context.Background(), "/test.Service/Method", "request")
		await.Snd(t, resultCh, result{matched: matched, response: response, err: err})
	}()

	await.Rcv(t, callbackStarted)
	unregister()
	await.Snd(t, continueCallback, struct{}{})
	generateResult := await.Rcv(t, resultCh)

	require.NoError(t, generateResult.err)
	require.Equal(t, "response", generateResult.response)
	require.True(t, generateResult.matched)

	_, ok = testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
}

func TestRegistersNamespaceIDAndNameAliases(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := NewRPCFaultGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}, func(context.Context, string, any) (bool, any, error) {
		return true, "response", nil
	})

	generateByID, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)
	generateByName, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceName, namespace.Name("namespace-name"))
	require.True(t, ok)

	matched, response, err := generateByID(context.Background(), "/test.Service/Method", "request")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "response", response)
	matched, response, err = generateByName(context.Background(), "/test.Service/Method", "request")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "response", response)

	unregister()
	_, ok = testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
	_, ok = testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceName, namespace.Name("namespace-name"))
	require.False(t, ok)
}
