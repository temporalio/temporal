package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRPCFaultGenerator_NoCallbacks(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", "request")

	require.NoError(t, err)
	require.Nil(t, response)
	require.False(t, matched)
}

func TestRPCFaultGenerator_CallbackSeparationAndArguments(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	ctx := context.Background()
	request := &struct{}{}
	response := &struct{}{}
	handlerErr := errors.New("handler")
	replacement := &struct{}{}
	injectedErr := errors.New("injected")
	requestCalls := 0
	responseCalls := 0
	generator.RegisterRequestCallback(func(actualCtx context.Context, fullMethod string, actualRequest any) (bool, any, error) {
		requestCalls++
		require.Equal(t, ctx, actualCtx)
		require.Equal(t, "/test.Service/Method", fullMethod)
		require.Same(t, request, actualRequest)
		return false, nil, nil
	})
	generator.RegisterResponseCallback(func(actualCtx context.Context, fullMethod string, actualRequest, actualResponse any, actualErr error) (bool, any, error) {
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
	generator.RegisterRequestCallback(func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 1)
		return false, nil, nil
	})
	generator.RegisterRequestCallback(func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 2)
		return true, "response", nil
	})
	generator.RegisterRequestCallback(func(context.Context, string, any) (bool, any, error) {
		calls = append(calls, 3)
		return true, "other response", nil
	})

	matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", "request")

	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.True(t, matched)
	require.Equal(t, []int{1, 2}, calls)
}

func TestRPCFaultGenerator_UnregisterIsIdempotent(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	unregister := generator.RegisterRequestCallback(func(context.Context, string, any) (bool, any, error) {
		return true, "response", nil
	})

	unregister()
	unregister()
	matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", "request")

	require.NoError(t, err)
	require.Nil(t, response)
	require.False(t, matched)
}

func TestRPCFaultGenerator_UnregisterDuringGenerate(t *testing.T) {
	t.Parallel()

	generator := NewRPCFaultGenerator()
	callbackStarted := make(chan struct{})
	continueCallback := make(chan struct{})
	unregister := generator.RegisterRequestCallback(func(context.Context, string, any) (bool, any, error) {
		close(callbackStarted)
		<-continueCallback
		return true, "response", nil
	})
	type result struct {
		matched  bool
		response any
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", "request")
		resultCh <- result{matched: matched, response: response, err: err}
	}()

	<-callbackStarted
	unregister()
	close(continueCallback)
	generateResult := <-resultCh

	require.NoError(t, generateResult.err)
	require.Equal(t, "response", generateResult.response)
	require.True(t, generateResult.matched)

	matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", "request")
	require.NoError(t, err)
	require.Nil(t, response)
	require.False(t, matched)
}
