package nexus

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainNexusInterceptors(t *testing.T) {
	var calls []string
	chain := []Interceptor{
		func(ctx context.Context, in InterceptorInput, next HandlerFunc) (any, error) {
			calls = append(calls, "first-before")
			result, err := next(ctx, in)
			calls = append(calls, "first-after")
			return result, err
		},
		func(ctx context.Context, in InterceptorInput, next HandlerFunc) (any, error) {
			calls = append(calls, "second-before")
			result, err := next(ctx, in)
			calls = append(calls, "second-after")
			return result, err
		},
	}

	result, err := ChainInterceptors(func(context.Context, InterceptorInput) (any, error) {
		calls = append(calls, "handler")
		return "result", nil
	}, chain)(context.Background(), StartOpInput{})

	require.NoError(t, err)
	require.Equal(t, "result", result)
	require.Equal(t, []string{
		"first-before",
		"second-before",
		"handler",
		"second-after",
		"first-after",
	}, calls)
}

func TestChainNexusInterceptorsShortCircuit(t *testing.T) {
	var calls []string
	chain := []Interceptor{
		func(context.Context, InterceptorInput, HandlerFunc) (any, error) {
			calls = append(calls, "interceptor")
			// dont call next - just return
			return "intercepted", nil
		},
	}

	result, err := ChainInterceptors(func(context.Context, InterceptorInput) (any, error) {
		calls = append(calls, "handler")
		return "handler", nil
	}, chain)(context.Background(), StartOpInput{})

	require.NoError(t, err)
	require.Equal(t, "intercepted", result)
	require.Equal(t, []string{"interceptor"}, calls)
}
