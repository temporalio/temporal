package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainNexusInterceptors(t *testing.T) {
	var calls []string
	chain := []NexusInterceptor{
		func(ctx context.Context, in NexusInterceptorInput, next NexusHandlerFunc) (any, error) {
			calls = append(calls, "first-before")
			result, err := next(ctx, in)
			calls = append(calls, "first-after")
			return result, err
		},
		func(ctx context.Context, in NexusInterceptorInput, next NexusHandlerFunc) (any, error) {
			calls = append(calls, "second-before")
			result, err := next(ctx, in)
			calls = append(calls, "second-after")
			return result, err
		},
	}

	result, err := ChainNexusInterceptors(func(context.Context, NexusInterceptorInput) (any, error) {
		calls = append(calls, "handler")
		return "result", nil
	}, chain)(context.Background(), StartNexusOpInput{})

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
	chain := []NexusInterceptor{
		func(context.Context, NexusInterceptorInput, NexusHandlerFunc) (any, error) {
			calls = append(calls, "interceptor")
			// dont call next - just return
			return "intercepted", nil
		},
	}

	result, err := ChainNexusInterceptors(func(context.Context, NexusInterceptorInput) (any, error) {
		calls = append(calls, "handler")
		return "handler", nil
	}, chain)(context.Background(), StartNexusOpInput{})

	require.NoError(t, err)
	require.Equal(t, "intercepted", result)
	require.Equal(t, []string{"interceptor"}, calls)
}
