package nexus

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func TestInterceptorInputRequest(t *testing.T) {
	dispatchRequest := &http.Request{Method: http.MethodPost}
	requestStartTime := time.Date(2026, time.May, 5, 17, 0, 0, 123456789, time.UTC)
	requestMetadata := RequestMetadata{Request: dispatchRequest}
	inputs := []InterceptorInput{
		NewStartOpInput("s", "o", "n", requestStartTime, nexus.StartOperationOptions{}, nil, ForwardingInfo{}, requestMetadata),
		NewCancelOpInput("s", "o", "n", requestStartTime, nexus.CancelOperationOptions{}, "t", ForwardingInfo{}, requestMetadata),
	}
	for _, input := range inputs {
		require.Same(t, dispatchRequest, input.Request())
		require.True(t, input.StartTime().Equal(requestStartTime))
	}

	completionRequest := &nexusrpc.CompletionRequest{HTTPRequest: &http.Request{}}
	completionInput, err := NewCompleteOpInput("n", requestStartTime, completionRequest, nil, ForwardingInfo{}, RequestMetadata{})
	require.NoError(t, err)
	require.Same(t, completionRequest, completionInput.Request())
	require.True(t, completionInput.StartTime().Equal(requestStartTime))
}

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
