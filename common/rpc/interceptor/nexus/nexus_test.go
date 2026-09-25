package nexus

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func TestOperationInputOutcomes(t *testing.T) {
	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
	tests := []struct {
		name    string
		input   InterceptorInput
		out     any
		err     error
		outcome string
	}{
		{
			name:    "start synchronous success",
			input:   StartOpInput{},
			out:     &nexus.HandlerStartOperationResultSync[any]{},
			outcome: "sync_success",
		},
		{
			name:    "start asynchronous success",
			input:   StartOpInput{},
			out:     &nexus.HandlerStartOperationResultAsync{},
			outcome: "async_success",
		},
		{
			name:    "start interceptor error",
			input:   StartOpInput{},
			err:     &InterceptorError{Err: errors.New("failed"), Outcome: "custom_outcome"},
			outcome: "custom_outcome",
		},
		{
			name:    "cancel success",
			input:   CancelOpInput{},
			outcome: "success",
		},
		{
			name:    "cancel unclassified error",
			input:   CancelOpInput{},
			err:     errors.New("failed"),
			outcome: "internal_error",
		},
		{
			name:    "completion success",
			input:   CompleteOpInput{},
			outcome: "success",
		},
		{
			name:    "completion interceptor error",
			input:   CompleteOpInput{},
			err:     &InterceptorError{Err: errors.New("failed"), Outcome: "custom_outcome"},
			outcome: "custom_outcome",
		},
		{
			name:    "completion handler error",
			input:   CompleteOpInput{},
			err:     handlerErr,
			outcome: "error_bad_request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.outcome, tc.input.Outcome(tc.out, tc.err))
		})
	}

	require.Equal(t, "interceptor error (): <nil>", (&InterceptorError{}).Error())
	_, err := NewCompleteOpInput("namespace", time.Now(), nil, nil, ForwardingInfo{}, RequestMetadata{})
	require.EqualError(t, err, "nexus completion request not found")
}

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
