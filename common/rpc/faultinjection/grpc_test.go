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
		miss     bool
		response any
		err      error
	}

	for _, test := range []struct {
		name      string
		request   any
		callbacks []callback
		response  any
		calls     []string
	}{
		{
			name:    "no callbacks",
			request: "request",
		},
		{
			name:    "namespace before global",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, response: "namespace response"},
				{name: "global", response: "global response"},
			},
			response: "namespace response",
			calls:    []string{"namespace"},
		},
		{
			name:    "global after namespace miss",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, miss: true},
				{name: "global", response: "response"},
			},
			response: "response",
			calls:    []string{"namespace", "global"},
		},
		{
			name:    "namespace mismatch",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, err: errors.New("injected")},
			},
		},
		{
			name: "namespace ID precedence",
			request: namespaceIDAndNameRequest{
				AddWorkflowTaskRequest: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				UpdateNamespaceRequest: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
			},
			callbacks: []callback{
				{name: "namespace", scope: RPCFaultScope{NamespaceName: "namespace-name"}, err: errors.New("injected")},
			},
		},
		{
			name:    "first match wins",
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			callbacks: []callback{
				{name: "first", scope: RPCFaultScope{NamespaceID: "namespace-id"}, miss: true}, // no match!
				{name: "second", scope: RPCFaultScope{NamespaceID: "namespace-id"}, response: "response"},
				{name: "third", scope: RPCFaultScope{NamespaceID: "namespace-id"}, response: "other response"}, // never reached!
			},
			response: "response",
			calls:    []string{"first", "second"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			generator := NewRPCFaultGenerator()
			var calls []string
			for _, callback := range test.callbacks {
				generator.RegisterRequestCallback(callback.scope, func(context.Context, string, any) (bool, any, error) {
					calls = append(calls, callback.name)
					return !callback.miss, callback.response, callback.err
				})
			}

			matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", test.request)

			require.NoError(t, err)
			require.Equal(t, test.response != nil, matched)
			require.Equal(t, test.response, response)
			require.Equal(t, test.calls, calls)
		})
	}
}

func TestRPCFaultGenerator_GenerateArguments(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	response := &struct{}{}
	handlerErr := errors.New("handler")
	replacement := &struct{}{}
	injectedErr := errors.New("injected")

	for _, test := range []struct {
		name          string
		scope         RPCFaultScope
		stage         rpcFaultStage
		callbackErr   error
		requestCalls  int
		responseCalls int
	}{
		{
			name:         "global request",
			stage:        rpcFaultStageRequest,
			requestCalls: 1,
		},
		{
			name:         "namespace request",
			scope:        RPCFaultScope{NamespaceID: "namespace-id"},
			stage:        rpcFaultStageRequest,
			callbackErr:  injectedErr,
			requestCalls: 1,
		},
		{
			name:          "global response",
			stage:         rpcFaultStageResponse,
			responseCalls: 1,
		},
		{
			name:          "namespace response",
			scope:         RPCFaultScope{NamespaceID: "namespace-id"},
			stage:         rpcFaultStageResponse,
			callbackErr:   injectedErr,
			responseCalls: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			generator := NewRPCFaultGenerator()
			requestCalls := 0
			responseCalls := 0
			generator.RegisterRequestCallback(test.scope, func(actualCtx context.Context, fullMethod string, actualRequest any) (bool, any, error) {
				requestCalls++
				require.Equal(t, ctx, actualCtx)
				require.Equal(t, "/test.Service/Method", fullMethod)
				require.Same(t, request, actualRequest)
				return true, replacement, test.callbackErr
			})
			generator.RegisterResponseCallback(test.scope, func(actualCtx context.Context, fullMethod string, actualRequest, actualResponse any, actualErr error) (bool, any, error) {
				responseCalls++
				require.Equal(t, ctx, actualCtx)
				require.Equal(t, "/test.Service/Method", fullMethod)
				require.Same(t, request, actualRequest)
				require.Same(t, response, actualResponse)
				require.ErrorIs(t, actualErr, handlerErr)
				return true, replacement, test.callbackErr
			})

			var matched bool
			var actualResponse any
			var actualErr error
			switch test.stage {
			case rpcFaultStageRequest:
				matched, actualResponse, actualErr = generator.GenerateRequest(ctx, "/test.Service/Method", request)
			case rpcFaultStageResponse:
				matched, actualResponse, actualErr = generator.GenerateResponse(ctx, "/test.Service/Method", request, response, handlerErr)
			default:
				t.Fatalf("unknown RPC fault stage: %v", test.stage)
			}

			if test.callbackErr == nil {
				require.NoError(t, actualErr)
			} else {
				require.ErrorIs(t, actualErr, test.callbackErr)
			}
			require.Same(t, replacement, actualResponse)
			require.True(t, matched)
			require.Equal(t, test.requestCalls, requestCalls)
			require.Equal(t, test.responseCalls, responseCalls)
		})
	}
}

func TestRPCFaultGenerator_Unregister(t *testing.T) {
	t.Parallel()

	t.Run("idempotent", func(t *testing.T) {
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
	})

	t.Run("during generate", func(t *testing.T) {
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
	})
}
