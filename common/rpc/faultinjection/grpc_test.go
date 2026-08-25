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

func TestRPCFaultGenerator_Generate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fullMethod := "/test.Service/Method"
	handlerResponse := &struct{}{}
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	bothNamespaces := RPCFaultScope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}

	type callback struct {
		name     string
		scope    RPCFaultScope
		miss     bool
		response any
		err      error
	}

	for _, stage := range []struct {
		name  string
		stage rpcFaultStage
	}{
		{
			name:  "request",
			stage: rpcFaultStageRequest,
		},
		{
			name:  "response",
			stage: rpcFaultStageResponse,
		},
	} {
		stage := stage
		for _, test := range []struct {
			name      string
			request   any
			callbacks []callback
			miss      bool
			response  any
			err       error
			calls     []string
		}{
			{
				name:    "no callbacks",
				request: "request",
				miss:    true,
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
					{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, err: injectedErr},
				},
				miss: true,
			},
			{
				name:    "matched error",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: RPCFaultScope{NamespaceID: "namespace-id"}, err: injectedErr},
				},
				err:   injectedErr,
				calls: []string{"namespace"},
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
			{
				name:    "scope with namespace ID and name/namespace ID",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: bothNamespaces, response: "response"},
				},
				response: "response",
				calls:    []string{"namespace"},
			},
			{
				name:    "scope with namespace ID and name/namespace name",
				request: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
				callbacks: []callback{
					{name: "namespace", scope: bothNamespaces, response: "response"},
				},
				response: "response",
				calls:    []string{"namespace"},
			},
		} {
			test := test
			t.Run(stage.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()

				generator := NewRPCFaultGenerator()
				var calls []string
				invoke := func(callback callback, actualCtx context.Context, actualMethod string, actualRequest, actualResponse any, actualErr error) (bool, any, error) {
					calls = append(calls, callback.name)
					require.Equal(t, ctx, actualCtx)
					require.Equal(t, fullMethod, actualMethod)
					require.Equal(t, test.request, actualRequest)
					if stage.stage == rpcFaultStageResponse {
						require.Equal(t, handlerResponse, actualResponse)
						require.ErrorIs(t, actualErr, handlerErr)
					}
					return !callback.miss, callback.response, callback.err
				}
				for _, callback := range test.callbacks {
					callback := callback
					switch stage.stage {
					case rpcFaultStageRequest:
						generator.RegisterRequestCallback(callback.scope, func(actualCtx context.Context, actualMethod string, actualRequest any) (bool, any, error) {
							return invoke(callback, actualCtx, actualMethod, actualRequest, nil, nil)
						})
					case rpcFaultStageResponse:
						generator.RegisterResponseCallback(callback.scope, func(actualCtx context.Context, actualMethod string, actualRequest, actualResponse any, actualErr error) (bool, any, error) {
							return invoke(callback, actualCtx, actualMethod, actualRequest, actualResponse, actualErr)
						})
					default:
						t.Fatalf("unknown RPC fault stage: %v", stage.stage)
					}
				}

				var matched bool
				var response any
				var err error
				switch stage.stage {
				case rpcFaultStageRequest:
					matched, response, err = generator.GenerateRequest(ctx, fullMethod, test.request)
				case rpcFaultStageResponse:
					matched, response, err = generator.GenerateResponse(ctx, fullMethod, test.request, handlerResponse, handlerErr)
				default:
					t.Fatalf("unknown RPC fault stage: %v", stage.stage)
				}

				if test.err == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, test.err)
				}
				require.Equal(t, !test.miss, matched)
				require.Equal(t, test.response, response)
				require.Equal(t, test.calls, calls)
			})
		}
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

	t.Run("namespace ID and name", func(t *testing.T) {
		t.Parallel()

		generator := NewRPCFaultGenerator()
		unregister := generator.RegisterRequestCallback(RPCFaultScope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}, func(context.Context, string, any) (bool, any, error) {
			return true, "response", nil
		})
		unregister()

		for _, test := range []struct {
			name    string
			request any
		}{
			{
				name:    "namespace ID",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			},
			{
				name:    "namespace name",
				request: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				matched, response, err := generator.GenerateRequest(context.Background(), "/test.Service/Method", test.request)

				require.NoError(t, err)
				require.False(t, matched)
				require.Nil(t, response)
			})
		}
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
