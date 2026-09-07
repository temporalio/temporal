//go:build test_dep

package grpcfaults_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/await"
)

func TestCallbackGenerator_Generate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fullMethod := "/test.Service/Method"
	handlerResponse := &struct{}{}
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	bothNamespaces := grpcfaults.Scope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}

	type callback struct {
		name     string
		scope    grpcfaults.Scope
		miss     bool
		response any
		err      error
	}

	for _, stage := range []struct {
		name     string
		response bool
	}{
		{
			name: "request",
		},
		{
			name:     "response",
			response: true,
		},
	} {
		stage := stage
		for _, test := range []struct {
			name          string
			request       any
			callbacks     []callback
			expectedMiss  bool
			expectedResp  any
			expectedErr   error
			expectedCalls []string
		}{
			{
				name:         "no callbacks",
				request:      "request",
				expectedMiss: true,
			},
			{
				name:    "namespace before global",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, response: "namespace response"},
					{name: "global", response: "global response"},
				},
				expectedResp:  "namespace response",
				expectedCalls: []string{"namespace"},
			},
			{
				name:    "global after namespace miss",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, miss: true},
					{name: "global", response: "response"},
				},
				expectedResp:  "response",
				expectedCalls: []string{"namespace", "global"},
			},
			{
				name:    "namespace mismatch",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, err: injectedErr},
				},
				expectedMiss: true,
			},
			{
				name:    "matched error",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, err: injectedErr},
				},
				expectedErr:   injectedErr,
				expectedCalls: []string{"namespace"},
			},
			{
				name:    "first match wins",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "first", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, miss: true}, // no match!
					{name: "second", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, response: "response"},
					{name: "third", scope: grpcfaults.Scope{NamespaceID: "namespace-id"}, response: "other response"}, // never reached!
				},
				expectedResp:  "response",
				expectedCalls: []string{"first", "second"},
			},
			{
				name:    "scope with namespace ID and name/namespace ID",
				request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
				callbacks: []callback{
					{name: "namespace", scope: bothNamespaces, response: "response"},
				},
				expectedResp:  "response",
				expectedCalls: []string{"namespace"},
			},
			{
				name:    "scope with namespace ID and name/namespace name",
				request: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
				callbacks: []callback{
					{name: "namespace", scope: bothNamespaces, response: "response"},
				},
				expectedResp:  "response",
				expectedCalls: []string{"namespace"},
			},
		} {
			test := test
			t.Run(stage.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()

				generator := grpcfaults.NewCallbackGenerator()
				var calls []string
				invoke := func(callback callback, actualCtx context.Context, actualMethod string, actualRequest, actualResponse any, actualErr error) *grpcfaults.Outcome {
					calls = append(calls, callback.name)
					require.Equal(t, ctx, actualCtx)
					require.Equal(t, fullMethod, actualMethod)
					require.Equal(t, test.request, actualRequest)
					if stage.response {
						require.Equal(t, handlerResponse, actualResponse)
						require.ErrorIs(t, actualErr, handlerErr)
					}
					if callback.miss {
						return nil
					}
					return &grpcfaults.Outcome{Response: callback.response, Error: callback.err}
				}

				for _, callback := range test.callbacks {
					callback := callback
					if stage.response {
						generator.RegisterResponseCallback(callback.scope, func(actualCtx context.Context, actualMethod string, actualRequest, actualResponse any, actualErr error) *grpcfaults.Outcome {
							return invoke(callback, actualCtx, actualMethod, actualRequest, actualResponse, actualErr)
						})
					} else {
						generator.RegisterRequestCallback(callback.scope, func(actualCtx context.Context, actualMethod string, actualRequest any) *grpcfaults.Outcome {
							return invoke(callback, actualCtx, actualMethod, actualRequest, nil, nil)
						})
					}
				}

				var outcome *grpcfaults.Outcome
				if stage.response {
					outcome = generator.GenerateResponse(ctx, fullMethod, test.request, handlerResponse, handlerErr)
				} else {
					outcome = generator.GenerateRequest(ctx, fullMethod, test.request)
				}

				if test.expectedMiss {
					require.Nil(t, outcome)
					require.Equal(t, test.expectedCalls, calls)
					return
				}
				require.NotNil(t, outcome)
				if test.expectedErr == nil {
					require.NoError(t, outcome.Error)
				} else {
					require.ErrorIs(t, outcome.Error, test.expectedErr)
				}
				require.Equal(t, test.expectedResp, outcome.Response)
				require.Equal(t, test.expectedCalls, calls)
			})
		}
	}
}

func TestCallbackGenerator_Unregister(t *testing.T) {
	t.Parallel()

	t.Run("idempotent", func(t *testing.T) {
		t.Parallel()

		generator := grpcfaults.NewCallbackGenerator()
		unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
			return &grpcfaults.Outcome{Response: "response"}
		})

		unregister()
		unregister()
		outcome := generator.GenerateRequest(
			context.Background(),
			"/test.Service/Method",
			&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		)

		require.Nil(t, outcome)
	})

	t.Run("removes namespace ID and name registrations", func(t *testing.T) {
		t.Parallel()

		generator := grpcfaults.NewCallbackGenerator()
		unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}, func(context.Context, string, any) *grpcfaults.Outcome {
			return &grpcfaults.Outcome{Response: "response"}
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
				outcome := generator.GenerateRequest(context.Background(), "/test.Service/Method", test.request)

				require.Nil(t, outcome)
			})
		}
	})

	t.Run("during generate", func(t *testing.T) {
		t.Parallel()

		generator := grpcfaults.NewCallbackGenerator()
		callbackStarted := make(chan struct{})
		continueCallback := make(chan struct{})
		unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
			await.Snd(t, callbackStarted, struct{}{})
			await.Rcv(t, continueCallback)
			return &grpcfaults.Outcome{Response: "response"}
		})
		type result struct {
			outcome *grpcfaults.Outcome
		}
		resultCh := make(chan result, 1)
		go func() {
			outcome := generator.GenerateRequest(
				context.Background(),
				"/test.Service/Method",
				&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			)
			await.Snd(t, resultCh, result{outcome: outcome})
		}()

		await.Rcv(t, callbackStarted)
		unregister()
		await.Snd(t, continueCallback, struct{}{})
		generateResult := await.Rcv(t, resultCh)

		require.NotNil(t, generateResult.outcome)
		require.Equal(t, "response", generateResult.outcome.Response)
		require.NoError(t, generateResult.outcome.Error)

		outcome := generator.GenerateRequest(
			context.Background(),
			"/test.Service/Method",
			&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		)
		require.Nil(t, outcome)
	})
}
