package frontend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/draft/testfixture"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/nexusworkflowref"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const convTestRequestID = "request-id"

// hsmCompletionToken builds the HSM token used by these conversion tests.
func hsmCompletionToken() *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "workflow-id",
		RunId:       "run-id",
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: nexusoperations.OperationMachineType,
				Id:   "42",
			}},
		},
		RequestId: convTestRequestID,
	}
}

func chasmCompletionToken(t *testing.T) *tokenspb.NexusOperationCompletion {
	t.Helper()
	completion, err := nexusworkflowref.HSMRefToCHASMRef(hsmCompletionToken())
	require.NoError(t, err)
	return completion
}

func TestConvertCompletionToOtherFramework(t *testing.T) {
	t.Parallel()

	// HSM -> CHASM.
	toChasm, err := convertCompletionToOtherFramework(hsmCompletionToken())
	require.NoError(t, err)
	require.NotEmpty(t, toChasm.GetComponentRef())
	require.Nil(t, toChasm.GetRef())
	require.Equal(t, convTestRequestID, toChasm.GetRequestId())

	// CHASM -> HSM.
	toHSM, err := convertCompletionToOtherFramework(chasmCompletionToken(t))
	require.NoError(t, err)
	require.NotNil(t, toHSM.GetRef())
	require.Empty(t, toHSM.GetComponentRef())
	require.Equal(t, convTestRequestID, toHSM.GetRequestId())
}

func TestCompleteOperation_FrameworkFallback(t *testing.T) {
	t.Parallel()

	notFound := serviceerror.NewNotFound("operation not found")
	internalErr := serviceerror.NewInternal("boom")
	primaryNotFound := serviceerror.NewNotFound("primary framework: operation not found")
	fallbackNotFound := serviceerror.NewNotFound("fallback framework: operation not found")

	testCases := []struct {
		name string
		// chasmDisabled suppresses the HSM -> CHASM fallback.
		chasmDisabled bool
		token         func(t *testing.T) *tokenspb.NexusOperationCompletion
		setupClient   func(t *testing.T, client *historyservicemock.MockHistoryServiceClient)
		// wantErr is the exact error completeOperation must return; nil expects success.
		wantErr error
	}{
		{
			name:  "HSM primary succeeds, no fallback",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
					Return(&historyservice.CompleteNexusOperationResponse{}, nil)
			},
		},
		{
			name:  "HSM primary NotFound converts to CHASM and succeeds",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
						Return(nil, notFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
							require.NotEmpty(t, req.GetCompletion().GetComponentRef())
							require.Equal(t, convTestRequestID, req.GetCompletion().GetRequestId())
							return &historyservice.CompleteNexusOperationChasmResponse{}, nil
						}),
				)
			},
		},
		{
			name:  "CHASM primary NotFound converts to HSM and succeeds",
			token: chasmCompletionToken,
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).
						Return(nil, notFound),
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationResponse, error) {
							require.NotNil(t, req.GetCompletion().GetRef())
							return &historyservice.CompleteNexusOperationResponse{}, nil
						}),
				)
			},
		},
		{
			name:          "no HSM to CHASM fallback when chasm disabled for namespace",
			chasmDisabled: true,
			token:         func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound)
			},
			wantErr: notFound,
		},
		{
			name: "no fallback when token has no request ID",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion {
				token := hsmCompletionToken()
				token.RequestId = ""
				return token
			},
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound)
			},
			wantErr: notFound,
		},
		{
			name:  "no fallback on non-NotFound error",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, internalErr)
			},
			wantErr: internalErr,
		},
		{
			name:  "both frameworks NotFound returns the error from initial lookup (HSM Completion Token)",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, primaryNotFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, fallbackNotFound),
				)
			},
			wantErr: primaryNotFound,
		},
		{
			name:  "both frameworks NotFound returns the error from initial lookup (CHASM Completion Token)",
			token: chasmCompletionToken,
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, primaryNotFound),
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, fallbackNotFound),
				)
			},
			wantErr: primaryNotFound,
		},
		{
			name:  "initial lookup NotFound with fallback non-NotFound returns error from fallback",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, primaryNotFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, internalErr),
				)
			},
			wantErr: internalErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := historyservicemock.NewMockHistoryServiceClient(ctrl)
			tc.setupClient(t, client)

			h := &nexusCompletionHandler{HistoryClient: client}
			req := &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded, OperationToken: "operation-token"}

			err := h.completeOperation(context.Background(), log.NewNoopLogger(), tc.token(t), &commonpb.Payload{}, req, nil, !tc.chasmDisabled, draft.NewSelection(func() []string { return nil }))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNexusDraftCompletionSelections(t *testing.T) {
	for _, features := range [][]string{nil, {"feature-a"}, {"feature-b"}, {"feature-a", "feature-b"}} {
		for _, chasm := range []bool{false, true} {
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/chasm=%v/failed=%v", features, chasm, failed), func(t *testing.T) {
					ctrl := gomock.NewController(t)
					client := historyservicemock.NewMockHistoryServiceClient(ctrl)
					h := &nexusCompletionHandler{HistoryClient: client}
					reads := 0
					selection := newNexusDraftSelection(func(ns string) []string { require.Equal(t, "tenant", ns); reads++; return features }, "tenant")
					var roots []any
					wanted := &testfixture.Message{Stable: "stable", FeatureA: "a", FeatureB: "b"}
					h.omitDraftFields = func(root any, s *draft.Selection) (bool, error) {
						require.Same(t, selection, s)
						roots = append(roots, root)
						wanted = &testfixture.Message{Stable: "stable", FeatureA: "a", FeatureB: "b"}
						_, err := draft.Omit(wanted, s)
						require.NoError(t, err)
						switch r := root.(type) {
						case *commonpb.Payload:
							r.Metadata["a"] = []byte(wanted.FeatureA)
							r.Metadata["b"] = []byte(wanted.FeatureB)
						case *commonpb.Link:
							r.GetWorkflowEvent().WorkflowId = wanted.FeatureA
							r.GetWorkflowEvent().RunId = wanted.FeatureB
						case *nexuspb.Failure:
							r.Message = wanted.FeatureA
							r.StackTrace = wanted.FeatureB
						case *failurepb.Failure:
							r.Message = wanted.FeatureA
							r.StackTrace = wanted.FeatureB
						default:
							t.Fatalf("unexpected root %T", root)
						}
						return true, nil
					}
					payload := &commonpb.Payload{Data: []byte("opaque"), Metadata: map[string][]byte{"stable": []byte("metadata")}}
					link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "stable"}}}
					verify := func(actualPayload *commonpb.Payload, links []*commonpb.Link, message, stack string) {
						require.Same(t, link, links[0])
						require.Equal(t, "stable", links[0].GetWorkflowEvent().Namespace)
						require.Equal(t, wanted.FeatureA, links[0].GetWorkflowEvent().WorkflowId)
						require.Equal(t, wanted.FeatureB, links[0].GetWorkflowEvent().RunId)
						if failed {
							require.Equal(t, wanted.FeatureA, message)
							require.Equal(t, wanted.FeatureB, stack)
						} else {
							require.Same(t, payload, actualPayload)
							require.Equal(t, []byte("opaque"), actualPayload.Data)
							require.Equal(t, []byte("metadata"), actualPayload.Metadata["stable"])
							require.Equal(t, []byte(wanted.FeatureA), actualPayload.Metadata["a"])
							require.Equal(t, []byte(wanted.FeatureB), actualPayload.Metadata["b"])
						}
					}
					req := &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded}
					if failed {
						payload = nil
						req.State = nexus.OperationStateFailed
						req.Error = &nexus.OperationError{OriginalFailure: &nexus.Failure{Message: "original", StackTrace: "original", Details: []byte(`{"opaque":true}`)}}
					}
					var err error
					if chasm {
						client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
							verify(r.GetSuccess(), r.Links, r.GetFailure().GetMessage(), r.GetFailure().GetStackTrace())
							return &historyservice.CompleteNexusOperationChasmResponse{}, nil
						})
						err = h.completeChasmOperation(context.Background(), log.NewNoopLogger(), chasmCompletionToken(t), payload, req, []*commonpb.Link{link}, selection)
					} else {
						client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *historyservice.CompleteNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationResponse, error) {
							verify(r.GetSuccess(), r.Links, r.GetFailure().GetMessage(), r.GetFailure().GetStackTrace())
							if failed {
								require.Equal(t, []byte(`{"opaque":true}`), r.GetFailure().Details)
							}
							return &historyservice.CompleteNexusOperationResponse{}, nil
						})
						err = h.completeHSMOperation(context.Background(), hsmCompletionToken(), payload, req, []*commonpb.Link{link}, selection)
					}
					require.NoError(t, err)
					require.Len(t, roots, 2)
					require.Equal(t, 1, reads)
				})
			}
		}
	}
}

func TestNexusDraftCompletionFallbackSnapshot(t *testing.T) {
	for _, chasm := range []bool{false, true} {
		t.Run(fmt.Sprintf("chasm=%v", chasm), func(t *testing.T) {
			client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
			h := &nexusCompletionHandler{HistoryClient: client}
			features := []string{"feature-a"}
			reads := 0
			selection := draft.NewSelection(func() []string { reads++; return features })
			calls := 0
			h.omitDraftFields = func(root any, s *draft.Selection) (bool, error) {
				require.Same(t, selection, s)
				calls++
				require.True(t, s.Enabled("feature-a"))
				require.False(t, s.Enabled("feature-b"))
				features = []string{"feature-b"}
				return true, nil
			}
			if chasm {
				gomock.InOrder(client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("retry")), client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(&historyservice.CompleteNexusOperationResponse{}, nil))
			} else {
				gomock.InOrder(client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("retry")), client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(&historyservice.CompleteNexusOperationChasmResponse{}, nil))
			}
			token := hsmCompletionToken()
			if chasm {
				token = chasmCompletionToken(t)
			}
			err := h.completeOperation(context.Background(), log.NewNoopLogger(), token, &commonpb.Payload{}, &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded}, []*commonpb.Link{{}}, true, selection)
			require.NoError(t, err)
			require.Equal(t, 4, calls)
			require.Equal(t, 1, reads)
		})
	}
}

func TestNexusDraftCompletionOmissionErrors(t *testing.T) {
	for _, chasm := range []bool{false, true} {
		for _, kind := range []string{"payload", "link", "failure"} {
			t.Run(fmt.Sprintf("chasm=%v/%s", chasm, kind), func(t *testing.T) {
				h := &nexusCompletionHandler{}
				h.omitDraftFields = func(root any, _ *draft.Selection) (bool, error) { return true, errors.New("private payload") }
				req := &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded}
				var payload *commonpb.Payload
				var links []*commonpb.Link
				switch kind {
				case "payload":
					payload = &commonpb.Payload{}
				case "link":
					links = []*commonpb.Link{{}}
				case "failure":
					req.State = nexus.OperationStateFailed
					req.Error = &nexus.OperationError{OriginalFailure: &nexus.Failure{Message: "failure"}}
				default:
					t.Fatalf("unexpected omission root kind %q", kind)
				}
				selection := draft.NewSelection(func() []string { return nil })
				var err error
				if chasm {
					err = h.completeChasmOperation(context.Background(), log.NewNoopLogger(), chasmCompletionToken(t), payload, req, links, selection)
				} else {
					err = h.completeHSMOperation(context.Background(), hsmCompletionToken(), payload, req, links, selection)
				}
				var he *nexus.HandlerError
				require.ErrorAs(t, err, &he)
				require.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
				require.Equal(t, "internal error", he.Message)
			})
		}
	}
}

func newDraftNexusCompletionHandler(t *testing.T) *nexusCompletionHandler {
	t.Helper()
	operation, _ := newDraftNexusHandler(t)
	oc := newOperationContext(contextOptions{namespaceState: enumspb.NAMESPACE_STATE_REGISTERED, quota: 1, namespaceRateLimitAllow: true, rateLimitAllow: true})
	return &nexusCompletionHandler{ClusterMetadata: operation.clusterMetadata, NamespaceRegistry: operation.namespaceRegistry, Logger: log.NewNoopLogger(), MetricsHandler: metrics.NoopMetricsHandler,
		Config: &Config{MaxNexusOperationTokenLength: dynamicconfig.GetIntPropertyFnFilteredByNamespace(4096), BlobSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024), EnableChasm: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)}, CallbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(), AuthInterceptor: oc.auth, NamespaceValidationInterceptor: oc.namespaceValidationInterceptor, NamespaceConcurrencyLimitInterceptor: oc.namespaceConcurrencyLimitInterceptor, NamespaceRateLimitInterceptor: oc.namespaceRateLimitInterceptor, RateLimitInterceptor: oc.rateLimitInterceptor, RedirectionInterceptor: oc.redirectionInterceptor, clientVersionChecker: headers.NewDefaultVersionChecker()}
}

func draftNexusCompletionRequest(t *testing.T, h *nexusCompletionHandler) *nexusrpc.CompletionRequest {
	t.Helper()
	token, err := h.CallbackTokenGenerator.Tokenize(hsmCompletionToken())
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, commonnexus.PathCompletionCallbackNoIdentifier, nil)
	request.Header.Set(commonnexus.CallbackTokenHeader, token)
	return &nexusrpc.CompletionRequest{HTTPRequest: request, State: nexus.OperationStateSucceeded, OperationToken: "stable-token", Result: draftNexusInput()}
}

func TestNexusDraftCompletionLocalBoundary(t *testing.T) {
	h := newDraftNexusCompletionHandler(t)
	reads := 0
	h.Config.EnabledDraftFeatureAPIs = func(ns string) []string {
		require.Equal(t, "test-namespace", ns)
		reads++
		return []string{"feature-a"}
	}
	h.omitDraftFields = func(root any, s *draft.Selection) (bool, error) {
		payload, ok := root.(*commonpb.Payload)
		require.True(t, ok)
		require.True(t, s.Enabled("feature-a"))
		require.False(t, s.Enabled("feature-b"))
		payload.Metadata["omitted"] = []byte("yes")
		return true, nil
	}
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	h.HistoryClient = client
	client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *historyservice.CompleteNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationResponse, error) {
		require.Equal(t, []byte("yes"), r.GetSuccess().Metadata["omitted"])
		require.Equal(t, []byte("opaque input"), r.GetSuccess().Data)
		require.Equal(t, "stable-token", r.OperationToken)
		return &historyservice.CompleteNexusOperationResponse{}, nil
	})
	require.NoError(t, h.CompleteOperation(context.Background(), draftNexusCompletionRequest(t, h)))
	require.Equal(t, 1, reads)
}

func TestNexusDraftCompletionForwarding(t *testing.T) {
	h := newDraftNexusCompletionHandler(t)
	operation, _ := newDraftNexusHandler(t)
	client := draftNexusForwardingClient(t, operation)
	h.NamespaceRegistry = operation.namespaceRegistry
	h.ForwardingClients = operation.forwardingClients
	h.Config.EnableNamespaceNotActiveAutoForwarding = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	h.Config.EnabledDraftFeatureAPIs = func(string) []string { t.Fatal("forwarding must not read exposure config"); return nil }
	h.omitDraftFields = func(any, *draft.Selection) (bool, error) {
		t.Fatal("forwarding must not omit public roots")
		return false, nil
	}
	forwarded := false
	client.Transport = draftNexusTransport(func(r *http.Request) (*http.Response, error) {
		forwarded = true
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "opaque input", string(body))
		require.Equal(t, "true", r.Header.Get(interceptor.DCRedirectionAPIHeaderName))
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	require.NoError(t, h.CompleteOperation(context.Background(), draftNexusCompletionRequest(t, h)))
	require.True(t, forwarded)
}
