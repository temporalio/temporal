package frontend

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/nexusworkflowref"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const convTestRequestID = "request-id"

// hsmCompletionToken builds a minimal HSM-shaped token; chasmCompletionToken derives the CHASM-shaped
// token from it via nexusworkflowref, so only the HSM ref layout is spelled out here.
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

	// HSM token converts to a CHASM-shaped token (ComponentRef, no HSM ref).
	toChasm, err := convertCompletionToOtherFramework(hsmCompletionToken())
	require.NoError(t, err)
	require.NotEmpty(t, toChasm.GetComponentRef())
	require.Nil(t, toChasm.GetRef())
	require.Equal(t, convTestRequestID, toChasm.GetRequestId())

	// CHASM token converts to an HSM-shaped token (ref, no ComponentRef).
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

	testCases := []struct {
		name string
		// chasmDisabled models a CHASM-disabled namespace; it suppresses the HSM->CHASM fallback.
		chasmDisabled bool
		token         func(t *testing.T) *tokenspb.NexusOperationCompletion
		setupClient   func(t *testing.T, client *historyservicemock.MockHistoryServiceClient)
		wantErr       bool
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
			wantErr: true,
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
			wantErr: true,
		},
		{
			name:  "no fallback on non-NotFound error",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, internalErr)
			},
			wantErr: true,
		},
		{
			name:  "both frameworks NotFound returns NotFound",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, notFound),
				)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := historyservicemock.NewMockHistoryServiceClient(ctrl)
			tc.setupClient(t, client)

			h := &nexusCompletionHandler{HistoryClient: client}
			req := &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded, OperationToken: "operation-token"}

			err := h.completeOperation(context.Background(), log.NewNoopLogger(), tc.token(t), &commonpb.Payload{}, req, nil, !tc.chasmDisabled)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
