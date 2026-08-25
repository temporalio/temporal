package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/nexusworkflowref"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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

			err := h.completeOperation(context.Background(), log.NewNoopLogger(), tc.token(t), &commonpb.Payload{}, nil, false, req, nil, !tc.chasmDisabled)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func decodeCanonicalFailure(t *testing.T, wireFailure *nexuspb.Failure) *failurepb.Failure {
	t.Helper()
	nf := commonnexus.ProtoFailureToNexusFailure(wireFailure)
	canonical, err := commonnexus.NexusFailureToTemporalFailure(nf)
	require.NoError(t, err)
	return canonical
}

func wrappedTemporalFailure(t *testing.T, cause *failurepb.Failure) *nexus.Failure {
	t.Helper()
	nf, err := commonnexus.TemporalFailureToNexusFailure(cause)
	require.NoError(t, err)
	return &nexus.Failure{
		Message:  "nexus operation completed unsuccessfully",
		Metadata: map[string]string{"unwrap-error": "true"},
		Cause:    &nf,
	}
}

// completeHSMAndCaptureFailure returns the HSM failure sent to History. It drives the failure through
// resolveFailureForCompletion, the same production code CompleteOperation calls.
func completeHSMAndCaptureFailure(
	t *testing.T,
	state nexus.OperationState,
	originalFailure *nexus.Failure,
	blobSizeLimitError, blobSizeLimitWarn int,
) *nexuspb.Failure {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := historyservicemock.NewMockHistoryServiceClient(ctrl)
	var got *nexuspb.Failure
	client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationResponse, error) {
			got = req.GetFailure()
			return &historyservice.CompleteNexusOperationResponse{}, nil
		})
	h := &nexusCompletionHandler{HistoryClient: client, ThrottledLogger: log.NewNoopLogger()}
	req := &nexusrpc.CompletionRequest{
		State: state,
		Error: &nexus.OperationError{State: state, OriginalFailure: originalFailure},
	}
	failure, failureTruncated, err := h.resolveFailureForCompletion(req, "namespace-id", "workflow-id", "run-id", blobSizeLimitError, blobSizeLimitWarn, metrics.NoopMetricsHandler)
	require.NoError(t, err)

	err = h.completeOperation(context.Background(), log.NewNoopLogger(), hsmCompletionToken(), nil, failure, failureTruncated, req, nil, false)
	require.NoError(t, err)
	require.NotNil(t, got)
	return got
}

func TestCompleteOperation_HSM_FailureSize(t *testing.T) {
	t.Parallel()

	longMessage := strings.Repeat("this failure message is way too long to fit under the configured limits. ", 10)
	t.Run("forwards failures below the limit", func(t *testing.T) {
		original := &nexus.Failure{Message: "small failure", Metadata: map[string]string{"k": "v"}}
		got := completeHSMAndCaptureFailure(t, nexus.OperationStateFailed, original, 1<<20, 1<<20)
		want := commonnexus.NexusFailureToProtoFailure(*original)
		require.True(t, proto.Equal(want, got))
	})

	t.Run("truncates typed failures", func(t *testing.T) {
		original := wrappedTemporalFailure(t, &failurepb.Failure{
			Message: longMessage,
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeApplicationError", NonRetryable: true},
			},
		})
		const limit = 150
		got := decodeCanonicalFailure(t, completeHSMAndCaptureFailure(t, nexus.OperationStateFailed, original, limit, 130))
		require.LessOrEqual(t, got.Size(), limit)
		require.Equal(t, common.FailureReasonFailureExceedsLimit, got.GetMessage())
		require.True(t, got.GetServerFailureInfo().GetNonRetryable())
		require.Equal(t, "SomeApplicationError", got.GetCause().GetApplicationFailureInfo().GetType())
		require.Less(t, len(got.GetCause().GetMessage()), len(longMessage))
	})

	t.Run("uses the canonical size", func(t *testing.T) {
		original := &failurepb.Failure{
			Message: "a moderately sized application failure message used for boundary testing",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError", NonRetryable: true},
			},
		}
		originalFailure, err := commonnexus.TemporalFailureToNexusFailure(original)
		require.NoError(t, err)

		wireSize := commonnexus.NexusFailureToProtoFailure(originalFailure).Size()
		canonical, err := commonnexus.NexusFailureToTemporalFailure(originalFailure)
		require.NoError(t, err)
		limit := canonical.Size()
		require.Greater(t, wireSize, limit)

		got := completeHSMAndCaptureFailure(t, nexus.OperationStateFailed, &originalFailure, limit, limit)
		want := commonnexus.NexusFailureToProtoFailure(originalFailure)
		require.True(t, proto.Equal(want, got))
	})
}

// completeChasmAndCaptureFailure returns the CHASM failure sent to History. It drives the failure through
// resolveFailureForCompletion, the same production code CompleteOperation calls, rather than
// reimplementing its unwrap/convert/size-check/truncate decision.
func completeChasmAndCaptureFailure(
	t *testing.T,
	state nexus.OperationState,
	originalFailure *nexus.Failure,
	blobSizeLimitError, blobSizeLimitWarn int,
) *failurepb.Failure {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := historyservicemock.NewMockHistoryServiceClient(ctrl)
	var got *failurepb.Failure
	client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
			got = req.GetFailure()
			return &historyservice.CompleteNexusOperationChasmResponse{}, nil
		})
	h := &nexusCompletionHandler{HistoryClient: client, ThrottledLogger: log.NewNoopLogger()}
	req := &nexusrpc.CompletionRequest{
		State: state,
		Error: &nexus.OperationError{State: state, OriginalFailure: originalFailure},
	}
	failure, failureTruncated, err := h.resolveFailureForCompletion(req, "namespace-id", "workflow-id", "run-id", blobSizeLimitError, blobSizeLimitWarn, metrics.NoopMetricsHandler)
	require.NoError(t, err)

	err = h.completeOperation(context.Background(), log.NewNoopLogger(), chasmCompletionToken(t), nil, failure, failureTruncated, req, nil, true)
	require.NoError(t, err)
	require.NotNil(t, got)
	return got
}

func TestCompleteOperation_CHASM_FailureSize(t *testing.T) {
	t.Parallel()

	longMessage := strings.Repeat("this failure message is way too long to fit under the configured limits. ", 10)

	t.Run("forwards failures below the limit", func(t *testing.T) {
		original := &nexus.Failure{Message: "small failure", Metadata: map[string]string{"k": "v"}}
		got := completeChasmAndCaptureFailure(t, nexus.OperationStateFailed, original, 1<<20, 1<<20)
		want, err := commonnexus.NexusFailureToTemporalFailure(*nexusrpc.UnwrapFailure(original))
		require.NoError(t, err)
		require.True(t, proto.Equal(want, got))
	})

	// CHASM has no separate State field on the completion request: HandleNexusCompletion routes to
	// onCanceled vs. onFailed based solely on whether the top-level failure carries CanceledFailureInfo,
	// so that marker must survive truncation.
	for _, tc := range []struct {
		name     string
		original *nexus.Failure
		canceled bool
	}{
		{
			name: "failed",
			original: wrappedTemporalFailure(t, &failurepb.Failure{
				Message: longMessage,
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
				},
			}),
			canceled: false,
		},
		{
			name: "canceled",
			original: wrappedTemporalFailure(t, &failurepb.Failure{
				Message: longMessage,
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
			}),
			canceled: true,
		},
	} {
		t.Run("truncates "+tc.name+" failures", func(t *testing.T) {
			const limit = 150
			got := completeChasmAndCaptureFailure(t, nexus.OperationStateFailed, tc.original, limit, 80)
			require.LessOrEqual(t, got.Size(), limit)
			require.Equal(t, common.FailureReasonFailureExceedsLimit, got.GetMessage())
			require.Less(t, len(got.GetCause().GetMessage()), len(longMessage))
			require.Equal(t, tc.canceled, got.GetCanceledFailureInfo() != nil)
		})
	}
}

func TestTruncateOversizedFailure(t *testing.T) {
	t.Parallel()

	t.Run("within budget is returned unchanged", func(t *testing.T) {
		f := &failurepb.Failure{Message: "small"}
		got := truncateOversizedFailure(f, 1000, 500)
		require.Same(t, f, got)
	})

	oversized := &failurepb.Failure{Message: strings.Repeat("a", 500)}
	for _, tc := range []struct {
		name       string
		errorLimit int
		warnLimit  int
	}{
		{"warn below error (typical config)", 200, 100},
		{"warn equal to error", 200, 200},
		{"warn misconfigured above error", 100, 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateOversizedFailure(oversized, tc.errorLimit, tc.warnLimit)
			require.LessOrEqual(t, got.Size(), tc.errorLimit, "truncated failure must respect the error limit regardless of the warn limit")
			require.Equal(t, common.FailureReasonFailureExceedsLimit, got.GetMessage())
			require.NotNil(t, got.GetCause())
		})
	}
}
