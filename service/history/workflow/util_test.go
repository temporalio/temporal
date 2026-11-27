package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResolveCallbackAttachmentEventReferenceFallsBackToLegacy(t *testing.T) {
	t.Parallel()

	const requestID = "legacy-request-id"
	readErr := serviceerror.NewUnavailable("CHASM unavailable")
	for _, tc := range []struct {
		name    string
		rootErr error
	}{
		{name: "MissingReference"},
		{name: "MissingRoot", rootErr: serviceerror.NewNotFound("component not found")},
		{name: "ReadFailure", rootErr: readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mutableState := historyi.NewMockMutableState(gomock.NewController(t))
			gomock.InOrder(
				mutableState.EXPECT().ChasmEnabled().Return(true),
				mutableState.EXPECT().ChasmWorkflowComponentReadOnly(t.Context()).
					Return(&chasmworkflow.Workflow{}, &chasm.MockContext{}, tc.rootErr),
				mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
					RequestIds: map[string]*persistencespb.RequestIDInfo{
						requestID: {
							EventId:   42,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
						},
					},
				}),
			)

			reference, found, err := ResolveCallbackAttachmentEventReference(t.Context(), mutableState, "update-id", requestID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, chasmworkflow.CallbackAttachmentEventReference{
				EventID:   42,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			}, reference)
		})
	}
}

func TestHasCallbackAttachmentWithoutCHASMRoot(t *testing.T) {
	t.Parallel()

	readErr := serviceerror.NewUnavailable("CHASM unavailable")
	for _, tc := range []struct {
		name         string
		chasmEnabled bool
		rootErr      error
		legacyHit    bool
		wantErr      error
	}{
		{name: "CHASMDisabledUsesLegacy", legacyHit: true},
		{name: "MissingRootUsesLegacy", chasmEnabled: true, rootErr: serviceerror.NewNotFound("component not found"), legacyHit: true},
		{name: "MissingRootWithoutLegacyIsAMiss", chasmEnabled: true, rootErr: serviceerror.NewNotFound("component not found")},
		{name: "ReadFailureIsAnError", chasmEnabled: true, rootErr: readErr, wantErr: readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ms := historyi.NewMockMutableState(gomock.NewController(t))
			ms.EXPECT().StoresUpdateCallbacksInChasm().Return(tc.chasmEnabled)
			if tc.chasmEnabled {
				ms.EXPECT().ChasmEnabled().Return(true)
				ms.EXPECT().ChasmWorkflowComponentReadOnly(gomock.Any()).Return(nil, nil, tc.rootErr)
			}
			if tc.wantErr == nil {
				ms.EXPECT().HasRequestID("request-id").Return(tc.legacyHit)
			}
			attached, err := (MutableStateWithEffects{MutableState: ms}).HasCallbackAttachment("update-id", "request-id")
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.legacyHit, attached)
		})
	}
}

func TestResolveCallbackAttachmentEventReferenceIgnoresFrameworkPlaceholder(t *testing.T) {
	t.Parallel()

	ms := historyi.NewMockMutableState(gomock.NewController(t))
	ms.EXPECT().ChasmEnabled().Return(false)
	ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RequestIds: map[string]*persistencespb.RequestIDInfo{"request-id": {AttachTime: timestamppb.Now()}},
	})

	ref, found, err := ResolveCallbackAttachmentEventReference(t.Context(), ms, "update-id", "request-id")
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, ref)
}

func TestResolveCallbackAttachmentEventReferenceSkipsNoopCHASMTree(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mutableState.EXPECT().ChasmEnabled().Return(false)

	reference, found, err := ResolveCallbackAttachmentEventReference(
		t.Context(),
		mutableState,
		"update-id",
		"request-id",
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, enumspb.EVENT_TYPE_UNSPECIFIED, reference.EventType)
	require.Zero(t, reference.EventID)
}

func TestResolveCallbackAttachmentEventReferenceUsesCHASMState(t *testing.T) {
	t.Parallel()

	const (
		updateID  = "update-id"
		requestID = "request-id"
		eventID   = int64(42)
	)
	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().ChasmEnabled().Return(true)
	chasmCtx := &chasm.MockMutableContext{}
	wf := &chasmworkflow.Workflow{
		Updates: chasm.Map[string, *chasmworkflow.WorkflowUpdate]{
			updateID: chasm.NewComponentField(chasmCtx, &chasmworkflow.WorkflowUpdate{
				UpdateState: &chasmworkflowpb.UpdateState{
					UpdateId: updateID,
					CallbackAttachmentEventReferences: map[string]*chasmworkflowpb.CallbackAttachmentEventReference{
						requestID: {
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
							EventId:   eventID,
						},
					},
				},
			}),
		},
	}
	mutableState.EXPECT().ChasmWorkflowComponentReadOnly(gomock.Any()).Return(wf, chasmCtx, nil)

	reference, found, err := ResolveCallbackAttachmentEventReference(
		t.Context(),
		mutableState,
		updateID,
		requestID,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, reference.EventType)
	require.Equal(t, eventID, reference.EventID)
}

func TestForceTerminateWorkflowRecordsMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
	).Return(&historypb.HistoryEvent{}, nil)
	mutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		"active",
	))

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	err := ForceTerminateWorkflow(
		mutableState,
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
		metricsHandler,
		chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit,
	)
	require.NoError(t, err)

	recordings := capture.Snapshot()[metrics.ExecutionForceTerminations.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, "test-namespace", recordings[0].Tags["namespace"])
	require.Equal(t, string(chasm.WorkflowArchetype), recordings[0].Tags["archetype"])
	require.Equal(t, string(chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit), recordings[0].Tags["reason"])
}
