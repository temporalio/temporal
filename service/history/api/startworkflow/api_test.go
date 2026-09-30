package startworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

func testCallbacks() []*commonpb.Callback {
	return []*commonpb.Callback{{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback"}},
	}}
}

// Callbacks attached on conflict must reserve those of the workflow's in-flight Updates, or they
// could consume the headroom those Updates were admitted against, and the execution would exceed
// its limits once they are accepted.
func TestValidateAttachedCallbacks_ReservesInFlightUpdateCallbacks(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().VisitUpdates(gomock.Any()).AnyTimes()
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	ms.EXPECT().GetUpdateOutcome(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found")).AnyTimes()

	updateReg := update.NewRegistry(ms)
	inFlightReq := &updatepb.Request{
		Meta:                &updatepb.Meta{UpdateId: "in-flight-update-id"},
		Input:               &updatepb.Input{Name: "not_empty"},
		RequestId:           "in-flight-request-id",
		CompletionCallbacks: testCallbacks(),
	}
	inFlightUpd, _, err := updateReg.FindOrCreate(ctx, "in-flight-update-id")
	require.NoError(t, err)
	require.NoError(t, inFlightUpd.Admit(inFlightReq, workflow.WithEffects(effect.Immediate(ctx), ms)))

	wfContext := historyi.NewMockWorkflowContext(ctrl)
	wfContext.EXPECT().UpdateRegistry(gomock.Any()).Return(updateReg)

	attached := testCallbacks()
	limitErr := serviceerror.NewFailedPrecondition("cannot attach more than 1 callbacks to an execution")
	// The in-flight request round-trips through the Registry's serialized copy.
	ms.EXPECT().ValidateCallbackAddition(
		protomock.Eq([]chasmworkflow.CallbackAddition{{
			UpdateID:  "in-flight-update-id",
			RequestID: "in-flight-request-id",
			Callbacks: inFlightReq.CompletionCallbacks,
		}}),
		chasmworkflow.CallbackAddition{RequestID: "attach-request-id", Callbacks: attached},
	).Return(limitErr)

	err = validateAttachedCallbacks(ctx, api.NewWorkflowLease(wfContext, nil, ms), "attach-request-id", attached)
	require.ErrorIs(t, err, limitErr)
}

func TestValidateAttachedCallbacks_NothingAttached(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	// Neither the Registry nor the mutable state is consulted.
	wfContext := historyi.NewMockWorkflowContext(ctrl)
	ms := historyi.NewMockMutableState(ctrl)

	require.NoError(t, validateAttachedCallbacks(context.Background(), api.NewWorkflowLease(wfContext, nil, ms), "", nil))
}
