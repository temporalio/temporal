package pollupdate_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/pollupdate"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

type mockUpdateReg struct {
	update.Registry
	OutcomeFunc func(string) (future.Future[*updatepb.Outcome], bool)
}

func (mur *mockUpdateReg) Outcome(protoInstID string) (future.Future[*updatepb.Outcome], bool) {
	return mur.OutcomeFunc(protoInstID)
}

func pollRequestForTest(t *testing.T, suffix string) *historyservice.PollWorkflowExecutionUpdateRequest {
	nsID := fmt.Sprintf("%v-namsespace.%v", t.Name(), suffix)
	return &historyservice.PollWorkflowExecutionUpdateRequest{
		NamespaceId: nsID,
		Request: &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: nsID,
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: fmt.Sprintf("%v-workflow_id.%v", t.Name(), suffix),
					RunId:      fmt.Sprintf("%v-run_id.%v", t.Name(), suffix),
				},
				UpdateId: fmt.Sprintf("%v-update_id.%v", t.Name(), suffix),
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
		},
	}
}

func TestPollUpdateInFlight(t *testing.T) {
	updateReg := &mockUpdateReg{}
	mockMS := workflow.NewMockMutableState(gomock.NewController(t))
	mockWfCtx := workflow.NewMockContext(gomock.NewController(t))
	mockWfCtx.EXPECT().UpdateRegistry().Return(updateReg).AnyTimes()
	mockWfCtx.EXPECT().LoadMutableState(gomock.Any()).Return(mockMS, nil).AnyTimes()
	ctxLookup := func(
		context.Context,
		*clockspb.VectorClock,
		api.MutableStateConsistencyPredicate,
		definition.WorkflowKey,
	) (api.WorkflowContext, error) {
		return api.NewWorkflowContext(mockWfCtx, func(error) {}, nil), nil
	}
	observers := workflow.NewObserverSet()

	t.Run("not found", func(t *testing.T) {
		updateReg.OutcomeFunc = func(string) (future.Future[*updatepb.Outcome], bool) {
			return nil, false
		}
		want := serviceerror.NewNotFound("not found")
		mockMS.EXPECT().GetUpdateOutcome(gomock.Any(), gomock.Any()).Return(nil, want)
		respchan := make(chan *historyservice.PollWorkflowExecutionUpdateResponse)
		errchan := make(chan error)
		go func() {
			resp, err := pollupdate.Invoke(
				context.TODO(),
				pollRequestForTest(t, "0"),
				ctxLookup,
				observers,
			)
			if err != nil {
				errchan <- err
				return
			}
			respchan <- resp
		}()
		err := <-errchan
		require.ErrorIs(t, err, want)
	})

	t.Run("in-flight update", func(t *testing.T) {
		outcomeFuture := future.NewFuture[*updatepb.Outcome]()
		updateReg.OutcomeFunc = func(string) (future.Future[*updatepb.Outcome], bool) {
			return outcomeFuture, true
		}
		respchan := make(chan *historyservice.PollWorkflowExecutionUpdateResponse)
		errchan := make(chan error)
		go func() {
			resp, err := pollupdate.Invoke(
				context.TODO(),
				pollRequestForTest(t, "0"),
				ctxLookup,
				observers,
			)
			if err != nil {
				errchan <- err
				return
			}
			respchan <- resp
		}()

		want := &updatepb.Outcome{}
		outcomeFuture.Set(want, nil)
		pollResp := <-respchan
		require.Same(t, want, pollResp.Response.Outcome)
	})

	t.Run("outcome from history", func(t *testing.T) {
		want := &updatepb.Outcome{}
		mockMS.EXPECT().GetUpdateOutcome(gomock.Any(), gomock.Any()).Return(want, nil)
		updateReg.OutcomeFunc = func(string) (future.Future[*updatepb.Outcome], bool) {
			return nil, false
		}
		respchan := make(chan *historyservice.PollWorkflowExecutionUpdateResponse)
		errchan := make(chan error)
		go func() {
			resp, err := pollupdate.Invoke(
				context.TODO(),
				pollRequestForTest(t, "0"),
				ctxLookup,
				observers,
			)
			if err != nil {
				errchan <- err
				return
			}
			respchan <- resp
		}()
		pollResp := <-respchan
		require.Same(t, want, pollResp.Response.Outcome)
	})
}
