package updateworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

func TestApplyRequest_RejectsUpdateOnPausedWorkflow(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
	ms.EXPECT().IsWorkflowExecutionStatusPaused().Return(true)
	// Required by update.NewRegistry
	ms.EXPECT().GetCurrentVersion().Return(int64(1))
	ms.EXPECT().VisitUpdates(gomock.Any())

	updateReg := update.NewRegistry(ms)

	updater := &Updater{
		req: createUpdateRequest("test-update-id"),
	}

	action, err := updater.ApplyRequest(context.Background(), updateReg, ms)

	require.Nil(t, action)
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.Contains(t, err.Error(), "Workflow is paused")
}

func createUpdateRequest(updateID string) *historyservice.UpdateWorkflowExecutionRequest {
	return &historyservice.UpdateWorkflowExecutionRequest{
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{
					UpdateId: updateID,
				},
			},
		},
	}
}
