package recordworkflowtaskstarted

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type mutableStateModifier func(*historyi.MockMutableState)

func TestRecordWorkflowTaskStarted_Errors(t *testing.T) {
	t.Run("Stamp Mismatch", func(t *testing.T) {
		resp, err := invoke(t, func(mutableState *historyi.MockMutableState) {
			executionInfo := &persistencespb.WorkflowExecutionInfo{WorkflowTaskStamp: 1}
			mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).Times(1)
		})()

		require.Nil(t, resp)
		require.Error(t, err)
		var obsoleteErr *serviceerrors.ObsoleteMatchingTask
		require.ErrorAs(t, err, &obsoleteErr)
		require.Contains(t, err.Error(), "Workflow task stamp mismatch")
	})

	t.Run("Task Not Found", func(t *testing.T) {
		resp, err := invoke(t, func(mutableState *historyi.MockMutableState) {
			mutableState.EXPECT().GetWorkflowTaskByID(gomock.Any()).Return(nil).Times(1)
		})()

		require.Nil(t, resp)
		require.Error(t, err)
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Contains(t, err.Error(), "Workflow task not found")
	})

	t.Run("Workflow Completed", func(t *testing.T) {
		resp, err := invoke(t, func(mutableState *historyi.MockMutableState) {
			mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(1)
		})()

		require.Nil(t, resp)
		require.Error(t, err)
		require.Equal(t, consts.ErrWorkflowCompleted, err)
	})
}

func invoke(t *testing.T, modifyMutableState mutableStateModifier) func() (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	testNamespaceID := tests.NamespaceID
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	scheduledEventID := int64(42)

	request := &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId: testNamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ScheduledEventId: scheduledEventID,
		RequestId:        "test-request",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			Identity: "test-worker",
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: "test-tq",
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		},
	}

	mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(testNamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockNamespaceRegistry).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata).AnyTimes()

	mutableState := historyi.NewMockMutableState(ctrl)
	modifyMutableState(mutableState) // must come before setting defaults
	mutableState.EXPECT().GetWorkflowTaskByID(scheduledEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: scheduledEventID,
		Stamp:            0,
	}).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	mockWorkflowContext := historyi.NewMockWorkflowContext(ctrl)
	workflowLease := api.NewWorkflowLease(mockWorkflowContext, func(_ error) {}, mutableState)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLease(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(workflowLease, nil)

	config := &configs.Config{}

	return func() (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
		return Invoke(ctx, request, shardContext, config, nil, nil, consistencyChecker)
	}
}
