package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestStandbyTransferTaskPostActionTaskDiscarded_CloseExecutionLogsParentWorkflow(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("parent-namespace-id", "parent-workflow-id", "parent-run-id")
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Warn(
		"Discarding standby transfer task due to task being pending for too long.",
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Do(func(_ string, logTags ...tag.Tag) {
		require.Equal(t, map[string]any{
			"parent-namespace-id": workflowKey.NamespaceID,
			"parent-workflow-id":  workflowKey.WorkflowID,
			"parent-run-id":       workflowKey.RunID,
		}, logTagValues(logTags[1:]))
	})

	err := standbyTransferTaskPostActionTaskDiscarded(
		context.Background(),
		&tasks.CloseExecutionTask{},
		&verifyCompletionRecordedPostActionInfo{parentWorkflowKey: &workflowKey},
		logger,
	)
	require.ErrorIs(t, err, consts.ErrTaskDiscarded)
}

func TestStandbyTransferTaskPostActionTaskDiscarded_StartChildExecutionLogsChildWorkflow(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("child-namespace-id", "child-workflow-id", "child-run-id")
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Warn(
		"Discarding standby transfer task due to task being pending for too long.",
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Do(func(_ string, logTags ...tag.Tag) {
		require.Equal(t, map[string]any{
			"child-namespace-id": workflowKey.NamespaceID,
			"child-workflow-id":  workflowKey.WorkflowID,
			"child-run-id":       workflowKey.RunID,
		}, logTagValues(logTags[1:]))
	})

	err := standbyTransferTaskPostActionTaskDiscarded(
		context.Background(),
		&tasks.StartChildExecutionTask{},
		&startChildExecutionPostActionInfo{childWorkflowKey: &workflowKey},
		logger,
	)
	require.ErrorIs(t, err, consts.ErrTaskDiscarded)
}

func logTagValues(logTags []tag.Tag) map[string]any {
	values := make(map[string]any, len(logTags))
	for _, logTag := range logTags {
		zapTag := logTag.(tag.ZapTag)
		values[zapTag.Key()] = zapTag.Value()
	}
	return values
}
