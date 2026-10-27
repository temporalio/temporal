package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestStandbyTransferTaskPostActionTaskDiscarded_CloseExecutionLogsParentWorkflow(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("parent-namespace-id", "parent-workflow-id", "parent-run-id")
	logger := log.NewMockLogger(gomock.NewController(t))
	eventDetails := make(map[string]any)
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
		eventDetails,
	)
	require.ErrorIs(t, err, consts.ErrTaskDiscarded)
	require.Equal(t, map[string]any{
		"parent_namespace_id": workflowKey.NamespaceID,
		"parent_workflow_id":  workflowKey.WorkflowID,
		"parent_run_id":       workflowKey.RunID,
	}, eventDetails)
}

func TestStandbyTransferTaskPostActionTaskDiscarded_StartChildExecutionLogsChildWorkflow(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("child-namespace-id", "child-workflow-id", "child-run-id")
	logger := log.NewMockLogger(gomock.NewController(t))
	eventDetails := make(map[string]any)
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
		eventDetails,
	)
	require.ErrorIs(t, err, consts.ErrTaskDiscarded)
	require.Equal(t, map[string]any{
		"child_namespace_id": workflowKey.NamespaceID,
		"child_workflow_id":  workflowKey.WorkflowID,
		"child_run_id":       workflowKey.RunID,
	}, eventDetails)
}

func TestExecutionExistsOnSource(t *testing.T) {
	workflowKey := definition.NewWorkflowKey(tests.StandbyNamespaceID.String(), "workflow-id", "run-id")
	testCases := []struct {
		name           string
		namespaceEntry *namespace.Namespace
		describeErr    error
		expectDescribe bool
		expectedExists bool
		expectedErr    error
	}{
		{
			name:           "ExecutionExistsOnSource",
			namespaceEntry: tests.GlobalStandbyNamespaceEntry,
			expectDescribe: true,
			expectedExists: true,
		},
		{
			name:           "ExecutionNotFoundOnSource",
			namespaceEntry: tests.GlobalStandbyNamespaceEntry,
			describeErr:    serviceerror.NewNotFound("workflow not found"),
			expectDescribe: true,
			expectedExists: false,
		},
		{
			name:           "SourceInHandover",
			namespaceEntry: tests.GlobalStandbyNamespaceEntry,
			describeErr:    serviceerror.NewUnavailable(consts.ErrNamespaceHandover.Error()),
			expectDescribe: true,
			expectedErr:    consts.ErrNamespaceHandover,
		},
		{
			name:           "UnexpectedErrorFromSource",
			namespaceEntry: tests.GlobalStandbyNamespaceEntry,
			describeErr:    serviceerror.NewUnavailable("some random error"),
			expectDescribe: true,
			expectedExists: true,
		},
		{
			name:           "NamespaceTurnedActive",
			namespaceEntry: tests.GlobalNamespaceEntry,
			expectedErr:    consts.ErrTaskRetry,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			registry.EXPECT().GetNamespaceByID(namespace.ID(workflowKey.NamespaceID)).Return(tc.namespaceEntry, nil).AnyTimes()
			chasmRegistry := chasm.NewRegistry(log.NewNoopLogger())
			require.NoError(t, chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
			clientBean := client.NewMockBean(ctrl)
			if tc.expectDescribe {
				remoteAdminClient := adminservicemock.NewMockAdminServiceClient(ctrl)
				clientBean.EXPECT().GetRemoteAdminClient(cluster.TestAlternativeClusterName).Return(remoteAdminClient, nil)
				remoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
					Namespace: tests.StandbyNamespace.String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: workflowKey.WorkflowID,
						RunId:      workflowKey.RunID,
					},
					Archetype:       chasm.WorkflowArchetype,
					SkipForceReload: true,
				})).Return(&adminservice.DescribeMutableStateResponse{}, tc.describeErr)
			}

			exists, err := executionExistsOnSource(
				context.Background(),
				workflowKey,
				chasm.WorkflowArchetypeID,
				log.NewNoopLogger(),
				cluster.TestCurrentClusterName,
				clientBean,
				registry,
				chasmRegistry,
			)
			require.ErrorIs(t, err, tc.expectedErr)
			if tc.expectedErr == nil {
				require.Equal(t, tc.expectedExists, exists)
			}
		})
	}
}

func logTagValues(logTags []tag.Tag) map[string]any {
	values := make(map[string]any, len(logTags))
	for _, logTag := range logTags {
		zapTag := logTag.(tag.ZapTag)
		values[zapTag.Key()] = zapTag.Value()
	}
	return values
}
