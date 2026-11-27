package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestExternalTransferTaskResolved(t *testing.T) {
	t.Parallel()
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), "workflow-id", "run-id")
	signal := &tasks.SignalExecutionTask{WorkflowKey: workflowKey, InitiatedEventID: 10, Version: 123}
	cancel := &tasks.CancelExecutionTask{WorkflowKey: workflowKey, InitiatedEventID: 10, Version: 123}
	for _, tc := range []struct {
		name     string
		mutate   func(*persistencespb.WorkflowMutableState)
		resolved bool
	}{
		{name: "resolved", resolved: true},
		{name: "pending", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.SignalInfos = map[int64]*persistencespb.SignalInfo{10: {Version: 123}}
			s.RequestCancelInfos = map[int64]*persistencespb.RequestCancelInfo{10: {Version: 123}}
		}},
		{name: "closed", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		}, resolved: true},
		{name: "divergent branch", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.ExecutionInfo.VersionHistories.Histories[0].Items[0].Version++
		}},
		{name: "remote behind initiation", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.NextEventId = 10
		}},
		{name: "zombie", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
		}},
		{name: "missing version history", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.ExecutionInfo.VersionHistories = nil
		}},
		{name: "missing execution state", mutate: func(s *persistencespb.WorkflowMutableState) {
			s.ExecutionState = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := resolvedExternalTaskState()
			if tc.mutate != nil {
				tc.mutate(state)
			}
			require.Equal(t, tc.resolved, externalTransferTaskResolved(signal, state))
			require.Equal(t, tc.resolved, externalTransferTaskResolved(cancel, state))
		})
	}
}

func TestVerifyAndSyncExternalTransferTask(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		describeErr error
		syncErr     error
		failover    bool
		wantErr     error
	}{
		{name: "source unavailable", describeErr: serviceerror.NewUnavailable("source unavailable"), wantErr: consts.ErrTaskRetry},
		{name: "execution missing on source", describeErr: serviceerror.NewNotFound("execution missing"), wantErr: consts.ErrTaskRetry},
		{name: "ownership changed after remote verification", failover: true, wantErr: consts.ErrTaskRetry},
		{name: "sync failed", syncErr: serviceerror.NewUnavailable("sync unavailable"), wantErr: consts.ErrTaskDiscarded},
		{name: "sync skipped", syncErr: serviceerror.NewFailedPrecondition("sync unsupported"), wantErr: consts.ErrTaskDiscarded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			shardContext := historyi.NewMockShardContext(ctrl)
			registry := namespace.NewMockRegistry(ctrl)
			metadata := cluster.NewMockMetadata(ctrl)
			remote := adminservicemock.NewMockAdminServiceClient(ctrl)
			bean := client.NewMockBean(ctrl)
			entry := tests.GlobalNamespaceEntry
			source := cluster.TestCurrentClusterName
			local := cluster.TestAlternativeClusterName
			workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), "workflow-id", "run-id")
			task := &tasks.SignalExecutionTask{WorkflowKey: workflowKey, InitiatedEventID: 10, Version: 123}
			shardContext.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
			registry.EXPECT().GetNamespaceByID(tests.NamespaceID).DoAndReturn(func(namespace.ID) (*namespace.Namespace, error) {
				return entry, nil
			}).AnyTimes()
			bean.EXPECT().GetRemoteAdminClient(source).Return(remote, nil)
			remote.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *adminservice.DescribeMutableStateRequest, ...grpc.CallOption) (*adminservice.DescribeMutableStateResponse, error) {
					if tc.failover {
						entry = entry.Clone(namespace.WithActiveCluster(local))
					}
					return &adminservice.DescribeMutableStateResponse{DatabaseMutableState: resolvedExternalTaskState()}, tc.describeErr
				},
			)
			if tc.syncErr != nil {
				shardContext.EXPECT().GetClusterMetadata().Return(metadata)
				metadata.EXPECT().GetCurrentClusterName().Return(local)
				metadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo)
				shardContext.EXPECT().GetRemoteAdminClient(source).Return(remote, nil)
				remote.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(nil, tc.syncErr)
			}
			executor := &transferQueueStandbyTaskExecutor{
				transferQueueTaskExecutorBase: &transferQueueTaskExecutorBase{shardContext: shardContext},
				clusterName:                   local,
				clientBean:                    bean,
			}
			err := executor.verifyAndSyncExternalTransferTask(t.Context(), task, &externalTaskSyncPosition{}, nil, log.NewNoopLogger())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func resolvedExternalTaskState() *persistencespb.WorkflowMutableState {
	return &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING},
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(
				nil, []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(19, 123)},
			)),
		},
		NextEventId: 20,
	}
}
