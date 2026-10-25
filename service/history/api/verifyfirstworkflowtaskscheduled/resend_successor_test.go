package verifyfirstworkflowtaskscheduled

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestResendChildAndVerify_RetainedSuccessor(t *testing.T) {
	for _, matching := range []bool{true, false} {
		name := "matching successor"
		if !matching {
			name = "reused workflow ID"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			shard := historyi.NewMockShardContext(ctrl)
			registry := namespace.NewMockRegistry(ctrl)
			metadata := cluster.NewMockMetadata(ctrl)
			remote := adminservicemock.NewMockAdminServiceClient(ctrl)
			checker := api.NewMockWorkflowConsistencyChecker(ctrl)
			entry := tests.GlobalNamespaceEntry
			shard.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
			shard.EXPECT().GetClusterMetadata().Return(metadata).AnyTimes()
			registry.EXPECT().GetNamespaceByID(entry.ID()).Return(entry, nil).AnyTimes()
			metadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()
			metadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
			shard.EXPECT().GetRemoteAdminClient(cluster.TestCurrentClusterName).Return(remote, nil).AnyTimes()
			original := &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "first"}
			transition := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 2}
			histories := &historyspb.VersionHistories{}
			remote.EXPECT().SyncWorkflowState(gomock.Any(), protomock.Eq(&adminservice.SyncWorkflowStateRequest{
				NamespaceId: entry.ID().String(), Execution: original, ArchetypeId: chasm.WorkflowArchetypeID,
				VersionedTransition: transition, VersionHistories: histories,
				TargetClusterId: int32(cluster.TestAlternativeClusterInitialFailoverVersion),
			})).Return(nil, serviceerror.NewNotFound("first run expired"))
			firstRunID := "first"
			if !matching {
				firstRunID = "unrelated"
			}
			remote.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
				Namespace: entry.Name().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: "child"},
				Archetype: chasm.WorkflowArchetype, SkipForceReload: true,
			})).Return(&adminservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{
				ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "successor", FirstExecutionRunId: firstRunID},
			}}, nil)
			if matching {
				artifact := &replicationspb.VersionedTransitionArtifact{}
				// Pull the exact successor without passing replication hints from the original run.
				remote.EXPECT().SyncWorkflowState(gomock.Any(), protomock.Eq(&adminservice.SyncWorkflowStateRequest{
					NamespaceId: entry.ID().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "successor"},
					ArchetypeId: chasm.WorkflowArchetypeID, TargetClusterId: int32(cluster.TestAlternativeClusterInitialFailoverVersion),
				})).Return(&adminservice.SyncWorkflowStateResponse{VersionedTransitionArtifact: artifact}, nil)
				engine := historyi.NewMockEngine(ctrl)
				shard.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
				engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), chasm.WorkflowArchetypeID, artifact, cluster.TestCurrentClusterName).Return(nil)
				checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), definition.NewWorkflowKey(entry.ID().String(), "child", "first"), locks.PriorityLow).Return(nil, serviceerror.NewNotFound("first expired"))
				ms := historyi.NewMockMutableState(ctrl)
				ms.EXPECT().GetFirstRunID(gomock.Any()).Return("first", nil)
				ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: "successor"})
				lease := ndc.NewMockWorkflow(ctrl)
				lease.EXPECT().GetMutableState().Return(ms)
				lease.EXPECT().GetReleaseFn().Return(func(error) {})
				checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), definition.NewWorkflowKey(entry.ID().String(), "child", ""), locks.PriorityLow).Return(lease, nil)
			}
			err := resendChildAndVerify(t.Context(), &historyservice.VerifyFirstWorkflowTaskScheduledRequest{NamespaceId: entry.ID().String(), WorkflowExecution: original}, checker, shard, entry.ID(), transition, histories, serviceerror.NewNotFound("missing"), false)
			if matching {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, consts.ErrWorkflowNotReady)
			}
		})
	}
}
