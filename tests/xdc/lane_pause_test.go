package xdc

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestLanePauseWithApplyBacklog holds application for one namespace. Its real outstanding
// tasks must pause its lane while a neighboring namespace continues replicating.
func (s *streamBasedReplicationTestSuite) TestLanePauseWithApplyBacklog() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	s.Require().Equal(int32(1), s.numHistoryShards, "both namespaces must share a shard")
	active, standby := s.clusters[0], s.clusters[1]
	neighbor := "lane-pause-neighbor-" + uuid.NewString()
	_, err := active.FrontendClient().RegisterNamespace(s.T().Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace: neighbor, Clusters: s.clusterReplicationConfig(), ActiveClusterName: active.ClusterName(),
		IsGlobalNamespace: true, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)
	s.Require().NoError(s.waitUntilNamespaceReplicated(s.T().Context(), neighbor))
	stopIsolation := s.isolateSuiteNamespace(1000)
	defer stopIsolation()
	s.startWorkflowsUntilLaneSendsTasks()

	var held atomic.Bool
	var refused atomic.Int64
	held.Store(true)
	defer held.Store(false)
	standby.InjectHook(s.T(), testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor,
		func(task *replicationspb.ReplicationTask, execute func() error) error {
			if task.GetRawTaskInfo().GetNamespaceId() == s.namespaceID && held.Load() {
				refused.Add(1)
				return serviceerror.NewUnavailable("namespace application held by lane pause test")
			}
			return execute()
		}), testhooks.GlobalScope)
	standby.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationReceiverLaneMaxOutstandingTaskCount, 2)
	var workflowIDs []string
	for range 20 {
		workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(s.T().Context()))
	}
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.GreaterOrEqual(t, refused.Load(), int64(3), "application must have encountered real pending tasks")
		require.True(t, s.sawLaneAckMatching(active, func(state *replicationspb.SyncReplicationState) bool {
			for _, lane := range state.GetLaneStates() {
				if lane.GetFlowControlCommand() == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
					return true
				}
			}
			return false
		}))
	}, 30*time.Second, 100*time.Millisecond)

	// Receiving another ACK proves the sender finished processing the one carrying PAUSE.
	acks := s.ackCount(active)
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.Greater(t, s.ackCount(active), acks)
	}, 30*time.Second, 100*time.Millisecond)
	// Work created after PAUSE must remain on the sender while default traffic progresses.
	pausedWorkflowID := s.startLaneTestWorkflow(s.T().Context())
	workflowIDs = append(workflowIDs, pausedWorkflowID)
	_, err = active.FrontendClient().StartWorkflowExecution(s.T().Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId: uuid.NewString(), Namespace: neighbor, WorkflowId: "neighbor-workflow",
		WorkflowType: &commonpb.WorkflowType{Name: "neighbor"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "neighbor"},
	})
	s.Require().NoError(err)
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		_, err := standby.FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: neighbor, Execution: &commonpb.WorkflowExecution{WorkflowId: "neighbor-workflow"},
		})
		require.NoError(t, err)
	}, 20*time.Second, 100*time.Millisecond)
	acks = s.ackCount(active)
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.GreaterOrEqual(t, s.ackCount(active), acks+3)
	}, 30*time.Second, 100*time.Millisecond)
	s.Require().False(s.sawLaneMessage(active, func(message *replicationspb.WorkflowReplicationMessages) bool {
		for _, task := range message.GetReplicationTasks() {
			if task.GetRawTaskInfo().GetWorkflowId() == pausedWorkflowID {
				return true
			}
		}
		return false
	}), "a paused lane sent newly created work")
	s.Require().False(s.sawLaneAckMatching(active, func(state *replicationspb.SyncReplicationState) bool {
		return state.GetHighPriorityState().GetFlowControlCommand() == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	}), "the isolated backlog paused the default lane")

	// Discard earlier RESUMEs so the next one must follow releasing application.
	active.GetReplicationStreamRecorder().Clear()
	held.Store(false)
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.True(t, s.sawLaneAckMatching(active, func(state *replicationspb.SyncReplicationState) bool {
			for _, lane := range state.GetLaneStates() {
				if lane.GetFlowControlCommand() == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME {
					return true
				}
			}
			return false
		}))
	}, 30*time.Second, 100*time.Millisecond)
	for _, id := range workflowIDs {
		execution := &commonpb.WorkflowExecution{WorkflowId: id}
		source, err := retirementHistory(s.T().Context(), active.FrontendClient(), s.namespaceName, execution)
		s.Require().NoError(err)
		await.Require(s.T().Context(), s.T(), func(t *await.T) {
			target, err := retirementHistory(t.Context(), standby.FrontendClient(), s.namespaceName, execution)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, source, target)
		}, 30*time.Second, 100*time.Millisecond)
	}
}
