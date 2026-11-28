package xdc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/protobuf/proto"
)

// TestResetAfterConflictPreservesWinningHistory checks that a replicated reset
// inherits the winning prefix after the original run has switched branches.
func (s *FunctionalClustersTestSuite) TestResetAfterConflictPreservesWinningHistory() {
	if !s.enableTransitionHistory {
		s.T().Skip("requires state-based replication")
	}
	for _, tc := range []struct {
		name         string
		coldSnapshot bool
	}{
		{name: "FirstSync"},
		{name: "ColdSnapshot", coldSnapshot: true},
	} {
		s.Run(tc.name, func() { s.resetAfterConflictPreservesWinningHistory(tc.coldSnapshot) })
	}
}

func (s *FunctionalClustersTestSuite) resetAfterConflictPreservesWinningHistory(coldSnapshot bool) {
	ctx, cancel := context.WithTimeout(s.T().Context(), 120*time.Second)
	defer cancel()
	ns := s.createBufferedEventsNamespace(ctx)
	queue := &taskqueuepb.TaskQueue{Name: "reset-conflict-" + uuid.NewString()}
	coldSnapshotFrom := func(source int, task *replicationspb.ReplicationTask) error {
		attrs := task.GetSyncVersionedTransitionTaskAttributes()
		if attrs == nil || attrs.WorkflowId != queue.Name {
			return nil
		}
		response, err := s.clusters[source].AdminClient().SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
			NamespaceId:     ns.ID,
			Execution:       &commonpb.WorkflowExecution{WorkflowId: attrs.WorkflowId, RunId: attrs.RunId},
			TargetClusterId: int32(2 - source), ArchetypeId: chasm.WorkflowArchetypeID,
		})
		if err != nil {
			return err
		}
		artifact := response.VersionedTransitionArtifact
		if artifact.IsFirstSync || len(artifact.EventBatches) != 0 {
			return errors.New("expected a non-first-sync snapshot without events")
		}
		attrs.VersionedTransitionArtifact = artifact
		return nil
	}
	removeInitialHook := func() {}
	if coldSnapshot {
		// Resync preserves source branch IDs, so the source ancestor can later
		// refer to a losing history on the destination.
		removeInitialHook = s.clusters[1].InjectHook(s.T(), testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor,
			func(task *replicationspb.ReplicationTask, execute func() error) error {
				if err := coldSnapshotFrom(0, task); err != nil {
					return err
				}
				return execute()
			}), testhooks.GlobalScope)
	}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace: ns.Name, WorkflowID: queue.Name, TaskQueue: queue,
	})
	first := s.pollBufferedEventsWorkflowTask(ctx, 0, ns.Name, queue)
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task: first, Commands: []*commandpb.Command{scheduleActivityCommand("activity", queue, 2*time.Minute)},
	})
	await.Require(ctx, s.T(), func(t *await.T) {
		response, err := s.clusters[1].FrontendClient().GetWorkflowExecutionHistory(t.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns.Name, Execution: execution,
		})
		require.NoError(t, err)
		require.Len(t, response.History.Events, 5)
	}, replicationWaitTime, replicationCheckInterval)
	removeInitialHook()
	commonHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns.Name, execution)
	s.EqualHistoryEvents(`
1 WorkflowExecutionStarted
2 WorkflowTaskScheduled
3 WorkflowTaskStarted
4 WorkflowTaskCompleted
5 ActivityTaskScheduled`, commonHistory)

	// Activity results and markers are not reapplied from the losing branch.
	// Keep its replication blocked so the winner is written from the common prefix.
	s.blockReplicationForWorkflow(1, execution.WorkflowId)
	toOldActive := s.blockReplicationForWorkflow(0, execution.WorkflowId, func(task *replicationspb.ReplicationTask) error {
		attrs := task.GetSyncVersionedTransitionTaskAttributes()
		if !coldSnapshot || attrs == nil || attrs.RunId == execution.RunId {
			return nil
		}
		// Fetch an eventless snapshot so the receiver backfills the reset history.
		return coldSnapshotFrom(1, task)
	})
	activity := s.pollBufferedActivityTask(ctx, ns.Name, queue)
	s.completeActivityTask(ctx, activity)
	losingTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns.Name, queue)
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task: losingTask,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{MarkerName: "losing-marker"},
			},
		}},
	})

	s.failoverToNewActiveCluster(ctx, ns.Name)
	for i := range 3 {
		s.writeSignalOnNewActive(ctx, activeClusterSignal{
			Namespace: ns, Execution: execution, SignalName: fmt.Sprintf("winner-%d", i),
		})
	}
	winnerTask := s.pollBufferedEventsWorkflowTask(ctx, 1, ns.Name, queue)
	_, err := s.clusters[1].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: winnerTask.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{MarkerName: "winning-marker"},
			},
		}},
	})
	s.NoError(err)
	winnerHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns.Name, execution)

	// Establish convergence of the original run before creating the reset.
	for {
		s.releaseReplicationTask(ctx, toOldActive)
		local := s.getWorkflowHistory(ctx, s.T(), 0, ns.Name, execution)
		if len(local) == len(winnerHistory) && proto.Equal(local[len(local)-1], winnerHistory[len(winnerHistory)-1]) {
			s.ProtoEqual(&historypb.History{Events: winnerHistory}, &historypb.History{Events: local})
			break
		}
	}
	reset, err := s.clusters[1].FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: ns.Name, WorkflowExecution: execution, RequestId: uuid.NewString(),
		Reason: "reset after conflict", WorkflowTaskFinishEventId: winnerTask.StartedEventId + 1,
	})
	s.NoError(err)
	resetExecution := &commonpb.WorkflowExecution{WorkflowId: execution.WorkflowId, RunId: reset.RunId}
	// The original run's termination can precede the reset snapshot on the stream.
	for {
		s.releaseReplicationTask(ctx, toOldActive)
		_, describeErr := s.clusters[0].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: ns.Name, Execution: resetExecution, Archetype: chasm.WorkflowArchetype,
		})
		if describeErr == nil {
			break
		}
	}
	expected := s.getWorkflowHistory(ctx, s.T(), 1, ns.Name, resetExecution)
	actual := s.getWorkflowHistory(ctx, s.T(), 0, ns.Name, resetExecution)
	s.ProtoEqual(&historypb.History{Events: expected}, &historypb.History{Events: actual})
}
