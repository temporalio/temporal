package xdc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
)

// TestLaneRetirementWithContinuousWrites keeps one workflow writing on the source shard
// throughout retirement, then compares its complete history on both clusters.
func (s *streamBasedReplicationTestSuite) TestLaneRetirementWithContinuousWrites() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	active, standby := s.clusters[0], s.clusters[1]
	stopIsolation := active.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationLaneIsolation, []dynamicconfig.ReplicationLaneIsolationEntry{{
		RemoteCluster: standby.ClusterName(), NamespaceID: s.namespaceID, TasksPerSecond: 1,
	}})
	workflowID := "lane-retirement-" + uuid.NewString()
	started, err := active.FrontendClient().StartWorkflowExecution(s.T().Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId: uuid.NewString(), Namespace: s.namespaceName, WorkflowId: workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "lane-retirement"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "lane-retirement"},
	})
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: started.GetRunId()}
	type writerResult struct {
		signals int64
		err     error
	}
	var written atomic.Int64
	var exited atomic.Pointer[writerResult]
	done := make(chan writerResult, 1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWriter := func() { stopOnce.Do(func() { close(stop) }) }
	writerCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	finished := false
	s.T().Cleanup(func() {
		stopWriter()
		cancel()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				s.T().Error("continuous signal writer did not stop")
			}
		}
	})
	go func() {
		result := writerResult{}
		defer func() { exited.Store(&result); done <- result }()
		// This pacing supplies ongoing traffic; ordering is established by the recorded
		// lane messages and the final histories, rather than by a sleep.
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-writerCtx.Done():
				result.err = writerCtx.Err()
				return
			case <-ticker.C:
			}
			_, result.err = active.FrontendClient().SignalWorkflowExecution(writerCtx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: s.namespaceName, WorkflowExecution: execution,
				SignalName: "write", Input: payloads.EncodeString(fmt.Sprint(result.signals)),
				RequestId: uuid.NewString(),
			})
			if result.err != nil {
				return
			}
			result.signals++
			written.Store(result.signals)
		}
	}()

	var laneID string
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.Nil(t, exited.Load(), "the signal writer exited before isolation")
		for _, message := range retirementMessages(active) {
			for _, task := range message.GetReplicationTasks() {
				if task.GetRawTaskInfo().GetWorkflowId() == workflowID && message.GetLaneInfo().GetLaneId() != "" {
					laneID = message.GetLaneInfo().GetLaneId()
				}
			}
		}
		require.NotEmpty(t, laneID, "the workflow was never sent on a lane")
	}, 45*time.Second, 100*time.Millisecond)
	beforeRetirement := written.Load()
	stopIsolation()
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		require.Nil(t, exited.Load(), "the signal writer exited before retirement")
		retired := false
		for _, message := range retirementMessages(active) {
			if message.GetLaneInfo().GetLaneId() == laneID && message.GetLaneInfo().GetRetire() {
				retired = true
			}
		}
		require.True(t, retired, "the lane did not retire while its shard kept writing")
		require.Greater(t, written.Load(), beforeRetirement)
	}, 45*time.Second, 100*time.Millisecond)
	stopWriter()
	result := await.Rcv(s.T(), done)
	finished = true
	s.NoError(result.err)
	s.Positive(result.signals)
	source, err := retirementHistory(s.T().Context(), active.FrontendClient(), s.namespaceName, execution)
	s.NoError(err)
	var signalCount int64
	for _, event := range source.Events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			signalCount++
		}
	}
	s.Equal(result.signals, signalCount)
	await.Require(s.T().Context(), s.T(), func(t *await.T) {
		target, err := retirementHistory(t.Context(), standby.FrontendClient(), s.namespaceName, execution)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, source, target)
	}, 45*time.Second, 100*time.Millisecond)
}

func retirementMessages(cluster *testcore.TestCluster) []*replicationspb.WorkflowReplicationMessages {
	var messages []*replicationspb.WorkflowReplicationMessages
	for _, captured := range cluster.GetReplicationStreamRecorder().GetMessages() {
		for _, payload := range []proto.Message{captured.Request, captured.Response} {
			response, ok := payload.(interface {
				GetMessages() *replicationspb.WorkflowReplicationMessages
			})
			if ok {
				messages = append(messages, response.GetMessages())
			}
		}
	}
	return messages
}

func retirementHistory(ctx context.Context, client workflowservice.WorkflowServiceClient, namespace string, execution *commonpb.WorkflowExecution) (*historypb.History, error) {
	history := &historypb.History{}
	var token []byte
	for {
		response, err := client.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace, Execution: execution, NextPageToken: token, MaximumPageSize: 1000,
		})
		if err != nil {
			return nil, err
		}
		history.Events = append(history.Events, response.GetHistory().GetEvents()...)
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return history, nil
		}
	}
}
