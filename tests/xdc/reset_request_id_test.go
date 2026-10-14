package xdc

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *FunctionalClustersTestSuite) TestResetRequestIDDeduplicationAfterFailover() {
	for _, explicitRunID := range []bool{true, false} {
		s.Run(fmt.Sprintf("explicit_run_id=%t", explicitRunID), func() {
			namespace := s.createGlobalNamespace()
			active := s.clusters[0].FrontendClient()
			standby := s.clusters[1].FrontendClient()
			workflowID := uuid.NewString()
			taskQueue := &taskqueuepb.TaskQueue{Name: uuid.NewString()}
			started, err := active.StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
				Namespace:           namespace,
				WorkflowId:          workflowID,
				WorkflowType:        &commonpb.WorkflowType{Name: "reset-request-id"},
				TaskQueue:           taskQueue,
				RequestId:           uuid.NewString(),
				WorkflowRunTimeout:  durationpb.New(5 * time.Minute),
				WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			})
			s.Require().NoError(err)
			workflowTask, err := active.PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: namespace, TaskQueue: taskQueue,
			})
			s.Require().NoError(err)
			_, err = active.RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: workflowTask.GetTaskToken(),
			})
			s.Require().NoError(err)

			execution := &commonpb.WorkflowExecution{WorkflowId: workflowID}
			if explicitRunID {
				execution.RunId = started.GetRunId()
			}
			request := &workflowservice.ResetWorkflowExecutionRequest{
				Namespace:                 namespace,
				WorkflowExecution:         execution,
				WorkflowTaskFinishEventId: 4,
				RequestId:                 uuid.NewString(),
				Reason:                    "reset request deduplication after failover",
			}
			first, err := active.ResetWorkflowExecution(testcore.NewContext(), request)
			s.Require().NoError(err)
			resetExecution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: first.GetRunId()}
			await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
				history, err := standby.GetWorkflowExecutionHistory(t.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace: namespace, Execution: resetExecution,
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(history.GetHistory().GetEvents()), 5)
			}, replicationWaitTime, replicationCheckInterval)

			s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)
			for _, rebuild := range []bool{false, true} {
				if rebuild {
					// Rebuild requires history written in the current active cluster's version.
					_, err = standby.SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
						Namespace: namespace, WorkflowExecution: resetExecution, SignalName: "after-failover",
					})
					s.Require().NoError(err)
					_, err = s.clusters[1].AdminClient().RebuildMutableState(testcore.NewContext(), &adminservice.RebuildMutableStateRequest{
						Namespace: namespace, Execution: resetExecution,
					})
					s.Require().NoError(err)
				}
				retry, err := standby.ResetWorkflowExecution(testcore.NewContext(), request)
				s.Require().NoError(err)
				s.Require().Equal(first.GetRunId(), retry.GetRunId(), "rebuild=%t", rebuild)
				described, err := standby.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: namespace, Execution: resetExecution,
				})
				s.Require().NoError(err)
				s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, described.GetWorkflowExecutionInfo().GetStatus())
			}
			request.RequestId = uuid.NewString()
			distinct, err := standby.ResetWorkflowExecution(testcore.NewContext(), request)
			s.Require().NoError(err)
			s.Require().NotEqual(first.GetRunId(), distinct.GetRunId())
		})
	}
}
