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
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The original child run is deleted explicitly instead of waiting out retention.
// Replication gates hold all later updates, so a successful resend cannot be
// mistaken for ordinary replication catching up.
func (s *parentChildXDCTestSuite) TestStandbyVerifiesRetainedChildSuccessor() {
	for _, mechanism := range []string{"continue_backoff", "continue_immediate", "retry", "cron", "reset", "continue_reset"} {
		s.Run(mechanism, func() { s.runRetainedChildSuccessor(mechanism, false, false) })
	}
}

func (s *parentChildXDCTestSuite) TestStandbyResendsRetainedChildSuccessor() {
	for _, mechanism := range []string{"continue_backoff", "continue_immediate", "retry", "cron", "reset", "continue_reset"} {
		s.Run(mechanism, func() { s.runRetainedChildSuccessor(mechanism, true, false) })
	}
}

func (s *parentChildXDCTestSuite) TestStandbyDoesNotSilentlyDiscardRetainedChildSuccessor() {
	s.runRetainedChildSuccessor("continue_backoff", true, true)
}

func (s *parentChildXDCTestSuite) runRetainedChildSuccessor(mechanism string, remote, discard bool) {
	var firstRunID string
	steps := []parentChildScenarioStep{
		setStandbyClusterDelay(initialStandbyCluster, 0),
		setStandbyTaskResendDelay(initialStandbyCluster, enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, 0),
	}
	if remote && !discard {
		steps = append(steps, enableChildWorkflowResend(initialStandbyCluster))
	}
	if discard {
		steps = append(steps, setStandbyTaskDiscardDelay(initialStandbyCluster, enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, 0))
	}
	steps = append(steps,
		startParentWorkflow(),
		applyReplicationThroughTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		startChildWithSuccessorPolicy(mechanism),
		waitForWorkflowEventOnCluster(initialActiveCluster, parentWorkflow, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED),
	)
	if !remote {
		steps = append(steps, applyReplicationThroughTaskContainingEvent(initialStandbyCluster, childWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED))
	}
	steps = append(steps, advanceChildToSuccessor(mechanism, &firstRunID))
	if !remote {
		steps = append(steps, applyRetainedChildSuccessor())
	}
	steps = append(steps, deleteOriginalChildRun(initialActiveCluster, &firstRunID))
	if !remote {
		steps = append(steps, deleteOriginalChildRun(initialStandbyCluster, &firstRunID))
	}
	steps = append(steps,
		parentChildScenarioStep{name: "verify before recovery", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
			if mechanism == "continue_backoff" || mechanism == "retry" || mechanism == "cron" {
				// These successors exist before their backoff timer schedules a task.
				// Verification must still accept them, and failover below must make them runnable.
				state, err := r.workflowMutableState(ctx, initialActiveCluster, childWorkflow)
				if err != nil {
					return err
				}
				s.Require().Zero(state.GetExecutionInfo().GetWorkflowTaskScheduledEventId())
			}
			_, err := r.suite.clusters[int(initialStandbyCluster)].HistoryClient().VerifyFirstWorkflowTaskScheduled(ctx, childFirstRunVerification(r, firstRunID, false))
			if remote {
				s.Require().ErrorAs(err, new(*serviceerror.NotFound))
				return r.confirmWorkflowMissing(ctx, initialStandbyCluster, childWorkflow)
			}
			s.Require().NoError(err)
			s.Require().Empty(r.metricCaptures[int(initialStandbyCluster)].capture.SnapshotMetric(metrics.ChildWorkflowResendAttempts.Name()))
			return nil
		}},
		applyReplicationThroughTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED),
	)
	if discard {
		s.runParentChildScenario(parentChildScenario{steps: steps, expectations: []parentChildExpectation{taskWasDiscardedOnCluster(initialStandbyCluster, metrics.TaskTypeTransferStandbyTaskStartChildExecution)}})
		return
	}
	steps = append(steps, parentChildScenarioStep{name: "wait for local verification of the expected first run", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		await.Require(ctx, s.T(), func(t *await.T) {
			_, err := s.clusters[int(initialStandbyCluster)].HistoryClient().VerifyFirstWorkflowTaskScheduled(t.Context(), childFirstRunVerification(r, firstRunID, false))
			require.NoError(t, err)
		}, testTimeout, replicationCheckInterval)
		state, err := r.workflowMutableState(ctx, initialStandbyCluster, childWorkflow)
		if err != nil {
			return err
		}
		s.Require().Equal(firstRunID, state.GetExecutionState().GetFirstExecutionRunId())
		parent, err := r.workflowMutableState(ctx, initialStandbyCluster, parentWorkflow)
		if err != nil {
			return err
		}
		s.Require().Len(parent.GetChildExecutionInfos(), 1)
		for _, child := range parent.GetChildExecutionInfos() {
			s.Require().Equal(firstRunID, child.GetStartedRunId())
		}
		if remote {
			return r.requireCapturedMetric(initialStandbyCluster, metrics.ChildWorkflowResendAttempts.Name(), nil)
		}
		return nil
	}}, forceFailoverNamespaceTo(initialStandbyCluster), parentChildScenarioStep{name: "run recovered successor and report its final result", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		poller := taskpoller.New(s.T(), r.activeCluster().FrontendClient(), r.namespace)
		_, err := poller.PollAndHandleWorkflowTask(r.childTestVars, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Require().Equal(r.childRunID, task.GetWorkflowExecution().GetRunId())
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		}, taskpoller.WithTimeout(testTimeout))
		if err != nil {
			return err
		}
		// Termination ends cron and retry chains as well, letting every mechanism
		// assert that the final successor reports back to the waiting parent.
		_, err = r.activeCluster().FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{Namespace: r.namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: r.childID, RunId: r.childRunID}, Reason: "finish successor recovery test"})
		return err
	}})
	s.runParentChildScenario(parentChildScenario{steps: steps, expectations: []parentChildExpectation{{name: "parent records termination of the recovered successor", check: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		events, err := r.workflowHistoryOnCluster(ctx, initialStandbyCluster, parentWorkflow)
		if err != nil {
			return err
		}
		event := findHistoryEvent(events, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, nil)
		if event == nil {
			return errors.New("parent has not recorded child termination")
		}
		s.Require().Equal(r.childRunID, event.GetChildWorkflowExecutionTerminatedEventAttributes().GetWorkflowExecution().GetRunId())
		state, err := r.workflowMutableState(ctx, initialStandbyCluster, parentWorkflow)
		if err != nil {
			return err
		}
		s.Require().Empty(state.GetChildExecutionInfos())
		s.Require().Empty(r.metricCaptures[int(initialStandbyCluster)].capture.SnapshotMetric(metrics.TaskDiscarded.Name()))
		return nil
	}}}})
}

func childFirstRunVerification(r *parentChildScenarioRuntime, firstRunID string, resend bool) *historyservice.VerifyFirstWorkflowTaskScheduledRequest {
	return &historyservice.VerifyFirstWorkflowTaskScheduledRequest{NamespaceId: r.namespaceID, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: r.childID, RunId: firstRunID}, ResendChild: resend}
}

func startChildWithSuccessorPolicy(mechanism string) parentChildScenarioStep {
	return parentChildScenarioStep{name: "start child with " + mechanism, run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		if mechanism == "continue_immediate" || mechanism == "continue_reset" {
			for _, cluster := range r.suite.clusters {
				r.cleanups = append(r.cleanups, cluster.OverrideDynamicConfig(r.suite.T(), dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0)))
			}
		}
		attr := &commandpb.StartChildWorkflowExecutionCommandAttributes{WorkflowId: r.childID, WorkflowType: &commonpb.WorkflowType{Name: "child-workflow"}, TaskQueue: r.childTestVars.TaskQueue(), WorkflowRunTimeout: durationpb.New(5 * time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second), ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON}
		if mechanism == "retry" {
			attr.RetryPolicy = &commonpb.RetryPolicy{InitialInterval: durationpb.New(20 * time.Second), BackoffCoefficient: 1, MaximumAttempts: 2}
		}
		if mechanism == "cron" {
			attr.CronSchedule = "@every 20s"
		}
		poller := taskpoller.New(r.suite.T(), r.activeCluster().FrontendClient(), r.namespace)
		_, err := poller.PollAndHandleWorkflowTask(r.parentTestVars, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: attr}}}}, nil
		}, taskpoller.WithTimeout(testTimeout))
		return err
	}}
}

func advanceChildToSuccessor(mechanism string, firstRunID *string) parentChildScenarioStep {
	return parentChildScenarioStep{name: "advance child through " + mechanism, run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		resp, err := r.activeCluster().FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: r.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: r.childID}})
		if err != nil {
			return err
		}
		*firstRunID = resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		r.childRunID = *firstRunID
		poller := taskpoller.New(r.suite.T(), r.activeCluster().FrontendClient(), r.namespace)
		_, err = poller.PollAndHandleWorkflowTask(r.childTestVars, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			var cmd *commandpb.Command
			switch mechanism {
			case "continue_backoff", "continue_immediate", "continue_reset":
				attr := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{WorkflowType: &commonpb.WorkflowType{Name: "child-workflow"}, TaskQueue: r.childTestVars.TaskQueue(), WorkflowRunTimeout: durationpb.New(5 * time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second)}
				if mechanism == "continue_backoff" {
					attr.BackoffStartInterval = durationpb.New(20 * time.Second)
				}
				cmd = &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: attr}}
			case "retry":
				cmd = &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{Message: "retry", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "retryable"}}}}}}
			case "cron":
				cmd = &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}
			case "reset":
			default:
				return nil, fmt.Errorf("unexpected successor mechanism %s", mechanism)
			}
			request := &workflowservice.RespondWorkflowTaskCompletedRequest{}
			if cmd != nil {
				request.Commands = []*commandpb.Command{cmd}
			}
			return request, nil
		}, taskpoller.WithTimeout(testTimeout))
		if err != nil {
			return err
		}
		if mechanism == "continue_reset" {
			resp, err = r.activeCluster().FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: r.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: r.childID}})
			if err != nil {
				return err
			}
			r.childRunID = resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			_, err = poller.PollAndHandleWorkflowTask(r.childTestVars, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			}, taskpoller.WithTimeout(testTimeout))
			if err != nil {
				return err
			}
		}
		if mechanism == "reset" || mechanism == "continue_reset" {
			if err = r.resetChildWorkflow(ctx); err != nil {
				return err
			}
		}
		resp, err = r.activeCluster().FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: r.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: r.childID}})
		if err != nil {
			return err
		}
		r.childRunID = resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		if r.childRunID == *firstRunID {
			return errors.New("child did not advance to a successor")
		}
		return nil
	}}
}

func applyRetainedChildSuccessor() parentChildScenarioStep {
	return parentChildScenarioStep{name: "replicate child through the selected successor", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		for {
			task, err := r.gates[int(initialStandbyCluster)].nextForWorkflow(ctx, r.childID)
			if err != nil {
				return err
			}
			if err = task.apply(); err != nil {
				return err
			}
			_, err = r.workflowMutableState(ctx, initialStandbyCluster, childWorkflow)
			if err == nil {
				return nil
			}
			if !common.IsNotFoundError(err) {
				return err
			}
		}
	}}
}

func deleteOriginalChildRun(cluster parentChildCluster, firstRunID *string) parentChildScenarioStep {
	return parentChildScenarioStep{name: fmt.Sprintf("delete original child run on %s", cluster), run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		execution := &commonpb.WorkflowExecution{WorkflowId: r.childID, RunId: *firstRunID}
		_, err := r.suite.clusters[int(cluster)].HistoryClient().DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{NamespaceId: r.namespaceID, WorkflowExecution: execution})
		if err != nil {
			return err
		}
		await.Require(ctx, r.suite.T(), func(t *await.T) {
			_, err := r.suite.clusters[int(cluster)].HistoryClient().DescribeMutableState(t.Context(), &historyservice.DescribeMutableStateRequest{NamespaceId: r.namespaceID, Execution: execution})
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
		}, testTimeout, replicationCheckInterval)
		return nil
	}}
}

// The passive parent's first child record intentionally lags behind the child's
// current execution. Neither local verification nor remote recovery may mistake
// a reused workflow ID for that pending chain.
func (s *parentChildXDCTestSuite) TestStandbyRejectsReusedChildWorkflowIDUntilParentCatchesUp() {
	var firstRunID string
	s.runParentChildScenario(parentChildScenario{steps: []parentChildScenarioStep{
		setStandbyClusterDelay(initialStandbyCluster, 0), enableChildWorkflowResend(initialStandbyCluster),
		startParentWorkflow(), applyReplicationThroughTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		startChildWithSuccessorPolicy("continue_immediate"), applyReplicationThroughTaskContainingEvent(initialStandbyCluster, childWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		advanceChildToSuccessor("continue_immediate", &firstRunID), applyRetainedChildSuccessor(),
		deleteOriginalChildRun(initialActiveCluster, &firstRunID), deleteOriginalChildRun(initialStandbyCluster, &firstRunID),
		applyReplicationThroughTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED),
		completeChildWorkflowTask(),
		waitForWorkflowEventOnCluster(initialActiveCluster, parentWorkflow, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED),
		parentChildScenarioStep{name: "reuse child workflow ID for a different chain", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
			resp, err := r.activeCluster().FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{Namespace: r.namespace, WorkflowId: r.childID, WorkflowType: &commonpb.WorkflowType{Name: "unrelated"}, TaskQueue: r.childTestVars.TaskQueue(), RequestId: uuid.NewString(), WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE})
			if err != nil {
				return err
			}
			r.childRunID = resp.GetRunId()
			return nil
		}}, parentChildScenarioStep{name: "replicate only the unrelated chain while parent completion is held", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
			for {
				task, err := r.gates[int(initialStandbyCluster)].nextForWorkflow(ctx, r.childID)
				if err != nil {
					return err
				}
				if task.metadata.runID == r.childRunID {
					return task.apply()
				}
				// Earlier chain tasks remain queued after its first run was deleted.
				// This scenario intentionally delivers the new chain ahead of the parent.
				if err := task.acknowledgeWithoutApplying(); err != nil {
					return err
				}
			}
		}},
		parentChildScenarioStep{name: "reject unrelated current execution locally and on source", run: func(ctx context.Context, r *parentChildScenarioRuntime) error {
			_, err := s.clusters[int(initialStandbyCluster)].HistoryClient().VerifyFirstWorkflowTaskScheduled(ctx, childFirstRunVerification(r, firstRunID, false))
			s.Require().ErrorAs(err, new(*serviceerror.NotFound))
			before, err := r.workflowMutableState(ctx, initialStandbyCluster, parentWorkflow)
			if err != nil {
				return err
			}
			s.Require().Len(before.GetChildExecutionInfos(), 1)
			_, err = s.clusters[int(initialStandbyCluster)].HistoryClient().VerifyFirstWorkflowTaskScheduled(ctx, childFirstRunVerification(r, firstRunID, true))
			s.Require().ErrorAs(err, new(*serviceerror.NotFound))
			// Latency is recorded after the background attempt finishes, not merely
			// when it is submitted. Verification must still fail after that attempt.
			await.Require(ctx, s.T(), func(t *await.T) {
				require.NotEmpty(t, r.metricCaptures[int(initialStandbyCluster)].capture.SnapshotMetric(metrics.ChildWorkflowResendLatency.Name()))
			}, testTimeout, replicationCheckInterval)
			_, err = s.clusters[int(initialStandbyCluster)].HistoryClient().VerifyFirstWorkflowTaskScheduled(ctx, childFirstRunVerification(r, firstRunID, false))
			s.Require().ErrorAs(err, new(*serviceerror.NotFound))
			return nil
		}},
		applyReplicationThroughTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED),
	}, expectations: []parentChildExpectation{{name: "parent catch-up removes only the old pending chain", check: func(ctx context.Context, r *parentChildScenarioRuntime) error {
		state, err := r.workflowMutableState(ctx, initialStandbyCluster, parentWorkflow)
		if err != nil {
			return err
		}
		if len(state.GetChildExecutionInfos()) != 0 {
			return errors.New("old child still pending")
		}
		child, err := r.workflowMutableState(ctx, initialStandbyCluster, childWorkflow)
		if err != nil {
			return err
		}
		s.Require().Equal(r.childRunID, child.GetExecutionState().GetFirstExecutionRunId())
		return nil
	}}}})
}
