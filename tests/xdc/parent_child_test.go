package xdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
)

type childRunAction int

const (
	completeChildRun childRunAction = iota
	continueAsNewChildRun
	resetChildRun
)

func TestParentChildXDCTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(parentChildXDCTestSuite))
}

func (s *parentChildXDCTestSuite) SetupSuite() {
	s.enableTransitionHistory = true
	s.setupSuite(testcore.WithNumHistoryShards(2))
}

func (s *parentChildXDCTestSuite) SetupTest() {
	s.setupTest()
}

func (s *parentChildXDCTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestRecoversOrphanedChildAfterForceFailover covers the cross-shard ordering where the child start
// reaches the passive without its initial workflow task. Parent replication from WorkflowTaskStarted
// onward, including StartChildWorkflowExecutionInitiated, remains delayed before force failover.
//
//	                       | parent on target                         | child on target                  | active          | outcome
//	-----------------------+------------------------------------------+----------------------------------+-----------------+------------------------------
//	parent prefix arrives  | WFT scheduled                            | does not exist                   | initial active  | common parent prefix
//	child start arrives    | parent update remains delayed            | pristine, points to old version  | initial active  | cross-shard partial state
//	force failover         | incomplete branch becomes current        | unchanged                        | initial standby | target begins active recovery
//	retry StartChild       | reissued initiation on the new branch    | old run terminated, new run open | initial standby | orphan is replaced atomically
//	assert                 | ChildWorkflowExecutionStarted            | replacement is current           | initial standby | parent can make progress
//
// This scenario uses the default transition-history replication. Event checkpoints select an entire
// replication task, and the delayed parent task remains unapplied through the assertions.
func (s *parentChildXDCTestSuite) TestRecoversOrphanedChildAfterForceFailover() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			enableOrphanedChildWorkflowReplacement(),
			startParentWorkflow(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			completeParentWorkflowTaskWithStartChildCommand(),
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			forceFailoverNamespaceTo(initialStandbyCluster),
			completeParentWorkflowTaskWithStartChildCommand(),
		},
		expectations: []parentChildExpectation{
			workflowHasEventOnCluster(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			),
			parentOwnsReplacementChild(),
			orphanedChildReplacementWasRecorded(),
		},
	})
}

// TestActiveRecoversChildCompletionWhenParentReplicationArrivesLate covers the cross-shard ordering
// where parent replication remains delayed until after the child closes on the new active cluster.
//
//	                    | parent on target                | child on target   | active          | outcome
//	--------------------+---------------------------------+-------------------+-----------------+-----------------------------------------------
//	conversion paused   | does not exist                  | does not exist    | initial active  | source cannot emit the parent snapshot
//	child start arrives | does not exist                  | RUNNING           | initial active  | child points to the missing parent
//	force failover      | does not exist                  | RUNNING           | initial standby | target becomes active with partial state
//	child closes        | does not exist                  | COMPLETED         | initial standby | CloseExecution cannot notify the missing parent
//	parent tasks arrive | child relationship restored     | COMPLETED         | initial standby | regenerated StartChild finds the child closed
//	refresh child tasks | ChildWorkflowExecutionCompleted | COMPLETED         | initial standby | recreated CloseExecution records completion
//
// Parent task conversion is paused before the first task is emitted. After the child closes, the
// first raw task is converted against the latest source state, producing a complete initial snapshot
// that includes ChildWorkflowExecutionStarted. No task is acknowledged or dropped. Event checkpoints
// select an entire replication task, not an individual event.
func (s *parentChildXDCTestSuite) TestActiveRecoversChildCompletionWhenParentReplicationArrivesLate() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			setStandbyClusterDelay(initialStandbyCluster, 0),
			delayParentReplicationTaskConversion(initialActiveCluster),
			startParentWorkflow(),
			completeParentWorkflowTaskWithStartChildCommand(),
			waitForWorkflowEventOnCluster(
				initialActiveCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, parentWorkflow),
			forceFailoverNamespaceTo(initialStandbyCluster),
			closeChildRunsBeforeApplyingParent(completeChildRun),
		},
		expectations: []parentChildExpectation{
			parentRecordsFinalChildRunCompletion(),
		},
	})
}

// TestActiveRecoversContinuedAsNewChildCompletionWhenParentReplicationArrivesLate covers the same
// delayed-parent ordering when the child continues as new before its terminal run closes.
//
//	                       | parent on target                 | child on target                         | active          | outcome
//	-----------------------+----------------------------------+-----------------------------------------+-----------------+-----------------------------------------------
//	conversion paused      | does not exist                   | does not exist                          | initial active  | source cannot emit the parent snapshot
//	child start arrives    | does not exist                   | run 1 RUNNING                           | initial active  | child points to the missing parent
//	force failover         | does not exist                   | run 1 RUNNING                           | initial standby | target becomes active with partial state
//	child continues as new | does not exist                   | run 1 CONTINUED_AS_NEW; run 2 RUNNING   | initial standby | run 1 CloseExecution processes without replying
//	terminal run closes    | does not exist                   | run 1 CONTINUED_AS_NEW; run 2 COMPLETED | initial standby | run 2 cannot notify the missing parent
//	parent tasks arrive    | child relationship restored      | run 2 COMPLETED                         | initial standby | StartChild resolves the current chain member
//	refresh current run    | ChildWorkflowExecutionCompleted  | run 2 COMPLETED                         | initial standby | recreated run 2 CloseExecution records completion
//
// Both child CloseExecution tasks are processed before the parent snapshot is applied. Scheduling
// the first task on run 1 reports that it is closed; loading current mutable state finds terminal run 2
// with the same first-run ID, so recovery refreshes run 2. Parent replication is delayed, not lost.
func (s *parentChildXDCTestSuite) TestActiveRecoversContinuedAsNewChildCompletionWhenParentReplicationArrivesLate() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			setStandbyClusterDelay(initialStandbyCluster, 0),
			delayParentReplicationTaskConversion(initialActiveCluster),
			startParentWorkflow(),
			completeParentWorkflowTaskWithStartChildCommand(),
			waitForWorkflowEventOnCluster(
				initialActiveCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, parentWorkflow),
			forceFailoverNamespaceTo(initialStandbyCluster),
			closeChildRunsBeforeApplyingParent(
				continueAsNewChildRun,
				completeChildRun,
			),
		},
		expectations: []parentChildExpectation{
			parentRecordsFinalChildRunCompletion(),
		},
	})
}

// TestActiveRecoversResetChildCompletionWhenParentReplicationArrivesLate covers the same
// delayed-parent ordering when a completed child is reset and its reset run also closes.
//
//	                    | parent on target                 | child on target                         | active          | outcome
//	--------------------+----------------------------------+-----------------------------------------+-----------------+-----------------------------------------------
//	conversion paused   | does not exist                   | does not exist                          | initial active  | source cannot emit the parent snapshot
//	child start arrives | does not exist                   | original run RUNNING                    | initial active  | child points to the missing parent
//	force failover      | does not exist                   | original run RUNNING                    | initial standby | target becomes active with partial state
//	original run closes | does not exist                   | original run COMPLETED                  | initial standby | completion cannot reach the missing parent
//	child is reset      | does not exist                   | original COMPLETED; reset run RUNNING   | initial standby | reset run preserves the original first-run ID
//	reset run closes    | does not exist                   | original COMPLETED; reset run COMPLETED | initial standby | reset completion cannot reach the missing parent
//	parent tasks arrive | child relationship restored      | reset run COMPLETED                     | initial standby | StartChild resolves the current reset run
//	refresh reset run   | ChildWorkflowExecutionCompleted  | reset run COMPLETED                     | initial standby | recreated reset-run CloseExecution records completion
//
// The original and reset-run CloseExecution tasks are processed before the parent snapshot is
// applied. Because the reset run inherits the original first-run ID, recovery recognizes it as the
// same child chain and refreshes the terminal reset run. Parent replication is delayed, not lost.
func (s *parentChildXDCTestSuite) TestActiveRecoversResetChildCompletionWhenParentReplicationArrivesLate() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			setStandbyClusterDelay(initialStandbyCluster, 0),
			delayParentReplicationTaskConversion(initialActiveCluster),
			startParentWorkflow(),
			completeParentWorkflowTaskWithStartChildCommand(),
			waitForWorkflowEventOnCluster(
				initialActiveCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, parentWorkflow),
			forceFailoverNamespaceTo(initialStandbyCluster),
			closeChildRunsBeforeApplyingParent(
				completeChildRun,
				resetChildRun,
				completeChildRun,
			),
		},
		expectations: []parentChildExpectation{
			parentRecordsFinalChildRunCompletion(),
		},
	})
}

func parentRecordsFinalChildRunCompletion() parentChildExpectation {
	return parentChildExpectation{
		name: "parent records completion of the final child run",
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			events, err := runtime.workflowHistoryOnCluster(ctx, initialStandbyCluster, parentWorkflow)
			if err != nil {
				return err
			}
			if findHistoryEvent(
				events,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
				func(event *historypb.HistoryEvent) bool {
					execution := event.GetChildWorkflowExecutionCompletedEventAttributes().GetWorkflowExecution()
					return execution.GetWorkflowId() == runtime.childID && execution.GetRunId() == runtime.childRunID
				},
			) == nil {
				return fmt.Errorf("parent has no completion for final child run %s/%s", runtime.childID, runtime.childRunID)
			}
			return nil
		},
	}
}

func closeChildRunsBeforeApplyingParent(actions ...childRunAction) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "close every child run before applying parent replication",
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			if len(actions) == 0 {
				return errors.New("no child close commands")
			}
			processedCloseTasks := make(chan string, len(actions))
			cleanup := runtime.suite.clusters[initialStandbyCluster].InjectHook(
				runtime.suite.T(),
				testhooks.NewHook(testhooks.HistoryTransferTaskInterceptor, func(task historytasks.Task, execute func()) {
					execute()
					if task.GetWorkflowID() == runtime.childID && task.GetType() == enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION {
						select {
						case processedCloseTasks <- task.GetRunID():
						default:
						}
					}
				}),
				namespace.ID(runtime.namespaceID),
			)
			defer cleanup()

			closeRun := func(commandType enumspb.CommandType) error {
				closedRunID, err := runtime.closeChildWorkflowTask(ctx, commandType)
				if err != nil {
					return err
				}
				for {
					select {
					case processedRunID := <-processedCloseTasks:
						if processedRunID == closedRunID {
							runtime.tracef("  processed child CloseExecution for run %s", closedRunID)
							return nil
						}
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}

			for _, action := range actions {
				switch action {
				case completeChildRun:
					if err := closeRun(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION); err != nil {
						return err
					}
				case continueAsNewChildRun:
					if err := closeRun(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION); err != nil {
						return err
					}
				case resetChildRun:
					if err := runtime.resetChildWorkflow(ctx); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unknown child run action %d", action)
				}
			}
			if runtime.resumeParentConversion == nil {
				return errors.New("parent replication task conversion is not delayed")
			}
			runtime.resumeParentConversion()
			return runtime.processReplicationThroughTaskContainingEvent(
				ctx,
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
				applyReplicationTask,
			)
		},
	}
}

func enableOrphanedChildWorkflowReplacement() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "enable orphaned child workflow replacement",
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			for _, cluster := range runtime.suite.clusters {
				runtime.cleanups = append(runtime.cleanups, cluster.OverrideDynamicConfig(
					runtime.suite.T(),
					dynamicconfig.EnableOrphanedChildWorkflowReplacement,
					true,
				))
			}
			return nil
		},
	}
}

func parentOwnsReplacementChild() parentChildExpectation {
	return parentChildExpectation{
		name: "parent owns a running replacement child",
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			if runtime.childRunID == "" {
				return errors.New("orphaned child WorkflowExecutionStarted was not applied")
			}
			currentChild, err := runtime.activeCluster().FrontendClient().DescribeWorkflowExecution(
				ctx,
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: runtime.namespace,
					Execution: &commonpb.WorkflowExecution{WorkflowId: runtime.childID},
				},
			)
			if err != nil {
				return err
			}
			currentChildInfo := currentChild.GetWorkflowExecutionInfo()
			replacementRunID := currentChildInfo.GetExecution().GetRunId()
			if replacementRunID == "" || replacementRunID == runtime.childRunID {
				return fmt.Errorf("current child run is %q, want a replacement for %q", replacementRunID, runtime.childRunID)
			}
			if currentChildInfo.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return fmt.Errorf("replacement child status is %s, want RUNNING", currentChildInfo.GetStatus())
			}

			parentEvents, err := runtime.activeWorkflowHistory(ctx, parentWorkflow)
			if err != nil {
				return err
			}
			if findHistoryEvent(
				parentEvents,
				enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
				func(event *historypb.HistoryEvent) bool {
					return event.GetStartChildWorkflowExecutionFailedEventAttributes().GetWorkflowId() == runtime.childID
				},
			) != nil {
				return fmt.Errorf("parent recorded StartChildWorkflowExecutionFailed for child %q", runtime.childID)
			}
			if findHistoryEvent(
				parentEvents,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
				func(event *historypb.HistoryEvent) bool {
					startedExecution := event.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution()
					return startedExecution.GetWorkflowId() == runtime.childID &&
						startedExecution.GetRunId() == replacementRunID
				},
			) == nil {
				return fmt.Errorf("parent has no ChildWorkflowExecutionStarted for replacement run %q", replacementRunID)
			}
			return nil
		},
	}
}

func orphanedChildReplacementWasRecorded() parentChildExpectation {
	return parentChildExpectation{
		name: "orphaned child replacement metric is recorded",
		check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.requireCapturedMetric(
				initialStandbyCluster,
				metrics.OrphanedChildWorkflowReplacement.Name(),
				map[string]string{"outcome": "replaced"},
			)
		},
	}
}

// TestStandbyResendsMissingChild covers the cross-shard ordering where the parent update
// identifying a started child reaches the passive while the child's WorkflowExecutionStarted
// task remains delayed.
//
//	                           | parent on target                | child on target       | active         | outcome
//	---------------------------+---------------------------------+-----------------------+----------------+---------------------------------------------
//	parent prefix arrives      | WFT scheduled                   | does not exist        | initial active | common parent prefix
//	child start is delayed     | unchanged                       | does not exist        | initial active | child start remains unapplied
//	parent child-start arrives | ChildWorkflowExecutionStarted   | does not exist        | initial active | standby verifies the child's first WFT
//	verification fails         | child-start relationship exists | does not exist        | initial active | VerifyFirstWorkflowTaskScheduled: NotFound
//	child is resent            | unchanged                       | RUNNING, WFT scheduled | initial active | child state is restored from the source
//
// Event checkpoints select an entire replication task, not an individual event. The delayed child
// start task intentionally remains unapplied while child state is restored through state sync.
func (s *parentChildXDCTestSuite) TestStandbyResendsMissingChild() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			// Track source time without the production standby lag so the task becomes resend-eligible quickly.
			setStandbyClusterDelay(initialStandbyCluster, 0),
			// Enable the child resend path on the passive cluster.
			enableChildWorkflowResend(initialStandbyCluster),
			// Skip the normal resend delay so the pending standby StartChild task requests a state sync.
			setStandbyTaskResendDelay(
				initialStandbyCluster,
				enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
				0,
			),
			// Create the parent and its first workflow task on the initial active cluster.
			startParentWorkflow(),
			// Establish the parent on the passive before replicating its child relationship.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			// Complete the parent task with StartChild, creating the child on the source.
			completeParentWorkflowTaskWithStartChildCommand(),
			// Keep the child start off the passive so the child is locally missing there.
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			// Confirm that the ordinary child replication task remains unapplied before verification.
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, childWorkflow),
			// Apply the parent's child-start record, triggering verification of the missing child.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			// Observe the original missing-child error returned while the resend runs in the background.
			waitForHistoryVerificationFailureOnCluster(
				initialStandbyCluster,
				historyClientVerifyFirstWorkflowTask,
				&serviceerror.NotFound{},
			),
		},
		expectations: []parentChildExpectation{
			{
				name: "child workflow resend is attempted on the initial standby cluster",
				check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
					return runtime.requireCapturedMetric(
						initialStandbyCluster,
						metrics.ChildWorkflowResendAttempts.Name(),
						nil,
					)
				},
			},
			workflowHasEventOnCluster(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
		},
	})
}

// TestStandbyResendsChildWithoutFirstWorkflowTask covers the cross-shard ordering where the
// child's WorkflowExecutionStarted reaches the passive, its first WorkflowTaskScheduled remains
// delayed, and the parent update identifying the started child then arrives.
//
//	                           | parent on target                | child on target                  | active         | outcome
//	---------------------------+---------------------------------+----------------------------------+----------------+----------------------------------------------
//	parent prefix arrives      | WFT scheduled                   | does not exist                   | initial active | common parent prefix
//	child start arrives        | unchanged                       | CREATED/RUNNING, no first WFT    | initial active | child execution exists on the target
//	child first WFT is delayed | unchanged                       | unchanged                        | initial active | WorkflowTaskScheduled remains unapplied
//	parent child-start arrives | ChildWorkflowExecutionStarted   | unchanged                        | initial active | standby verifies the child's first WFT
//	verification fails        | child-start relationship exists | next event ID 2, no scheduled ID | initial active | VerifyFirstWorkflowTaskScheduled: WorkflowNotReady
//	child is resent            | unchanged                       | RUNNING, WFT scheduled           | initial active | child state is restored from the source
//
// The replication gate applies WorkflowExecutionStarted while keeping the separate
// WorkflowTaskScheduled update delayed, so recovery must come from state sync.
func (s *parentChildXDCTestSuite) TestStandbyResendsChildWithoutFirstWorkflowTask() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			// Keep child Started and its first WFT in separate event-range replication tasks.
			useLegacyHistoryReplication(),
			// Track source time without the production standby lag so the task becomes resend-eligible quickly.
			setStandbyClusterDelay(initialStandbyCluster, 0),
			// Enable the child resend path on the passive cluster.
			enableChildWorkflowResend(initialStandbyCluster),
			// Skip the normal resend delay so the pending standby StartChild task requests a state sync.
			setStandbyTaskResendDelay(
				initialStandbyCluster,
				enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
				0,
			),
			// Create the parent and its first workflow task on the initial active cluster.
			startParentWorkflow(),
			// Establish the parent on the passive before replicating its child relationship.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			// Complete the parent task with StartChild, creating the child on the source.
			completeParentWorkflowTaskWithStartChildCommand(),
			// Create the child on the passive without advancing through its first WFT.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			// Hold the child's first WFT so its passive mutable state remains not ready.
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			// Drop the ordinary WFT replication task so it cannot heal the passive child.
			acknowledgeDelayedReplicationWithoutApplying(initialStandbyCluster, childWorkflow),
			// State sync uses transition history after the partial child state has been reproduced.
			useTransitionHistory(),
			// Initialize transition history on the source child while retaining its scheduled WFT.
			signalChildWorkflow(),
			// Do not let the transition update repair the passive through ordinary replication.
			acknowledgeNextReplicationTaskWithoutApplying(initialStandbyCluster, childWorkflow),
			// Apply the parent's child-start record, triggering verification of that partial child.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			// Observe the original not-ready error returned while the resend runs in the background.
			waitForHistoryVerificationFailureOnCluster(
				initialStandbyCluster,
				historyClientVerifyFirstWorkflowTask,
				&serviceerror.WorkflowNotReady{},
			),
		},
		expectations: []parentChildExpectation{
			{
				name: "child workflow resend is attempted on the initial standby cluster",
				check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
					return runtime.requireCapturedMetric(
						initialStandbyCluster,
						metrics.ChildWorkflowResendAttempts.Name(),
						nil,
					)
				},
			},
			workflowHasEventOnCluster(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
		},
	})
}

// TestStandbyDiscardsChildCloseTaskWhenParentCompletionIsMissing covers the cross-shard ordering
// where the child's completion reaches the passive while the parent's corresponding
// ChildWorkflowExecutionCompleted update remains delayed.
//
//	                           | parent on target                  | child on target | active         | outcome
//	---------------------------+-----------------------------------+-----------------+----------------+----------------------------------------------
//	parent-child state arrives | ChildWorkflowExecutionStarted     | RUNNING         | initial active | both sides initially agree that child is running
//	child closes on source     | unchanged on target               | unchanged       | initial active | source parent records child completion
//	parent completion delayed  | still tracks the running child    | unchanged       | initial active | parent completion remains unapplied
//	child completion arrives   | no child-completion event         | COMPLETED       | initial active | VerifyChildExecutionCompletionRecorded: WorkflowNotReady
//	discard window expires     | unchanged                         | COMPLETED       | initial active | standby CloseExecution task is discarded
//
// The local parent-verification grace is kept longer than the compressed discard window so this
// scenario exercises the retry-and-discard fallback rather than parent state resend. The delayed
// parent completion task remains unapplied through the assertions.
func (s *parentChildXDCTestSuite) TestStandbyDiscardsChildCloseTaskWhenParentCompletionIsMissing() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			// Track source time without the production standby lag so task expiration is observable quickly.
			setStandbyClusterDelay(initialStandbyCluster, 0),
			// Create the parent and its first workflow task on the initial active cluster.
			startParentWorkflow(),
			// Establish the parent on the passive before replicating its child relationship.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			// Start the child on the source and produce both sides of the relationship.
			completeParentWorkflowTaskWithStartChildCommand(),
			// Replicate the child so its later completion can run the passive CloseExecution task.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			// Let the passive parent record that the child started and is still running.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
			// Close the child on the source, which also records completion in the source parent.
			completeChildWorkflowTask(),
			// Wait for and hold the parent completion so the passive still considers the child running.
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			),
			// Prevent this scenario from switching to parent state resend before the task is discarded.
			setLocalParentVerificationGrace(initialStandbyCluster, time.Hour),
			// Apply child completion, triggering verification against the incomplete passive parent.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			),
			// Observe the exact not-ready error before shortening the task's discard window.
			waitForHistoryVerificationFailureOnCluster(
				initialStandbyCluster,
				historyClientVerifyChildCompletion,
				&serviceerror.WorkflowNotReady{},
			),
			// Expire only the pending standby CloseExecution task; its next attempt should be discarded.
			setStandbyTaskDiscardDelay(
				initialStandbyCluster,
				enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
				0,
			),
		},
		expectations: []parentChildExpectation{
			currentWorkflowHasStatusOnCluster(
				initialStandbyCluster,
				childWorkflow,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			),
			{
				name: "parent completion remains delayed on the initial standby cluster",
				check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
					events, err := runtime.workflowHistoryOnCluster(ctx, initialStandbyCluster, parentWorkflow)
					if err != nil {
						return err
					}
					if historyContainsEvent(events, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED) {
						return errors.New("passive parent already contains ChildWorkflowExecutionCompleted")
					}
					return nil
				},
			},
			taskWasDiscardedOnCluster(
				initialStandbyCluster,
				metrics.TaskTypeTransferStandbyTaskCloseExecution,
			),
		},
	})
}

// TestStandbyResendsMissingParentWhenChildCloses covers the cross-shard ordering where child
// replication reaches the passive while ordinary parent replication remains delayed. When the
// child's completion arrives, standby verification finds the parent missing and resends its state.
//
//	                         | parent on target                    | child on target          | active         | outcome
//	-------------------------+-------------------------------------+--------------------------+----------------+-----------------------------------------------
//	parent start is delayed  | does not exist                      | does not exist           | initial active | ordinary parent replication remains unapplied
//	child start arrives      | does not exist                      | RUNNING, points to parent | initial active | cross-shard partial state
//	child closes on source   | does not exist                      | unchanged on target      | initial active | source parent records child completion
//	confirm missing parent   | does not exist                      | unchanged                | initial active | establishes the resend precondition
//	child completion arrives | restored by parent state sync       | COMPLETED                | initial active | VerifyChild detects the missing parent
//	assert                   | has ChildWorkflowExecutionCompleted | COMPLETED                | initial active | resend observed; parent state restored
//
// The original parent replication task remains delayed. The local verification grace is set to zero
// so VerifyChildExecutionCompletionRecorded requests a parent resend within the test timeout.
func (s *parentChildXDCTestSuite) TestStandbyResendsMissingParentWhenChildCloses() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			// Create the parent and its first workflow task on the initial active cluster.
			startParentWorkflow(),
			// Keep all ordinary parent replication off the passive.
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			// Start the child on the source while its parent remains absent on the passive.
			completeParentWorkflowTaskWithStartChildCommand(),
			// Replicate only the child, creating a child whose parent is locally missing.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			// Close the child on the source to produce child-completion verification work.
			completeChildWorkflowTask(),
			// Ensure a state resend will include the parent's child-completion event.
			waitForWorkflowEventOnCluster(
				initialActiveCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			),
			// Confirm the passive still lacks the parent before delivering child completion.
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, parentWorkflow),
			// Skip the normal local retry window so the next verification requests a resend.
			setLocalParentVerificationGrace(initialStandbyCluster, 0),
			// Deliver child completion, causing standby verification to resend the missing parent.
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			),
		},
		expectations: []parentChildExpectation{
			{
				name: "parent workflow resend is attempted on the initial standby cluster",
				check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
					return runtime.requireCapturedMetric(
						initialStandbyCluster,
						metrics.ParentWorkflowResendAttempts.Name(),
						nil,
					)
				},
			},
			workflowHasEventOnCluster(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			),
		},
	})
}
