package xdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/tests/testcore"
)

func TestParentChildXDCTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(parentChildXDCTestSuite))
}

func (s *parentChildXDCTestSuite) SetupSuite() {
	s.enableTransitionHistory = true
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():                    true,
		dynamicconfig.EnableReplicationTaskBatching.Key():              false,
		dynamicconfig.EnableAsyncParentWorkflowResend.Key():            false,
		dynamicconfig.MaxLocalParentWorkflowVerificationDuration.Key(): time.Duration(0),
	}
	s.logger = log.NewTestLogger()
	s.setupSuite(testcore.WithNumHistoryShards(2))
}

func (s *parentChildXDCTestSuite) SetupTest() {
	s.setupTest()
}

func (s *parentChildXDCTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestReproOrphanedChildAfterForceFailover covers the cross-shard ordering where the child start
// reaches the passive while parent replication from WorkflowTaskStarted onward, including
// StartChildWorkflowExecutionInitiated, remains delayed, followed by force failover.
//
//	                       | parent on target                         | child on target                 | active          | outcome
//	-----------------------+------------------------------------------+---------------------------------+-----------------+------------------------------
//	parent prefix arrives  | WFT scheduled                            | does not exist                  | initial active  | common parent prefix
//	child start arrives    | parent update remains delayed            | RUNNING, points to old version  | initial active  | cross-shard partial state
//	force failover         | incomplete branch becomes current        | unchanged                       | initial standby | target begins active recovery
//	retry StartChild       | same initiated event ID at a new version | unchanged                       | initial standby | WORKFLOW_ALREADY_EXISTS
//	assert                 | no ChildWorkflowExecutionStarted         | still points to losing version  | initial standby | child is orphaned
//
// Event checkpoints select an entire replication task, not an individual event. The delayed parent
// task intentionally remains unapplied through the assertions.
func (s *parentChildXDCTestSuite) TestReproOrphanedChildAfterForceFailover() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
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

			// force failover
			forceFailoverNamespaceTo(initialStandbyCluster),

			completeParentWorkflowTaskWithStartChildCommand(),
		},
		expectations: []parentChildExpectation{
			parentHasStartChildFailure(enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS),
			currentWorkflowHasStatusOnCluster(
				initialStandbyCluster,
				childWorkflow,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
			{
				name: "child remains attached to the losing parent branch",
				check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
					if runtime.childRunID == "" {
						return errors.New("child WorkflowExecutionStarted was not applied")
					}
					childEvents, err := runtime.childHistory(ctx)
					if err != nil {
						return err
					}
					childStartedEvent := findHistoryEvent(
						childEvents,
						enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						nil,
					)
					if childStartedEvent == nil {
						return fmt.Errorf("child run %q has no persisted WorkflowExecutionStarted event", runtime.childRunID)
					}
					childStartedAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
					if childStartedAttrs == nil {
						return errors.New("persisted child start event has no attributes")
					}
					parentExecution := childStartedAttrs.GetParentWorkflowExecution()
					if parentExecution.GetWorkflowId() != runtime.parentID || parentExecution.GetRunId() != runtime.parentRunID {
						return fmt.Errorf(
							"child parent is %s/%s, want %s/%s",
							parentExecution.GetWorkflowId(),
							parentExecution.GetRunId(),
							runtime.parentID,
							runtime.parentRunID,
						)
					}

					parentEvents, err := runtime.parentHistory(ctx)
					if err != nil {
						return err
					}
					currentInitiatedEvent := findHistoryEvent(
						parentEvents,
						enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
						func(event *historypb.HistoryEvent) bool {
							attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
							return attrs.GetWorkflowId() == runtime.childID &&
								event.GetEventId() == childStartedAttrs.GetParentInitiatedEventId()
						},
					)
					currentChildStartedEvent := findHistoryEvent(
						parentEvents,
						enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
						func(event *historypb.HistoryEvent) bool {
							attrs := event.GetChildWorkflowExecutionStartedEventAttributes()
							return attrs.GetWorkflowExecution().GetWorkflowId() == runtime.childID &&
								attrs.GetWorkflowExecution().GetRunId() == runtime.childRunID
						},
					)
					if currentChildStartedEvent != nil {
						return errors.New("current parent branch owns the applied child")
					}
					if currentInitiatedEvent == nil {
						return fmt.Errorf(
							"current parent branch has no StartChild event %d for child %q",
							childStartedAttrs.GetParentInitiatedEventId(),
							runtime.childID,
						)
					}
					if currentInitiatedEvent.GetVersion() == childStartedAttrs.GetParentInitiatedEventVersion() {
						return fmt.Errorf(
							"child and current parent branch have the same initiation version %d",
							currentInitiatedEvent.GetVersion(),
						)
					}
					runtime.tracef(
						"orphan confirmed: child parent pointer=(%d, v%d), current parent initiation=(%d, v%d)",
						childStartedAttrs.GetParentInitiatedEventId(),
						childStartedAttrs.GetParentInitiatedEventVersion(),
						currentInitiatedEvent.GetEventId(),
						currentInitiatedEvent.GetVersion(),
					)
					return nil
				},
			},
		},
	})
}

// TestStandbyVerifiesMissingChild covers the cross-shard ordering where the parent update
// identifying a started child reaches the passive while the child's WorkflowExecutionStarted
// task remains delayed.
//
//	                           | parent on target                | child on target | active         | outcome
//	---------------------------+---------------------------------+-----------------+----------------+---------------------------------------------
//	parent prefix arrives      | WFT scheduled                   | does not exist  | initial active | common parent prefix
//	child start is delayed     | unchanged                       | does not exist  | initial active | child start remains unapplied
//	parent child-start arrives | ChildWorkflowExecutionStarted   | does not exist  | initial active | standby verifies the child's first WFT
//	assert                     | child-start relationship exists | does not exist  | initial active | VerifyFirstWorkflowTaskScheduled: NotFound
//
// Event checkpoints select an entire replication task, not an individual event. The delayed child
// start task intentionally remains unapplied through the assertions.
func (s *parentChildXDCTestSuite) TestStandbyVerifiesMissingChild() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			startParentWorkflow(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			completeParentWorkflowTaskWithStartChildCommand(),
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
		},
		expectations: []parentChildExpectation{
			workflowIsMissingOnCluster(initialStandbyCluster, childWorkflow),
			historyVerificationFailedOnCluster(
				initialStandbyCluster,
				historyClientVerifyFirstWorkflowTask,
				&serviceerror.NotFound{},
			),
		},
	})
}

// TestStandbyVerifiesChildWithoutFirstWorkflowTask covers the cross-shard ordering where the
// child's WorkflowExecutionStarted reaches the passive, its first WorkflowTaskScheduled remains
// delayed, and the parent update identifying the started child then arrives.
//
//	                           | parent on target                | child on target                  | active         | outcome
//	---------------------------+---------------------------------+----------------------------------+----------------+----------------------------------------------
//	parent prefix arrives      | WFT scheduled                   | does not exist                   | initial active | common parent prefix
//	child start arrives        | unchanged                       | CREATED/RUNNING, no first WFT    | initial active | child execution exists on the target
//	child first WFT is delayed | unchanged                       | unchanged                        | initial active | WorkflowTaskScheduled remains unapplied
//	parent child-start arrives | ChildWorkflowExecutionStarted   | unchanged                        | initial active | standby verifies the child's first WFT
//	assert                     | child-start relationship exists | next event ID 2, no scheduled ID | initial active | VerifyFirstWorkflowTaskScheduled: WorkflowNotReady
//
// This scenario uses legacy history replication because it fixes each transaction's event range.
// With receiver batching disabled, WorkflowExecutionStarted can be applied while the separate
// WorkflowTaskScheduled task remains delayed through the assertions.
func (s *parentChildXDCTestSuite) TestStandbyVerifiesChildWithoutFirstWorkflowTask() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			useLegacyHistoryReplication(),
			startParentWorkflow(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			completeParentWorkflowTaskWithStartChildCommand(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			),
		},
		expectations: []parentChildExpectation{
			{
				name: "child exists without its first workflow task on the initial standby cluster",
				check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
					mutableState, err := runtime.workflowMutableState(ctx, initialStandbyCluster, childWorkflow)
					if err != nil {
						return err
					}
					if state := mutableState.GetExecutionState().GetState(); state != enumsspb.WORKFLOW_EXECUTION_STATE_CREATED {
						return fmt.Errorf("child state is %s, want %s", state, enumsspb.WORKFLOW_EXECUTION_STATE_CREATED)
					}
					if status := mutableState.GetExecutionState().GetStatus(); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
						return fmt.Errorf("child status is %s, want %s", status, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
					}
					if scheduledEventID := mutableState.GetExecutionInfo().GetWorkflowTaskScheduledEventId(); scheduledEventID != common.EmptyEventID {
						return fmt.Errorf("child workflow task scheduled event ID is %d, want %d", scheduledEventID, common.EmptyEventID)
					}
					if nextEventID := mutableState.GetNextEventId(); nextEventID != common.FirstEventID+1 {
						return fmt.Errorf("child next event ID is %d, want %d", nextEventID, common.FirstEventID+1)
					}
					return nil
				},
			},
			historyVerificationFailedOnCluster(
				initialStandbyCluster,
				historyClientVerifyFirstWorkflowTask,
				&serviceerror.WorkflowNotReady{},
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
// The original parent replication task remains delayed. With the local verification grace set to
// zero and asynchronous resend disabled, VerifyChildExecutionCompletionRecorded synchronously pulls
// the parent through SyncWorkflowState and applies it on the passive.
func (s *parentChildXDCTestSuite) TestStandbyResendsMissingParentWhenChildCloses() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			startParentWorkflow(),
			delayReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			completeParentWorkflowTaskWithStartChildCommand(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			completeChildWorkflowTask(),
			waitForWorkflowEventOnCluster(
				initialActiveCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			),
			confirmWorkflowIsMissingOnCluster(initialStandbyCluster, parentWorkflow),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			),
		},
		expectations: []parentChildExpectation{
			historyVerificationRequestedOnCluster(initialStandbyCluster, historyClientVerifyChildCompletion),
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
			workflowExistsOnCluster(initialStandbyCluster, parentWorkflow),
			workflowHasEventOnCluster(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			),
		},
	})
}
