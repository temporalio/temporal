package acceptance

import (
	"fmt"
	"testing"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/patterns"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflowUpdate(t *testing.T) {
	ts := NewTestSuite(t)

	// covers TestWaitAccepted_GotCompleted [TODO: check marker]
	// covers TestEmptySpeculativeWorkflowTask_AcceptComplete [TODO: runID vs non-RunID]
	// covers TestNotEmptySpeculativeWorkflowTask_AcceptComplete
	t.Run("Update completes", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			if s.Maybe("non-empty speculative WFT") {
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft,
					Commands: GenList(ScheduleActivityTaskCommand{TaskQueue: tq})})
			} else { // TODO: give name here
				Act(wkr, RespondWorkflowTaskCompleted{Task: wft})
			}

			upd := ActStart(usr, UpdateWorkflowExecution{
				WorkflowExecution: wfe,
				WaitStage:         UpdateWaitStages})

			wft = Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: upd},
					WorkflowUpdateCompletionMessage{Update: upd})})

			s.Await(&upd.Completed)

			Act(usr, PollWorkflowExecutionUpdate{
				WorkflowUpdate: upd,
				WaitStage:      UpdateWaitStages})
		})
	})

	// covers TestScheduledSpeculativeWorkflowTask_DeduplicateID
	// covers TestStartedSpeculativeWorkflowTask_DeduplicateID
	// covers TestCompletedSpeculativeWorkflowTask_DeduplicateID [TODO]
	t.Run("Update dedups", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wkr.DrainWorkflowTask(tq)

			upd1 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			if s.Maybe("send 2nd update with same ID before task is scheduled") {
				ActStart(usr, UpdateWorkflowExecution{
					WorkflowExecution: wfe,
					UpdateID:          GenJust(upd1.GetID())})
			}

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			if s.Maybe("send 2nd update with same ID after task was scheduled") {
				ActStart(usr, UpdateWorkflowExecution{
					WorkflowExecution: wfe,
					UpdateID:          GenJust(upd1.GetID())})
			}

			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: upd1},
					WorkflowUpdateCompletionMessage{Update: upd1}),
			})

			s.Await(&upd1.Completed)

			// TODO: need separate clusters
			//if s.Maybe("close history shard") {
			//	// Close shard to make sure that for completed updates deduplication works even after a shard reload.
			//	c.CloseShard(wfe)
			//}

			if s.Maybe("send 2nd update with same ID after update was completed") {
				ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe, UpdateID: GenJust(upd1.GetID())})
			}
		})
	})

	// covers TestUpdatesAreSentToWorkerInOrderOfAdmission [TODO: assertion]
	t.Run("Update ordered", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			var updates []*model.WorkflowUpdate
			for i := 0; i < 3; i++ {
				updates = append(updates, ActStart(usr, UpdateWorkflowExecution{
					WorkflowExecution: wfe,
					UpdateID:          GenJust(ID(fmt.Sprintf("update-%d", i)))}))
			}

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: updates[0]},
					WorkflowUpdateCompletionMessage{Update: updates[0]},
					WorkflowUpdateAcceptMessage{Update: updates[1]},
					WorkflowUpdateCompletionMessage{Update: updates[1]},
					WorkflowUpdateAcceptMessage{Update: updates[2]},
					WorkflowUpdateCompletionMessage{Update: updates[2]}),
			})

			for _, upd := range updates {
				s.Await(&upd.Completed)
			}
		})
	})

	// covers TestStartedSpeculativeWorkflowTask_TerminateWorkflow [TODO: assert]
	// covers TestScheduledSpeculativeWorkflowTask_TerminateWorkflow [TODO: assert]
	t.Run("Workflow terminates during Update", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wkr.DrainWorkflowTask(tq)

			upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
			patterns.PollUpdateUntilAdmitted(s, upd, usr)

			if s.Maybe("terminate *before* Update is scheduled") {
				Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})
				s.Await(&upd.Aborted)
				return
			}

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			// terminate *after* Update is scheduled
			Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})

			_, _ = ActTry(wkr, RespondWorkflowTaskCompleted{
				Task: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: upd},
					WorkflowUpdateCompletionMessage{Update: upd}),
			})
			// TODO: verify error type/message

			s.Await(&upd.Aborted)
		})
	})

	// TODO: flaky
	// covers TestScheduledSpeculativeWorkflowTask_LostUpdate
	t.Run("Update lost", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wkr.DrainWorkflowTask(tq)

			ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe}, WithActTimeout(common.MinLongPollTimeout))

			loseNonDurableUpdates(c, wfe) // make speculative WFT and update registry disappear

			_, _ = ActTry(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			// TODO: check error type/message

			Act(usr, SignalWorkflowExecution{WorkflowExecution: wfe})

			Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		})
	})
}

// Simulating an unexpected loss of the update registry due to a crash. The shard finalizer won't run.
// Therefore, the workflow context is NOT cleared, pending update requests are NOT aborted and will time out.
func loseNonDurableUpdates(cluster *Cluster, wfe *model.WorkflowExecution) {
	cluster.SetGlobalDynamicConfig(dynamicconfig.ShardFinalizerTimeout, 0) // TODO: move to cluster init
	cluster.CloseShard(wfe)
}
