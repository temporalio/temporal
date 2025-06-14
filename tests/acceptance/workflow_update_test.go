package acceptance

import (
	"fmt"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflowUpdate(t *testing.T) {
	ts := NewTestSuite(t)

	// covers TestWaitAccepted_GotCompleted [TODO: check marker]
	// covers TestEmptySpeculativeWorkflowTask_AcceptComplete [TODO: runID vs non-RunID]
	// covers TestNotEmptySpeculativeWorkflowTask_AcceptComplete
	// covers TestSpeculativeWorkflowTask_ScheduleToStartTimeoutOnNormalTaskQueue [TODO: assert]
	t.Run("Update completes", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft1,
				Commands: Select(s, "complete first WFT",
					Option("with an empty command list.", []Arbitrary[*commandpb.Command]{}),
					Option("with update unrelated command", GenList(ScheduleActivityTaskCommand{TaskQueue: tq}))),
			})

			upd := ActStart(usr, UpdateWorkflowExecution{
				WorkflowExecution: wfe,
				WaitStage:         UpdateWaitStages})

			if s.Maybe("sleep 5+ seconds to make sure speculative workflow task time out has passed") {
				// TODO: use fault injection instead
				time.Sleep(5*time.Second + 100*time.Millisecond) //nolint:forbidigo
			}

			wft2 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft2,
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
	t.Run("Update is deduped", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wkr.DrainWorkflowTask(tq)

			upd1 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			if s.Maybe("send 2nd update with same ID *before* task is scheduled") {
				ActStart(usr, UpdateWorkflowExecution{
					WorkflowExecution: wfe,
					UpdateID:          GenJust(upd1.GetID())})
			}

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			if s.Maybe("send 2nd update with same ID *after* task was scheduled") {
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
	t.Run("Updates are ordered", func(t *testing.T) {
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

	// covers TestFirstNormalScheduledWorkflowTask_Reject [TODO: assert]
	t.Run("Update is rejected", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
			usr.PollUpdateUntilAdmitted(upd)

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			Act(wkr, RespondWorkflowTaskCompleted{
				Task:     wft,
				Messages: GenList(WorkflowUpdateRejectionMessage{Update: upd})})

			s.Await(&upd.Rejected)
		})
	})

	// covers TestEmptySpeculativeWorkflowTask_Reject [TODO: assert]
	// covers TestNotEmptySpeculativeWorkflowTask_Reject [TODO: assert]
	// covers TestSpeculativeWorkflowTask_Heartbeat [TODO: assert]
	t.Run("Update is rejected", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			if s.Maybe("heartbeat") {
				wkr.HeartbeatWorkflowTask(tq, wft1)
			}

			Act(wkr, RespondWorkflowTaskCompleted{
				Task: wft1,
				Commands: Select(s, "complete first WFT",
					Option("with an empty command list.", []Arbitrary[*commandpb.Command]{}),
					Option("with update unrelated command", GenList(ScheduleActivityTaskCommand{TaskQueue: tq}))),
			})

			upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
			usr.PollUpdateUntilAdmitted(upd)

			wft2 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			Act(wkr, RespondWorkflowTaskCompleted{
				Task:     wft2,
				Messages: GenList(WorkflowUpdateRejectionMessage{Update: upd})})

			s.Await(&upd.Rejected)

			Act(usr, SignalWorkflowExecution{WorkflowExecution: wfe})

			wkr.CompleteWorkflow(tq)
		})
	})

	// covers TestStartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal [TODO: assert]
	// covers TestScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal [TODO: assert]
	t.Run("Update is rejected", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wkr.DrainWorkflowTask(tq)

			upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
			usr.PollUpdateUntilAdmitted(upd)

			wft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			Act(usr, SignalWorkflowExecution{WorkflowExecution: wfe})

			wft2 := Act(wkr, RespondWorkflowTaskCompleted{
				Task:                  wft1,
				ReturnNewWorkflowTask: GenJust(true),
				Messages:              GenList(WorkflowUpdateRejectionMessage{Update: upd})})

			s.Await(&upd.Rejected)

			Act(wkr, RespondWorkflowTaskCompleted{Task: wft2})
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
			usr.PollUpdateUntilAdmitted(upd)

			if s.Maybe("terminate *before* Update is scheduled") {
				Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})
				s.Await(&upd.Aborted)
				return
			}

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			// terminate *after* Update is scheduled
			Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})

			_, _ = TryAct(wkr, RespondWorkflowTaskCompleted{
				Task: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: upd},
					WorkflowUpdateCompletionMessage{Update: upd}),
			})
			// TODO: verify error type/message

			s.Await(&upd.Aborted)
		})
	})

	// covers TestScheduledSpeculativeWorkflowTask_LostUpdate
	// TODO: TestStartedSpeculativeWorkflowTask_LostUpdate
	t.Run("Update is lost", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, _, tq, usr, wkr := ts.NewWorkflowStack(s,
				ClusterConfig{Key: dynamicconfig.ShardFinalizerTimeout, Val: 0})

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wkr.DrainWorkflowTask(tq)

			ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe}, WithActTimeout(common.MinLongPollTimeout))

			loseNonDurableUpdates(c, wfe) // make speculative WFT and update registry disappear

			_, _ = TryAct(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			// TODO: check error type/message

			Act(usr, SignalWorkflowExecution{WorkflowExecution: wfe})

			Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		})
	})

	// covers TestContinueAsNew_Suggestion [TODO: assert]
	t.Run("Update suggests CAN", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, ns, tq, usr, wkr := ts.NewWorkflowStack(s)

			Act(c, SetNamespaceConfig[float64]{
				NS:  ns,
				Key: dynamicconfig.WorkflowExecutionMaxTotalUpdatesSuggestContinueAsNewThreshold,
				Val: 1e-8}) // force CAN suggestion for 1st update

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wkr.DrainWorkflowTask(tq)

			ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			s.Ensure(&wft.SuggestedContinueAsNew)
		})
	})

	// covers TestContinueAsNew_UpdateIsNotCarriedOver
	t.Run("Update CAN", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			upd1 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

			upd2 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			Act(wkr, RespondWorkflowTaskCompleted{
				Task:     wft,
				Messages: GenList(WorkflowUpdateAcceptMessage{Update: upd1}),
				Commands: GenList(ContinueAsNewWorkflowCommand{}),
			})

			s.Await(&upd1.Failed)
			s.Await(&upd2.Aborted)
		})
	})

	t.Run("Update fails", func(t *testing.T) {

		// covers TestLastWorkflowTask_HasUpdateMessage
		t.Run("when accepted but workflow completes", func(t *testing.T) {
			ts.Run(t, func(s *Scenario) {
				_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

				wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
				upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

				wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft,
					Messages: GenList(WorkflowUpdateAcceptMessage{Update: upd}),
					Commands: GenList(CompleteWorkflowExecutionCommand{}),
				})

				s.Await(&upd.Failed)
			})
		})
	})
}

// Simulating an unexpected loss of the update registry due to a crash. The shard finalizer won't run.
// Therefore, the workflow context is NOT cleared, pending update requests are NOT aborted and will time out.
func loseNonDurableUpdates(cluster *Cluster, wfe *model.WorkflowExecution) {
	cluster.CloseShard(wfe)
}
