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
	"go.temporal.io/server/tests/acceptance/patterns"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

// TODO: TestRunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected
// TODO: TestRunningWorkflowTask_NewNotEmptySpeculativeWorkflowTask_Rejected
// TODO: TestValidateWorkerMessages
// TODO: TestSpeculativeWorkflowTask_Fail
// TODO: TestSpeculativeWorkflowTask_StartToCloseTimeout
// TODO: TestCompleteWorkflow_AbortUpdates
// TODO: TestFirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared
// TODO: TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId
// TODO: TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime
// TODO: TestStaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates
// TODO: TestSpeculativeWorkflowTask_WorkerSkippedProcessing_RejectByServer
// TODO: TestSpeculativeWorkflowTask_QueryFailureClearsWFContext
func TestWorkflowUpdate(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Update completes", func(t *testing.T) {

		// covers TestCompletedWorkflow
		// covers TestFirstNormalScheduledWorkflowTask_AcceptComplete
		// covers TestNormalScheduledWorkflowTask_AcceptComplete
		// covers TestWaitAccepted_GotCompleted [TODO: check marker]
		// covers TestEmptySpeculativeWorkflowTask_AcceptComplete
		// covers TestNotEmptySpeculativeWorkflowTask_AcceptComplete
		// covers TestStickySpeculativeWorkflowTask_AcceptComplete
		// covers TestSpeculativeWorkflowTask_ScheduleToStartTimeoutOnNormalTaskQueue [TODO: assert]
		// TODO: TestStickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable
		t.Run("accepted and completed at once", func(t *testing.T) {
			ts.Run(t, func(s *Scenario) {
				_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

				wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

				var useSpeculativeWFT bool
				if s.Maybe("complete 1st workflow task") {
					wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

					commands := Select(s, "command list",
						Option("empty", []Arbitrary[*commandpb.Command]{}),
						Option("non-empty", GenList(ScheduleActivityTaskCommand{TaskQueue: tq})))

					stickyQueue := Select(s, "stickiness",
						Option("normal task queue", (*StickyTaskQueue)(nil)),
						Option("sticky task queue", &StickyTaskQueue{NormalTaskQueue: tq}))

					Act(wkr, RespondWorkflowTaskCompleted{
						Task:            wft,
						Commands:        commands,
						StickyTaskQueue: stickyQueue,
					})

					useSpeculativeWFT = true // next Update will be delivered with 2nd (speculative) workflow task
				} else {
					// next Update will be delivered with 1st (normal) workflow task
				}

				upd := ActStart(usr, UpdateWorkflowExecution{
					WorkflowExecution: wfe, // TODO: with/without RunID
					WaitStage:         UpdateWaitStages,
				})

				if useSpeculativeWFT && s.Maybe("sleep 5s to time out speculative WFT") {
					// TODO: use fault injection instead
					time.Sleep(5*time.Second + 100*time.Millisecond) //nolint:forbidigo
				}

				patterns.AcceptAndCompleteUpdate(s, upd, wkr, tq)
				s.Await(&upd.Completed)

				Act(usr, PollWorkflowExecutionUpdate{
					WorkflowUpdate: upd,
					WaitStage:      UpdateWaitStages})
			})
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

	t.Run("Update concurrency", func(t *testing.T) {

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

		// TODO: Test1stAccept_2ndAccept_2ndComplete_1stComplete
		// TODO: Test1stAccept_2ndReject_1stComplete
		t.Run("Updates are isolated", func(t *testing.T) {
			t.Skip("TODO")

			ts.Run(t, func(s *Scenario) {
				_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

				wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

				// start 1st update
				upd1 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
				usr.PollUpdateUntilAdmitted(upd1)

				// accept 1st update
				wft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft1,
					Messages: GenList(WorkflowUpdateAcceptMessage{Update: upd1}),
					Commands: GenList(ScheduleActivityTaskCommand{TaskQueue: tq})})

				// start 2nd and 3rd update
				upd2 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
				upd3 := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
				usr.PollUpdateUntilAdmitted(upd2)
				usr.PollUpdateUntilAdmitted(upd3)

				// reject 2nd update; accept 3rd update
				wft2 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task: wft2,
					Messages: GenList(
						WorkflowUpdateRejectionMessage{Update: upd2},
						WorkflowUpdateAcceptMessage{Update: upd3})})

				// complete 1st and 3rd update
				wft3 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task: wft3,
					Messages: GenList(
						WorkflowUpdateAcceptMessage{Update: upd1},
						WorkflowUpdateAcceptMessage{Update: upd3})})

				s.Await(&upd1.Completed)
				s.Await(&upd2.Rejected)
				s.Await(&upd3.Completed)
			})
		})
	})

	t.Run("Update is rejected", func(t *testing.T) {

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
		// covers TestFirstNormalScheduledWorkflowTask_Reject [TODO: assert]
		t.Run("by workflow", func(t *testing.T) {
			ts.Run(t, func(s *Scenario) {
				_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

				wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

				if s.Maybe("complete 1st workflow task") {
					wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})

					if s.Maybe("heartbeat") {
						wkr.HeartbeatWorkflowTask(tq, wft)
					}

					Act(wkr, RespondWorkflowTaskCompleted{
						Task: wft,
						Commands: Select(s, "complete first WFT",
							Option("with an empty command list.", []Arbitrary[*commandpb.Command]{}),
							Option("with update unrelated command", GenList(ScheduleActivityTaskCommand{TaskQueue: tq}))),
					})
					// Next Update will be delivered with 2nd (speculative) workflow task.
				} else {
					// Next Update will be delivered with 1st (normal) workflow task.
				}

				upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
				usr.PollUpdateUntilAdmitted(upd)

				wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft,
					Messages: GenList(WorkflowUpdateRejectionMessage{Update: upd})})

				s.Await(&upd.Rejected)

				Act(usr, SignalWorkflowExecution{WorkflowExecution: wfe})

				wkr.CompleteWorkflow(tq)
			})
		})

		// covers TestStartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal [TODO: assert]
		// covers TestScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal [TODO: assert]
		t.Run("by buffered signal", func(t *testing.T) {
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
	})

	// covers TestScheduledSpeculativeWorkflowTask_LostUpdate
	// TODO: TestStartedSpeculativeWorkflowTask_LostUpdate
	t.Run("Update loss", func(t *testing.T) {
		t.Skip("TODO")

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

	t.Run("Update is aborted", func(t *testing.T) {

		// TODO: TestContinueAsNew_UpdateIsNotCarriedOver
		// covers TestStartedSpeculativeWorkflowTask_TerminateWorkflow [TODO: assert]
		// covers TestScheduledSpeculativeWorkflowTask_TerminateWorkflow [TODO: assert]
		t.Run("when workflow completes during update", func(t *testing.T) {
			t.Skip("TODO")

			ts.Run(t, func(s *Scenario) {
				_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

				wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})
				wkr.DrainWorkflowTask(tq)

				upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})
				usr.PollUpdateUntilAdmitted(upd)

				patterns.CloseWorkflow(s, usr, wkr, wfe, tq)
				// TODO: verify error type/message

				s.Await(&upd.Failed)
			})
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

	t.Run("Update limits", func(t *testing.T) {

		// covers TestContinueAsNew_Suggestion [TODO: assert]
		t.Run("suggest continue-as-new", func(t *testing.T) {
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
	})
}

// Simulating an unexpected loss of the update registry due to a crash. The shard finalizer won't run.
// Therefore, the workflow context is NOT cleared, pending update requests are NOT aborted and will time out.
func loseNonDurableUpdates(cluster *Cluster, wfe *model.WorkflowExecution) {
	cluster.CloseShard(wfe)
}
