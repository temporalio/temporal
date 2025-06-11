package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
)

// validateTaskTransition ensures that the given transition is possible with the current
// state machine state.
func validateTaskTransition[
	S comparable,
	SM hsm.StateMachine[S],
	E any](node *hsm.Node, transition hsm.Transition[S, SM, E]) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	sm, err := hsm.MachineData[SM](node)
	if err != nil {
		return err
	}
	if !transition.Possible(sm) {
		return fmt.Errorf(
			"%w: %w: cannot transition from state %v to %v",
			consts.ErrStaleReference,
			hsm.ErrInvalidTransition,
			sm.State(),
			transition.Destination,
		)
	}
	return nil
}

// generateRequestID generates a deterministic request ID for a buffered action's
// time. The request ID is deterministic because the jittered actual time (as
// well as the spec's nominal time) is, in turn, also deterministic.
//
// backfillID should be left blank for actions that are being started
// automatically, based on the schedule spec. It must be set for backfills,
// as backfills may generate buffered actions that overlap with both
// automatically-buffered actions, as well as other requested backfills.
func generateRequestID(scheduler Scheduler, backfillID string, nominal, actual time.Time) string {
	if backfillID == "" {
		backfillID = "auto"
	}
	return fmt.Sprintf(
		"sched-%s-%s-%s-%d-%d-%d",
		backfillID,
		scheduler.NamespaceId,
		scheduler.ScheduleId,
		scheduler.ConflictToken,
		nominal.UnixMilli(),
		actual.UnixMilli(),
	)
}

// newTaggedLogger returns a logger tagged with the Scheduler's attributes.
func newTaggedLogger(baseLogger log.Logger, scheduler Scheduler) log.Logger {
	return log.With(
		baseLogger,
		tag.NewStringTag("wf-namespace", scheduler.Namespace),
		tag.NewStringTag("schedule-id", scheduler.ScheduleId),
	)
}

// loadScheduler loads the Scheduler's persisted state. When clone is true,
// mutable structs are cloned before being returned. Set clone to true unless the
// caller is already holding the workflow lock.
func loadScheduler(node *hsm.Node, clone bool) (Scheduler, error) {
	prevScheduler, err := hsm.MachineData[Scheduler](node)
	if err != nil {
		return Scheduler{}, err
	}

	schedulerInternal := prevScheduler.SchedulerInternal
	if clone {
		schedulerInternal = common.CloneProto(schedulerInternal)
	}

	return Scheduler{
		SchedulerInternal:  schedulerInternal,
		cacheConflictToken: prevScheduler.cacheConflictToken,
		compiledSpec:       prevScheduler.compiledSpec,
	}, nil
}
