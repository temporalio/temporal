package common

import (
	"context"
	"fmt"
	"time"

	servercommon "go.temporal.io/server/common"
	"go.temporal.io/server/components/scheduler2/core"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
)

func ValidateTask[
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

// Intended to be called with a sub state machine's node. Returns a cloned copy
// of the top-level Scheduler from its persisted state.
func LoadSchedulerFromParent(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref) (scheduler core.Scheduler, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		prevScheduler, err := hsm.MachineData[core.Scheduler](node.Parent)
		if err != nil {
			return err
		}
		scheduler = core.Scheduler{
			SchedulerInternal: servercommon.CloneProto(prevScheduler.SchedulerInternal),
		}
		return nil
	})
	return
}

// Generates a deterministic request ID for a buffered action's time. The request
// ID is deterministic because the jittered actual time (as well as the spec's
// nominal time) is, in turn, also deterministic.
//
// backfillID should be left blank for actions that are being started
// automatically, based on the schedule spec. It must be set for backfills,
// as backfills may generate buffered actions that overlap with both
// automatically-buffered actions, as well as other requested backfills.
func GenerateRequestID(scheduler core.Scheduler, backfillID string, nominal, actual time.Time) string {
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
