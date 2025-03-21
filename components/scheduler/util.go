// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

// loadScheduler loads the Scheduler's persisted state.
func loadScheduler(node *hsm.Node) (Scheduler, error) {
	prevScheduler, err := hsm.MachineData[Scheduler](node)
	if err != nil {
		return Scheduler{}, err
	}

	return Scheduler{
		SchedulerInternal:  common.CloneProto(prevScheduler.SchedulerInternal),
		cacheConflictToken: prevScheduler.cacheConflictToken,
		compiledSpec:       prevScheduler.compiledSpec,
	}, nil
}
