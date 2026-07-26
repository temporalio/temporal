package model

import (
	"go.temporal.io/server/common/testing/umpire"
)

// This file models the *standalone activity* archetype — the same archetype the SAA branch
// models (SAA = Standalone Activity Archetype; see UMPIRE_PRIOR_ART.md). It is pure and
// server-free: a Lifecycle to plan over and validate statically, not (yet) wired as an
// observed Entity. When observed, the public lifecycle (scheduled/started/terminal) is
// black-box via ActivityTask* history events; only the backing_off/attempt retry loop needs
// internal signals.

type (
	ActivityState = string
	ActivityEvent = string
)

const (
	ActivityUnspecified ActivityState = "unspecified"
	ActivityScheduled   ActivityState = "scheduled"
	ActivityStarted     ActivityState = "started"
	ActivityBackingOff  ActivityState = "backing_off"
	ActivityCompleted   ActivityState = "completed"
	ActivityFailed      ActivityState = "failed"
	ActivityTimedOut    ActivityState = "timed_out"
	ActivityCanceled    ActivityState = "canceled"

	ActivitySchedule      ActivityEvent = "schedule"
	ActivityStart         ActivityEvent = "start"
	ActivityComplete      ActivityEvent = "complete"
	ActivityAttemptFailed ActivityEvent = "attempt_failed"
	ActivityFail          ActivityEvent = "fail"
	ActivityTimeout       ActivityEvent = "timeout"
	ActivityCancel        ActivityEvent = "cancel"
)

// NewActivityLifecycle builds the standalone-activity state machine: an activity is
// scheduled, starts, and either completes or — on a retryable failure — backs off and is
// rescheduled (bumping the attempt count); it can also fail, time out, or be canceled from
// the live states.
func NewActivityLifecycle() *umpire.Lifecycle {
	return umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: ActivityUnspecified,
		// The in-flight states must eventually settle; terminals derive from the graph.
		States: umpire.States{
			ActivityUnspecified: {},
			ActivityScheduled:   {umpire.MustProgress},
			ActivityStarted:     {umpire.MustProgress},
			ActivityBackingOff:  {umpire.MustProgress},
			ActivityCompleted:   {},
			ActivityFailed:      {},
			ActivityTimedOut:    {},
			ActivityCanceled:    {},
		},
		Transitions: []umpire.Transition{
			// schedule fires initially and again on each retry out of backing_off.
			{
				Event: ActivitySchedule,
				From:  []string{ActivityUnspecified, ActivityBackingOff},
				To:    ActivityScheduled,
			},
			{
				Event: ActivityStart,
				From:  []string{ActivityScheduled},
				To:    ActivityStarted,
			},
			{
				Event: ActivityComplete,
				From:  []string{ActivityStarted},
				To:    ActivityCompleted,
			},
			// attempt_failed: a retryable failure sends a started attempt into backoff.
			{
				Event: ActivityAttemptFailed,
				From:  []string{ActivityStarted},
				To:    ActivityBackingOff,
			},
			{
				Event: ActivityFail,
				From:  []string{ActivityScheduled, ActivityStarted},
				To:    ActivityFailed,
			},
			{
				Event: ActivityTimeout,
				From:  []string{ActivityScheduled, ActivityStarted, ActivityBackingOff},
				To:    ActivityTimedOut,
			},
			{
				Event: ActivityCancel,
				From:  []string{ActivityScheduled, ActivityStarted, ActivityBackingOff},
				To:    ActivityCanceled,
			},
		},
	})
}
