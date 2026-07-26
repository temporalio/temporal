package model

import (
	"go.temporal.io/server/common/testing/umpire"
)

// This file models the *standalone activity* archetype — the same archetype the SAA branch
// models (SAA = Standalone Activity Archetype; see UMPIRE_PRIOR_ART.md). Like that model it
// is pure and server-free: a Lifecycle plus a total transition function
// (ActivityTransition) predicting, for every (config, abstract state, event), the next
// abstract state, the rejection, and the observable side effects. It is not (yet) wired as
// an observed Entity — it exists to plan over and to validate statically. When observed, the
// public lifecycle (scheduled/started/terminal) is black-box via ActivityTask* history
// events; only the backing_off/attempt retry loop needs internal signals.

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

// ActivityConfig is the start-time configuration transitions branch on (SAA's Config).
type ActivityConfig struct {
	MaxAttempts int // 0 = unlimited; exhausting it turns a retryable failure terminal
}

// ActivityAbstract is the observable abstract state: lifecycle state + attempt count.
type ActivityAbstract struct {
	State   string
	Attempt int
}

// ActivityReject is the API/validation error an illegal event would produce.
type ActivityReject string

const (
	ActivityRejectNone         ActivityReject = ""
	ActivityRejectPrecondition ActivityReject = "FailedPrecondition"
)

// ActivityOutcome is the full predicted contract of applying an event (mirrors NexusOutcome).
type ActivityOutcome struct {
	Kind         umpire.TransitionKind
	From, Next   ActivityAbstract
	Reject       ActivityReject
	AttemptDelta int
	BackoffArmed bool
	Terminal     bool
}

// ActivityTransition is the total transition function for the standalone-activity archetype.
// As with NexusTransition, the lifecycle-state part is delegated to the generic Lifecycle's
// Classify (so they can't disagree); this layer adds the retry/attempt side effects, the
// reject kind on illegal edges, and the config-dependent budget-exhaustion branch.
func ActivityTransition(cfg ActivityConfig, cur ActivityAbstract, event string) ActivityOutcome {
	lc := NewActivityLifecycle()
	lc.SetState(cur.State)
	base := lc.Classify(event)

	out := ActivityOutcome{From: cur, Next: cur, Kind: base.Kind}
	switch base.Kind {
	case umpire.Illegal:
		out.Reject = ActivityRejectPrecondition
		out.Terminal = activityIsTerminal(cur.State)
		return out
	case umpire.NoOp:
		out.Terminal = activityIsTerminal(cur.State)
		return out
	}

	out.Next.State = base.To
	switch event {
	case ActivitySchedule:
		out.Next.Attempt = cur.Attempt + 1
		out.AttemptDelta = 1
	case ActivityAttemptFailed:
		if cfg.MaxAttempts > 0 && cur.Attempt >= cfg.MaxAttempts {
			out.Next.State = ActivityFailed
		} else {
			out.BackoffArmed = true
		}
	}
	out.Terminal = activityIsTerminal(out.Next.State)
	return out
}

func activityIsTerminal(state string) bool {
	switch state {
	case ActivityCompleted, ActivityFailed, ActivityTimedOut, ActivityCanceled:
		return true
	default:
		return false
	}
}
