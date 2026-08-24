package roundtrip

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// Cases for activities.

// TestActivityLifecycle covers the schedule/start/complete path. Activity timers are the
// most likely place for active and passive to diverge: the active cluster generates them in
// closeTransactionHandleActivityUserTimerTasks at close-transaction time, while the passive
// cluster derives them from CreateNextActivityTimer during the task refresh.
func (s *rtSuite) TestActivityLifecycle() {
	s.runCase(rtCase{
		name: "ActivityLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "schedule-activity", fn: rtScheduleActivity},
			rtStep{name: "start-activity", fn: rtStartActivity},
			rtStep{name: "complete-activity", fn: rtCompleteActivity, allowNoTasks: true},
		),
	})
}

// TestActivityRetries drives an activity through rtActivityRetries failed attempts,
// replicating after each one.
//
// Each attempt moves the activity's Attempt and Stamp and re-anchors its schedule-to-start
// deadline, which is exactly what getActivityTimerTaskStatus inspects when deciding which
// timer task mask bits survive replication. It is also the one lifecycle path where the
// dispatch task changes shape: a first attempt goes out as a transfer ActivityTask, while a
// retry goes out as a timer ActivityRetryTimerTask.
//
// A long sequence is worth having on its own: anything that accumulates per replicated
// delta shows up as a growing divergence here even when a single step looks clean.
func (s *rtSuite) TestActivityRetries() {
	steps := append(rtStartedWorkflowSteps(),
		rtStep{name: "schedule-retryable-activity", fn: rtScheduleRetryableActivity},
	)
	for attempt := 1; attempt <= rtActivityRetries; attempt++ {
		steps = append(steps,
			rtStep{
				name: fmt.Sprintf("start-attempt-%02d", attempt),
				fn:   rtStartActivity,
			},
			rtStep{
				name: fmt.Sprintf("fail-attempt-%02d", attempt),
				fn:   rtFailActivityWithRetry,
			},
		)
	}
	s.runCase(rtCase{name: "ActivityRetries", steps: steps})
}

// TestConcurrentActivities runs three activities at once with deliberately different
// deadlines, then times one out and lets it retry.
//
// The point is that only ONE ActivityTimeoutTask exists at a time across the whole workflow:
// CreateNextActivityTimer walks every pending activity's four candidate timers, sorts them,
// and covers the single earliest one that is not already covered. So which activity owns the
// live timer task changes as activities start, time out and retry -- and the two sides decide
// that independently. The active cluster runs GenerateActivityTimerTasks at
// close-transaction time; the passive cluster re-derives the whole sequence in
// refreshTasksForActivity, using the per-activity mask that getActivityTimerTaskStatus
// carried over.
//
// The three activities are shaped so ownership actually moves:
//
//	act-queued  schedule-to-start 2m   never started, so its schedule-to-start stays live
//	act-fast    start-to-close 30s     shortest once started, so it takes ownership
//	act-slow    start-to-close 15m     never the earliest, so it should never own the timer
//
// act-fast then times out on start-to-close, which retries (unlike a schedule-to-start or
// schedule-to-close timeout, which would close the activity instead). That both re-anchors
// its deadline and moves its attempt and stamp, which is what getActivityTimerTaskStatus
// inspects when deciding whether the replicated mask still describes a valid task.
func (s *rtSuite) TestConcurrentActivities() {
	const (
		queued = "act-queued"
		fast   = "act-fast"
		slow   = "act-slow"
	)

	s.runCase(rtCase{
		name: "ConcurrentActivities",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{
				name: "schedule-act-queued",
				fn: rtScheduleActivityNamed(queued, rtActivityTimeouts{
					scheduleToStart: 2 * time.Minute,
					scheduleToClose: 30 * time.Minute,
					startToClose:    10 * time.Minute,
				}, 5),
			},
			rtStep{
				name: "schedule-act-fast",
				fn: rtScheduleActivityNamed(fast, rtActivityTimeouts{
					scheduleToStart: 10 * time.Minute,
					scheduleToClose: 30 * time.Minute,
					startToClose:    30 * time.Second,
				}, 5),
			},
			rtStep{
				name: "schedule-act-slow",
				fn: rtScheduleActivityNamed(slow, rtActivityTimeouts{
					scheduleToStart: 10 * time.Minute,
					scheduleToClose: 30 * time.Minute,
					startToClose:    15 * time.Minute,
				}, 5),
			},

			// Starting act-fast retires its schedule-to-start and brings its 30s
			// start-to-close deadline live, which is earlier than anything else pending.
			rtStep{
				name:          "start-act-fast",
				fn:            rtStartActivityNamed(fast),
				requireActive: []string{"*tasks.ActivityTimeoutTask"},
			},
			rtStep{
				name:         "start-act-slow",
				fn:           rtStartActivityNamed(slow),
				allowNoTasks: true,
			},

			// A heartbeat on the activity that currently owns the timer task, with no
			// heartbeat timeout configured. Nothing about act-fast's deadlines moves, so its
			// replicated mask must be honoured and no new timeout task should appear on
			// either side. This is the step that covers getActivityTimerTaskStatus: break the
			// carryover and the passive cluster duplicates act-fast's start-to-close timer.
			rtStep{
				name:         "heartbeat-act-fast",
				fn:           rtHeartbeatActivityNamed(fast),
				allowNoTasks: true,
				forbidActive: []string{"*tasks.ActivityTimeoutTask"},
			},

			// act-fast times out and retries: a new attempt, a new stamp, and an
			// ActivityRetryTimerTask to put it back on the task queue.
			rtStep{
				name:          "timeout-act-fast",
				fn:            rtTimeoutActivityNamed(fast, enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
				requireActive: []string{"*tasks.ActivityRetryTimerTask"},
			},
			rtStep{
				name:         "start-act-fast-attempt-2",
				fn:           rtStartActivityNamed(fast),
				allowNoTasks: true,
			},

			rtStep{
				name:         "complete-act-slow",
				fn:           rtCompleteActivityNamed(slow),
				allowNoTasks: true,
			},
		),
	})
}
