package roundtrip

import (
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
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

// TestActivityPauseResume covers pausing an activity and resuming it.
//
// Pausing is the only operation that removes an activity from task generation without
// completing it: refreshTasksForActivity skips paused activities before deciding what to
// dispatch, and LoadAndSortActivityTimers skips them before building the timer sequence. So a
// paused activity should own nothing on either side, and resuming should bring both back.
func (s *rtSuite) TestActivityPauseResume() {
	const activityID = "act-paused"

	s.runCase(rtCase{
		name: "ActivityPauseResume",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{
				name: "schedule-activity",
				fn: rtScheduleActivityNamed(activityID, rtActivityTimeouts{
					scheduleToStart: 5 * time.Minute,
					scheduleToClose: 30 * time.Minute,
					startToClose:    10 * time.Minute,
				}, 5),
			},

			// Paused while still queued, so it should stop being dispatched and stop owning a
			// timer. It is the only pending activity, so nothing else takes over.
			rtStep{
				name:         "pause-activity",
				fn:           rtPauseActivityNamed(activityID),
				allowNoTasks: true,
				forbidActive: []string{"*tasks.ActivityTimeoutTask", "*tasks.ActivityTask"},
			},

			// Resuming a still-queued activity regenerates its retry task so it goes back to
			// the task queue, and the timer sequence starts covering it again.
			rtStep{
				name:          "unpause-activity",
				fn:            rtUnpauseActivityNamed(activityID),
				requireActive: []string{"*tasks.ActivityRetryTimerTask"},
				// Scoped to this step rather than made global: a blanket waiver for a
				// passive-only ActivityTask would blind every other activity case to a
				// missing dispatch.
				allow: []rtAllowRule{rtUnpauseDispatchDefect},
			},

			rtStep{
				name:         "start-activity",
				fn:           rtStartActivityNamed(activityID),
				allowNoTasks: true,
			},
		),
	})
}

// rtUnpauseDispatchDefect is declared separately so the reason can be written out at length
// without burying the step it applies to.
var rtUnpauseDispatchDefect = rtAllowRule{
	name:        "DEFECT-passive-dispatches-unpaused-activity-immediately",
	knownDefect: true,
	reason: "Unpausing a still-queued activity makes the two sides dispatch it differently. " +
		"The active side calls RegenerateActivityRetryTask, which moves ScheduledTime to the " +
		"computed retry time and emits an ActivityRetryTimerTask, so dispatch waits for that " +
		"timer. The passive side re-derives dispatch in refreshTasksForActivity, which chooses " +
		"between GenerateActivityRetryTasks and GenerateActivityTasks purely on `Attempt > 1`. " +
		"An unpaused activity is still on attempt 1, so the passive side emits an immediate " +
		"transfer ActivityTask instead.\n" +
		"\n" +
		"The scheduling intent is present in the replicated state -- ScheduledTime is in the " +
		"future -- but the branch never looks at it. With jitter 0 (as here) the two are nearly " +
		"equivalent because the retry timer fires at once. The harm shows up with " +
		"UnpauseActivity's jitter parameter, which exists to spread dispatch when many " +
		"activities are unpaused together: the passive cluster discards it, so after a failover " +
		"they all dispatch at once.",
	ref: "service/history/workflow/task_refresher.go refreshTasksForActivity (Attempt > 1 " +
		"branch) vs mutable_state_impl.go RegenerateActivityRetryTask; jitter comes from " +
		"service/history/api/unpauseactivity/api.go",
	match: func(side rtSide, identity string) bool {
		return (side == rtPassive && strings.HasPrefix(identity, "*tasks.ActivityTask")) ||
			(side == rtActive && strings.HasPrefix(identity, "*tasks.ActivityRetryTimerTask"))
	},
}

// TestActivityUpdateOptions changes a running activity's timeouts and checks whether the
// timeout task is recreated.
//
// UpdateActivityOptions clears TimerTaskStatus outright -- the API comments it as
// "invalidate timers" -- so the answer is that a timeout task is always recreated, and the
// question worth asking is *which* one. Clearing the mask makes the timer sequence pick the
// earliest deadline again from scratch, so changing an option can hand ownership to a
// different timeout type entirely.
//
// The three subcases are chosen so the recreated task differs each time. They also cover both
// directions of a deadline move, since shrinking and growing a timeout are not symmetric for
// a sequence that only ever covers the earliest deadline.
//
// The contrast case for all of this is the heartbeat step in TestConcurrentActivities: a
// heartbeat updates the activity without clearing the mask or moving a deadline, and there no
// task may be recreated.
func (s *rtSuite) TestActivityUpdateOptions() {
	const activityID = "act-updated"

	// Started with start-to-close as the live deadline and no heartbeat configured.
	baseTimeouts := rtActivityTimeouts{
		scheduleToStart: 5 * time.Minute,
		scheduleToClose: 30 * time.Minute,
		startToClose:    10 * time.Minute,
	}

	updateCase := func(
		name string,
		apply func(*persistencespb.ActivityInfo),
		expectTimeoutType enumspb.TimeoutType,
	) rtCase {
		return rtCase{
			name: name,
			steps: append(rtStartedWorkflowSteps(),
				rtStep{
					name: "schedule-activity",
					fn:   rtScheduleActivityNamed(activityID, baseTimeouts, 5),
				},
				rtStep{
					name:          "start-activity",
					fn:            rtStartActivityNamed(activityID),
					requireActive: []string{"*tasks.ActivityTimeoutTask"},
				},
				rtStep{
					name: "update-options",
					fn:   rtUpdateActivityOptionsNamed(activityID, apply),
					// Assert both that a timeout task was recreated and which deadline now
					// owns it. The diff alone would only prove the two sides agree on the
					// type, not that it is the right one.
					requireActive: []string{
						"*tasks.ActivityTimeoutTask",
						fmt.Sprintf("timeoutType:%v", expectTimeoutType),
					},
				},
			),
		}
	}

	for _, tc := range []rtCase{
		// Shrinking the live deadline: start-to-close stays the earliest, so the recreated
		// task is another start-to-close, just sooner.
		updateCase("ShortenStartToClose", func(ai *persistencespb.ActivityInfo) {
			ai.StartToCloseTimeout = durationpb.New(time.Minute)
		}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE),

		// Growing it past schedule-to-close hands ownership to schedule-to-close instead, so
		// the recreated task is a different timeout type.
		updateCase("LengthenStartToClosePastScheduleToClose", func(ai *persistencespb.ActivityInfo) {
			ai.StartToCloseTimeout = durationpb.New(time.Hour)
		}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),

		// Adding a heartbeat timeout introduces a new, earlier deadline on an activity that
		// had none, so the recreated task is a heartbeat timeout.
		updateCase("AddHeartbeatTimeout", func(ai *persistencespb.ActivityInfo) {
			ai.HeartbeatTimeout = durationpb.New(10 * time.Second)
		}, enumspb.TIMEOUT_TYPE_HEARTBEAT),
	} {
		s.SetupTest() // each subcase needs its own pair of clusters
		s.runCase(tc)
		s.TearDownTest()
	}
}
