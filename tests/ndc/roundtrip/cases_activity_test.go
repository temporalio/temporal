package roundtrip

import "fmt"

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
