package roundtrip

import "time"

// Cases for user timers.

// TestUserTimerLifecycle covers a pending user timer, which is where the earliest-timer-only
// optimization and the task-status mask live.
func (s *rtSuite) TestUserTimerLifecycle() {
	s.runCase(rtCase{
		name: "UserTimerLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "start-timer", fn: rtStartTimer},
		),
	})
}

// TestMultipleUserTimersFirstFires is the case closest to the reason this framework exists.
//
// The server keeps a timer task only for the *earliest pending user timer that does not
// already have one*, and the two sides get there by different routes: the active cluster
// calls GenerateUserTimerTasks at close-transaction time, while the passive cluster
// re-derives it through CreateNextUserTimer during the task refresh, gated on each timer's
// LastUpdateVersionedTransition.
//
// Creation order matters, and the two subcases below are not redundant -- they produce
// genuinely different task sequences:
//
// EarliestCreatedFirst starts 1m, 5m, 10m in that order. Only the 1m timer gets a task; the
// later two are never earliest while uncovered, so they get nothing. Firing the 1m timer is
// then the real handoff: the 5m timer is now earliest with no task of its own, and one is
// created for it.
//
// EarliestCreatedLast starts 5m, then 1m, then 10m. The 5m timer gets a task when it is the
// only one pending, and the 1m timer gets one when it becomes earliest -- so two tasks exist.
// Firing the 1m timer then produces no new timer task at all, because the 5m timer's task is
// still pending and its deadline never moved. Worth pinning: the intuitive reading, that
// something fires a fresh task for the next timer on every expiry, is not what happens.
func (s *rtSuite) TestMultipleUserTimersFirstFires() {
	timerCase := func(name string, order []string, expectHandoffTask bool) rtCase {
		steps := rtStartedWorkflowSteps()
		durations := map[string]time.Duration{
			"timer-early": time.Minute,
			"timer-mid":   5 * time.Minute,
			"timer-late":  10 * time.Minute,
		}
		for i, timerID := range order {
			steps = append(steps, rtStep{
				name: "start-" + timerID,
				fn:   rtStartTimerNamed(timerID, durations[timerID]),
				// Only a timer that becomes the earliest uncovered one produces a task, so
				// every other start step legitimately produces nothing.
				allowNoTasks: i > 0,
			})
		}
		// Firing always schedules a workflow task, so this step is never empty. The question
		// the case is really asking is whether a UserTimerTask accompanies it, so assert
		// that directly rather than relying on the diff -- the diff would happily pass if
		// both sides lost the handoff together.
		fireStep := rtStep{name: "fire-timer-early", fn: rtFireTimer("timer-early")}
		if expectHandoffTask {
			fireStep.requireActive = []string{"*tasks.UserTimerTask"}
		} else {
			fireStep.forbidActive = []string{"*tasks.UserTimerTask"}
		}
		steps = append(steps, fireStep)

		return rtCase{name: name, steps: steps}
	}

	for _, tc := range []rtCase{
		timerCase("EarliestCreatedFirst",
			[]string{"timer-early", "timer-mid", "timer-late"}, true),
		timerCase("EarliestCreatedLast",
			[]string{"timer-mid", "timer-early", "timer-late"}, false),
	} {
		s.SetupTest() // each subcase needs its own pair of clusters
		s.runCase(tc)
		s.TearDownTest()
	}
}
