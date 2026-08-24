package roundtrip

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
