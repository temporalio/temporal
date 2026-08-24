package roundtrip

// Cases for child workflows.

// TestChildWorkflowLifecycle covers a pending child, whose transfer task both sides derive
// from pending child info.
func (s *rtSuite) TestChildWorkflowLifecycle() {
	s.runCase(rtCase{
		name: "ChildWorkflowLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "start-child-workflow", fn: rtStartChildWorkflow},
		),
	})
}
