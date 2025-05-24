package acceptance

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkerVersioning(t *testing.T) {
	ts := NewTestSuite(t)

	// TODO: crossTq
	t.Run("Child workflows inherit the version", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, ns, tq1, usr, wkr := ts.NewWorkflowStack(s)
			c.SetDynamicConfig(ns, dynamicconfig.EnableDeploymentVersions, true)
			//tq2 := Act(c, CreateTaskQueue{Namespace: ns})
			dpl := Act(c, CreateWorkerDeployment{Namespace: ns})
			dvrA := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("A")})
			dvrB := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("B")})

			// TODO: PollWorkflowTaskQueueRequest does not return; instead, it seems
			// its action is propagated to the internal worker versioning workflow, too

			// Make version A current
			wft := ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq1, Version: dvrA})
			s.Await(&dvrA.IsCurrent, OnRetry(func() {
				Act(c, SetWorkerDeploymentCurrentVersion{Version: dvrA}, IgnoreErr())
			}))

			Act(usr, StartWorkflowExecution{TaskQueue: tq1})

			// Start 1st child workflow
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: wft,
				Commands:     GenList(StartChildWorkflowExecutionCommand{WorkflowId: GenJust[ID]("child1")}),
			})

			// Complete 1st child workflow
			cwft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq1, Version: dvrA})
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: cwft1,
				Commands:     GenList(CompleteWorkflowExecutionCommand{}),
			})

			// Make version B current
			ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq1, Version: dvrB})
			s.Await(&dvrB.IsCurrent, OnRetry(func() {
				Act(c, SetWorkerDeploymentCurrentVersion{Version: dvrB}, IgnoreErr())
			}))

			// Start 2nd child workflow
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: wft,
				Commands:     GenList(StartChildWorkflowExecutionCommand{WorkflowId: GenJust[ID]("child2")}),
			})

			// Complete 2nd child workflow
			cwft2 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq1, Version: dvrB})
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: cwft2,
				Commands:     GenList(CompleteWorkflowExecutionCommand{}),
			})
		})
	})
}
