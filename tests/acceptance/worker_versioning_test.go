package acceptance

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkerVersioning(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Start child workflow", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, ns, tq, usr, wkr := ts.NewWorkflowStack(s)
			Act(c, SetNamespaceConfig[bool]{NS: ns, Key: dynamicconfig.EnableDeploymentVersions, Val: true})

			dpl := Act(c, CreateWorkerDeployment{Namespace: ns})
			dvrA := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("A")})
			dvrB := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("B")})

			// Make version "A" current
			wftA := ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq, Version: dvrA})
			s.Await(&dvrA.IsCurrent, OnRetry(func() {
				_, _ = TryAct(c, SetWorkerDeploymentCurrentVersion{Version: dvrA})
			}))

			// Start workflow
			Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wftA.Await()

			// Make version "B" current
			wftB := ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq, Version: dvrB})
			s.Await(&dvrB.IsCurrent, OnRetry(func() {
				_, _ = TryAct(c, SetWorkerDeploymentCurrentVersion{Version: dvrB})
			}))
			wftB.Cancel()

			// Start child workflow
			childTaskQueue := GenEnum[*model.TaskQueue]("child task queue",
				tq,                                     // TODO: add labels
				Act(c, CreateTaskQueue{Namespace: ns}), // TODO: do lazy eval
			).Next(s)
			wft := Act(wkr, RespondWorkflowTaskCompleted{
				Task:               wftA.Await(),
				Commands:           GenList(StartChildWorkflowExecutionCommand{TaskQueue: childTaskQueue}),
				Version:            dvrA,
				VersioningBehavior: AnyVersionBehavior,
			})

			// Complete child workflow
			pullChildWorkflowVersion := dvrB
			if childTaskQueue == tq && wft.Response.VersioningBehavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
				pullChildWorkflowVersion = dvrA
			}
			cwft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: childTaskQueue, Version: pullChildWorkflowVersion})
			Act(wkr, RespondWorkflowTaskCompleted{
				Task:     cwft,
				Commands: GenList(CompleteWorkflowExecutionCommand{}),
			})
		})
	})
}
