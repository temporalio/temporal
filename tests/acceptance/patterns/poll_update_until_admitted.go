package patterns

import (
	enumspb "go.temporal.io/api/enums/v1"
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func PollUpdateUntilAdmitted(
	s *Scenario,
	upd *model.WorkflowUpdate,
	usr *testenv.WorkflowClient,
) {
	s.Await(&upd.Admitted, OnRetry(func() {
		_, _ = TryAct(usr, PollWorkflowExecutionUpdate{
			WorkflowUpdate: upd,
			WaitStage:      GenJust(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED)})
	}))
}
