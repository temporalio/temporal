package update

import (
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	updatepb "go.temporal.io/api/update/v1"
)

type (
	// Status describes the current status of Update from Registry.
	Status struct {
		// The most advanced stage reached by Update.
		Stage enumspb.UpdateWorkflowExecutionLifecycleStage
		// Outcome of Update if it is completed or rejected.
		Outcome *updatepb.Outcome
	}
)

func statusAdmitted() *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
	}
}

func statusAccepted() *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
	}
}

func statusRejected(rejection *failurepb.Failure) *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: &updatepb.Outcome{
			Value: &updatepb.Outcome_Failure{Failure: rejection},
		},
	}
}

func statusCompleted(outcome *updatepb.Outcome) *Status {
	return &Status{
		Stage:   enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: outcome,
	}
}
