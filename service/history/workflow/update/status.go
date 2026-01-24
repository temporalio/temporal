package update

import (
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/proto"
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
		Outcome: updatepb.Outcome_builder{
			Failure: proto.ValueOrDefault(rejection),
		}.Build(),
	}
}

func statusCompleted(outcome *updatepb.Outcome) *Status {
	return &Status{
		Stage:   enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: outcome,
	}
}
