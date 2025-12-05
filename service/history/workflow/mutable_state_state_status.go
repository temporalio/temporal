package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	// indicate invalid workflow state transition
	invalidStateTransitionMsg = "unable to change workflow state from %v to %v, status %v"
)

// setStateStatus sets state and status in WorkflowExecutionState.
func setStateStatus(
	e *persistencespb.WorkflowExecutionState,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {
	switch e.GetState() {
	case enumsspb.WORKFLOW_EXECUTION_STATE_VOID:
		// no validation
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED &&
				status != enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT &&
				status != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		default:
			return serviceerror.NewInternalf("unknown workflow state: %v", state)
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		default:
			return serviceerror.NewInternalf("unknown workflow state: %v", state)
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status != e.GetStatus() {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			return invalidStateTransitionErr(e.GetState(), state, status)

		default:
			return serviceerror.NewInternalf("unknown workflow state: %v", state)
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		default:
			return serviceerror.NewInternalf("unknown workflow state: %v", state)
		}
	default:
		return serviceerror.NewInternalf("unknown workflow state: %v", state)
	}

	e.State = state
	e.Status = status
	return nil
}

func invalidStateTransitionErr(
	currentState enumsspb.WorkflowExecutionState,
	targetState enumsspb.WorkflowExecutionState,
	targetStatus enumspb.WorkflowExecutionStatus,
) error {
	return serviceerror.NewInternalf(
		invalidStateTransitionMsg,
		currentState,
		targetState,
		targetStatus,
	)
}
