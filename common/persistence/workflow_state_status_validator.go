package persistence

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

var (
	validWorkflowStates = map[enumsspb.WorkflowExecutionState]struct{}{
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:   {},
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:   {},
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED: {},
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:    {},
		enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED: {},
	}

	validWorkflowStatuses = map[enumspb.WorkflowExecutionStatus]struct{}{
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:          {},
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:        {},
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:           {},
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:         {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:       {},
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:        {},
		enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED:           {},
	}
)

// ValidateCreateWorkflowStateStatus validate workflow state and close status
func ValidateCreateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & status
	if state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
			return serviceerror.NewInternalf("Create workflow with invalid state: %v or status: %v", state, status)
		}
	} else {
		if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return serviceerror.NewInternalf("Create workflow with invalid state: %v or status: %v", state, status)
		}
	}
	return nil
}

// ValidateUpdateWorkflowStateStatus validate workflow state and status
func ValidateUpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & status
	switch state {
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
			return serviceerror.NewInternalf("Update workflow with invalid state: %v or status: %v", state, status)
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return serviceerror.NewInternalf("Update workflow with invalid state: %v or status: %v", state, status)
		}
	default:
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
			return serviceerror.NewInternalf("Update workflow with invalid state: %v or status: %v", state, status)
		}
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state enumsspb.WorkflowExecutionState,
) error {

	if _, ok := validWorkflowStates[state]; !ok {
		return serviceerror.NewInternalf("Invalid workflow state: %v", state)
	}

	return nil
}

// validateWorkflowStatus validate workflow status
func validateWorkflowStatus(
	status enumspb.WorkflowExecutionStatus,
) error {

	if _, ok := validWorkflowStatuses[status]; !ok {
		return serviceerror.NewInternalf("Invalid workflow status: %v", status)
	}

	return nil
}
