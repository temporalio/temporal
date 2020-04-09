package persistence

import (
	"fmt"

	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"
)

var (
	validWorkflowStates = map[int]struct{}{
		WorkflowStateCreated:   {},
		WorkflowStateRunning:   {},
		WorkflowStateCompleted: {},
		WorkflowStateZombie:    {},
		WorkflowStateCorrupted: {},
	}

	validWorkflowStatuses = map[executionpb.WorkflowExecutionStatus]struct{}{
		executionpb.WorkflowExecutionStatus_Running:        {},
		executionpb.WorkflowExecutionStatus_Completed:      {},
		executionpb.WorkflowExecutionStatus_Failed:         {},
		executionpb.WorkflowExecutionStatus_Canceled:       {},
		executionpb.WorkflowExecutionStatus_Terminated:     {},
		executionpb.WorkflowExecutionStatus_ContinuedAsNew: {},
		executionpb.WorkflowExecutionStatus_TimedOut:       {},
	}
)

// ValidateCreateWorkflowStateStatus validate workflow state and close status
func ValidateCreateWorkflowStateStatus(
	state int,
	status executionpb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if state == WorkflowStateCompleted || status != executionpb.WorkflowExecutionStatus_Running {
		return serviceerror.NewInternal(fmt.Sprintf("Create workflow with invalid state: %v or close status: %v", state, status))
	}
	return nil
}

// ValidateUpdateWorkflowStateStatus validate workflow state and close status
func ValidateUpdateWorkflowStateStatus(
	state int,
	status executionpb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if status == executionpb.WorkflowExecutionStatus_Running {
		if state == WorkflowStateCompleted {
			return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or close status: %v", state, status))
		}
	} else {
		// executionpb.WorkflowExecutionStatus_Completed
		// executionpb.WorkflowExecutionStatus_Failed
		// executionpb.WorkflowExecutionStatus_Canceled
		// executionpb.WorkflowExecutionStatus_Terminated
		// executionpb.WorkflowExecutionStatus_ContinuedAsNew
		// executionpb.WorkflowExecutionStatus_TimedOut
		if state != WorkflowStateCompleted {
			return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or close status: %v", state, status))
		}
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state int,
) error {

	if _, ok := validWorkflowStates[state]; !ok {
		return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow state: %v", state))
	}

	return nil
}

// validateWorkflowStatus validate workflow close status
func validateWorkflowStatus(
	status executionpb.WorkflowExecutionStatus,
) error {

	if _, ok := validWorkflowStatuses[status]; !ok {
		return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow close status: %v", status))
	}

	return nil
}
