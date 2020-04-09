package persistence

import (
	"fmt"

	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"
)

// SetNextEventID sets the nextEventID
func (e *WorkflowExecutionInfo) SetNextEventID(id int64) {
	e.NextEventID = id
}

// IncreaseNextEventID increase the nextEventID by 1
func (e *WorkflowExecutionInfo) IncreaseNextEventID() {
	e.NextEventID++
}

// SetLastFirstEventID set the LastFirstEventID
func (e *WorkflowExecutionInfo) SetLastFirstEventID(id int64) {
	e.LastFirstEventID = id
}

// UpdateWorkflowStateStatus update the workflow state
func (e *WorkflowExecutionInfo) UpdateWorkflowStateStatus(
	state int,
	status executionpb.WorkflowExecutionStatus,
) error {

	switch e.State {
	case WorkflowStateVoid:
		// no validation
	case WorkflowStateCreated:
		switch state {
		case WorkflowStateCreated:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateRunning:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateCompleted:
			if status != executionpb.WorkflowExecutionStatus_Terminated &&
				status != executionpb.WorkflowExecutionStatus_TimedOut &&
				status != executionpb.WorkflowExecutionStatus_ContinuedAsNew {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateZombie:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case WorkflowStateRunning:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case WorkflowStateRunning:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateCompleted:
			if status == executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateZombie:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case WorkflowStateCompleted:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case WorkflowStateRunning:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case WorkflowStateCompleted:
			if status != e.Status {
				return e.createInvalidStateTransitionErr(e.State, state, status)

			}
		case WorkflowStateZombie:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case WorkflowStateZombie:
		switch state {
		case WorkflowStateCreated:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateRunning:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateCompleted:
			if status == executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case WorkflowStateZombie:
			if status == executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
	}

	e.State = state
	e.Status = status
	return nil

}

// UpdateWorkflowStateStatus update the workflow state
func (e *WorkflowExecutionInfo) createInvalidStateTransitionErr(
	currentState int,
	targetState int,
	targetStatus executionpb.WorkflowExecutionStatus,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(invalidStateTransitionMsg, currentState, targetState, targetStatus))
}
