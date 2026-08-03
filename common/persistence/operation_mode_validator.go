package persistence

import (
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

// NOTE: when modifying this file, plz make each case clear,
//  do not combine cases together.
// The idea for this file is to test whether current record
// points to a zombie record.

// ValidateCreateWorkflowModeState validate workflow creation mode & workflow state
func ValidateCreateWorkflowModeState(
	mode CreateWorkflowMode,
	newWorkflowSnapshot WorkflowSnapshot,
) error {

	workflowState := newWorkflowSnapshot.ExecutionState.State
	if err := checkWorkflowState(workflowState); err != nil {
		return err
	}

	switch mode {
	case CreateWorkflowModeBrandNew,
		CreateWorkflowModeUpdateCurrent:
		if workflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	case CreateWorkflowModeBypassCurrent:
		if workflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			workflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternalf("unknown mode: %v", mode)
	}
}

// ValidateUpdateWorkflowModeState validate workflow update mode & workflow state
func ValidateUpdateWorkflowModeState(
	mode UpdateWorkflowMode,
	currentWorkflowMutation WorkflowMutation,
	newWorkflowSnapshot *WorkflowSnapshot,
) error {

	currentWorkflowState := currentWorkflowMutation.ExecutionState.State
	if err := checkWorkflowState(currentWorkflowState); err != nil {
		return err
	}
	var newWorkflowState *enumsspb.WorkflowExecutionState
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionState.State
		if err := checkWorkflowState(*newWorkflowState); err != nil {
			return err
		}
	}

	switch mode {
	case UpdateWorkflowModeUpdateCurrent:
		// update current record
		// 1. current workflow only ->
		//  current workflow cannot be zombie
		// 2. current workflow & new workflow ->
		//  current workflow cannot be created / running,
		//  new workflow cannot be zombie

		// case 1
		if newWorkflowState == nil {
			if currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
			return newInvalidUpdateWorkflowWithNewMode(mode, currentWorkflowState, *newWorkflowState)
		}
		return nil

	case UpdateWorkflowModeBypassCurrent:
		// bypass current record
		// 1. current workflow only ->
		//  current workflow cannot be created / running
		// 2. current workflow & new workflow ->
		//  current workflow cannot be created / running,
		//  new workflow cannot be created / running

		// case 1
		if newWorkflowState == nil {
			if currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
				currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
			return newInvalidUpdateWorkflowWithNewMode(
				mode,
				currentWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	case UpdateWorkflowModeIgnoreCurrent:
		// Cannot have new workflow when skipping current workflow check
		if newWorkflowState != nil {
			return newInvalidUpdateWorkflowWithNewMode(
				mode,
				currentWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternalf("unknown mode: %v", mode)
	}
}

// ValidateConflictResolveWorkflowModeState validate workflow conflict resolve mode & workflow state
func ValidateConflictResolveWorkflowModeState(
	mode ConflictResolveWorkflowMode,
	resetWorkflowSnapshot WorkflowSnapshot,
	newWorkflowSnapshot *WorkflowSnapshot,
	currentWorkflowMutation *WorkflowMutation,
) error {

	resetWorkflowState := resetWorkflowSnapshot.ExecutionState.State
	if err := checkWorkflowState(resetWorkflowState); err != nil {
		return err
	}
	var newWorkflowState *enumsspb.WorkflowExecutionState
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionState.State
		if err := checkWorkflowState(*newWorkflowState); err != nil {
			return err
		}
	}
	var currentWorkflowState *enumsspb.WorkflowExecutionState
	if currentWorkflowMutation != nil {
		currentWorkflowState = &currentWorkflowMutation.ExecutionState.State
		if err := checkWorkflowState(*currentWorkflowState); err != nil {
			return err
		}
	}

	switch mode {
	case ConflictResolveWorkflowModeUpdateCurrent:
		// update current record
		// 1. reset workflow only ->
		//  reset workflow cannot be zombie
		// 2. reset workflow & new workflow ->
		//  reset workflow cannot be created / running / zombie,
		//  new workflow cannot be zombie
		// 3. current workflow & reset workflow ->
		//  current workflow cannot be created / running,
		//  reset workflow cannot be zombie
		// 4. current workflow & reset workflow & new workflow ->
		//  current workflow cannot be created / running,
		//  reset workflow cannot be created / running / zombie,
		//  new workflow cannot be zombie

		// TODO remove case 1 & 2 support once 2DC is deprecated
		// it is ok that currentWorkflowMutation is null, only for 2 DC case
		// NDC should always require current workflow for CAS
		// Note: current workflow mutation can be in zombie state, for the update

		// case 1 & 2
		if currentWorkflowState == nil {
			// case 1
			if newWorkflowState == nil {
				if resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
					return newInvalidConflictResolveWorkflowMode(
						mode,
						resetWorkflowState,
					)
				}
				return nil
			}

			// case 2
			if resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
				resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
				resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE ||
				*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
				return newInvalidConflictResolveWorkflowWithNewMode(
					mode,
					resetWorkflowState,
					*newWorkflowState,
				)
			}
			return nil
		}

		// case 3 & 4
		// case 3
		if newWorkflowState == nil {
			if *currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
				*currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
				resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
				return newInvalidConflictResolveWorkflowWithCurrentMode(
					mode,
					resetWorkflowState,
					*currentWorkflowState,
				)
			}
			return nil
		}

		// case 4
		if *currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			*currentWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
			return newInvalidConflictResolveWorkflowWithCurrentWithNewMode(
				mode,
				resetWorkflowState,
				*newWorkflowState,
				*currentWorkflowState,
			)
		}
		return nil

	case ConflictResolveWorkflowModeBypassCurrent:
		// bypass current record
		// * current workflow cannot be set
		// 1. reset workflow only ->
		//  reset workflow cannot be created / running
		// 2. reset workflow & new workflow ->
		//  reset workflow cannot be created / running / zombie,
		//  new workflow cannot be created / running / completed

		// precondition
		if currentWorkflowMutation != nil {
			return serviceerror.NewInternalf("Invalid workflow conflict resolve mode %v, encountered current workflow", mode)
		}

		// case 1
		if newWorkflowState == nil {
			if resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
				resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
				return newInvalidConflictResolveWorkflowMode(
					mode,
					resetWorkflowState,
				)
			}
			return nil
		}

		// case 2
		if resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			resetWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING ||
			*newWorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return newInvalidConflictResolveWorkflowWithNewMode(
				mode,
				resetWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternalf("unknown mode: %v", mode)
	}
}

func checkWorkflowState(state enumsspb.WorkflowExecutionState) error {
	switch state {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED:
		return nil
	default:
		return serviceerror.NewInternalf("unknown workflow state: %v", state)
	}
}

func newInvalidCreateWorkflowMode(
	mode CreateWorkflowMode,
	workflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow create mode %v, state: %v",
		mode,
		workflowState,
	)
}

func newInvalidUpdateWorkflowMode(
	mode UpdateWorkflowMode,
	currentWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow update mode %v, state: %v",
		mode,
		currentWorkflowState,
	)
}

func newInvalidUpdateWorkflowWithNewMode(
	mode UpdateWorkflowMode,
	currentWorkflowState enumsspb.WorkflowExecutionState,
	newWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow update mode %v, current state: %v, new state: %v",
		mode,
		currentWorkflowState,
		newWorkflowState,
	)
}

func newInvalidConflictResolveWorkflowMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow conflict resolve mode %v, reset state: %v",
		mode,
		resetWorkflowState,
	)
}

func newInvalidConflictResolveWorkflowWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState enumsspb.WorkflowExecutionState,
	newWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v",
		mode,
		resetWorkflowState,
		newWorkflowState,
	)
}

func newInvalidConflictResolveWorkflowWithCurrentMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState enumsspb.WorkflowExecutionState,
	currentWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, current state: %v",
		mode,
		resetWorkflowState,
		currentWorkflowState,
	)
}

func newInvalidConflictResolveWorkflowWithCurrentWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState enumsspb.WorkflowExecutionState,
	newWorkflowState enumsspb.WorkflowExecutionState,
	currentWorkflowState enumsspb.WorkflowExecutionState,
) error {
	return serviceerror.NewInternalf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v, current state: %v",
		mode,
		resetWorkflowState,
		newWorkflowState,
		currentWorkflowState,
	)
}
