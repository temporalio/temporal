// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"fmt"

	executiongenproto "github.com/temporalio/temporal/.gen/proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"
)

// NOTE: when modifying this file, plz make each case clear,
//  do not combine cases together.
// The idea for this file is to test whether current record
// points to a zombie record.

// ValidateCreateWorkflowModeState validate workflow creation mode & workflow state
func ValidateCreateWorkflowModeState(
	mode CreateWorkflowMode,
	newWorkflowSnapshot InternalWorkflowSnapshot,
) error {

	workflowState := newWorkflowSnapshot.ExecutionInfo.State
	if err := checkWorkflowState(workflowState); err != nil {
		return err
	}

	switch mode {
	case CreateWorkflowModeBrandNew,
		CreateWorkflowModeWorkflowIDReuse,
		CreateWorkflowModeContinueAsNew:
		if workflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
			workflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	case CreateWorkflowModeZombie:
		if workflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			workflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			workflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown mode: %v", mode))
	}
}

// ValidateUpdateWorkflowModeState validate workflow update mode & workflow state
func ValidateUpdateWorkflowModeState(
	mode UpdateWorkflowMode,
	currentWorkflowMutation InternalWorkflowMutation,
	newWorkflowSnapshot *InternalWorkflowSnapshot,
) error {

	currentWorkflowState := currentWorkflowMutation.ExecutionInfo.State
	if err := checkWorkflowState(currentWorkflowState); err != nil {
		return err
	}
	var newWorkflowState *executiongenproto.WorkflowExecutionState
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionInfo.State
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
			if currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
			return newInvalidUpdateWorkflowWithNewMode(mode, currentWorkflowState, *newWorkflowState)
		}
		return nil

	case UpdateWorkflowModeBypassCurrent:
		// bypass current record
		// 1. current workflow only ->
		//  current workflow cannot be created / running
		// 2. current workflow & new workflow ->
		//  current workflow cannot be created / running,
		//  new workflow cannot be created / running / completed

		// case 1
		if newWorkflowState == nil {
			if currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
				currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
			return newInvalidUpdateWorkflowWithNewMode(
				mode,
				currentWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown mode: %v", mode))
	}
}

// ValidateConflictResolveWorkflowModeState validate workflow conflict resolve mode & workflow state
func ValidateConflictResolveWorkflowModeState(
	mode ConflictResolveWorkflowMode,
	resetWorkflowSnapshot InternalWorkflowSnapshot,
	newWorkflowSnapshot *InternalWorkflowSnapshot,
	currentWorkflowMutation *InternalWorkflowMutation,
) error {

	resetWorkflowState := resetWorkflowSnapshot.ExecutionInfo.State
	if err := checkWorkflowState(resetWorkflowState); err != nil {
		return err
	}
	var newWorkflowState *executiongenproto.WorkflowExecutionState
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionInfo.State
		if err := checkWorkflowState(*newWorkflowState); err != nil {
			return err
		}
	}
	var currentWorkflowState *executiongenproto.WorkflowExecutionState
	if currentWorkflowMutation != nil {
		currentWorkflowState = &currentWorkflowMutation.ExecutionInfo.State
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
		//  new workflow cannot be zombie / completed
		// 3. current workflow & reset workflow ->
		//  current workflow cannot be created / running,
		//  reset workflow cannot be zombie
		// 4. current workflow & reset workflow & new workflow ->
		//  current workflow cannot be created / running,
		//  reset workflow cannot be created / running / zombie,
		//  new workflow cannot be zombie / completed

		// TODO remove case 1 & 2 support once 2DC is deprecated
		// it is ok that currentWorkflowMutation is null, only for 2 DC case
		// NDC should always require current workflow for CAS
		// Note: current workflow mutation can be in zombie state, for the update

		// case 1 & 2
		if currentWorkflowState == nil {
			// case 1
			if newWorkflowState == nil {
				if resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie {
					return newInvalidConflictResolveWorkflowMode(
						mode,
						resetWorkflowState,
					)
				}
				return nil
			}

			// case 2
			if resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
				resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
				resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
				*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
				*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
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
			if *currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
				*currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
				resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie {
				return newInvalidConflictResolveWorkflowWithCurrentMode(
					mode,
					resetWorkflowState,
					*currentWorkflowState,
				)
			}
			return nil
		}

		// case 4
		if *currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			*currentWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
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
			return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow conflict resolve mode %v, encounter current workflow", mode))
		}

		// case 1
		if newWorkflowState == nil {
			if resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
				resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running {
				return newInvalidConflictResolveWorkflowMode(
					mode,
					resetWorkflowState,
				)
			}
			return nil
		}

		// case 2
		if resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			resetWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running ||
			*newWorkflowState == executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed {
			return newInvalidConflictResolveWorkflowWithNewMode(
				mode,
				resetWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown mode: %v", mode))
	}
}

func checkWorkflowState(state executiongenproto.WorkflowExecutionState) error {
	switch state {
	case executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Created,
		executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Running,
		executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Zombie,
		executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Completed,
		executiongenproto.WorkflowExecutionState_WorkflowExecutionState_Corrupted:
		return nil
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
	}
}

func newInvalidCreateWorkflowMode(
	mode CreateWorkflowMode,
	workflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow create mode %v, state: %v",
		mode,
		workflowState,
	),
	)
}

func newInvalidUpdateWorkflowMode(
	mode UpdateWorkflowMode,
	currentWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow update mode %v, state: %v",
		mode,
		currentWorkflowState,
	),
	)
}

func newInvalidUpdateWorkflowWithNewMode(
	mode UpdateWorkflowMode,
	currentWorkflowState executiongenproto.WorkflowExecutionState,
	newWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow update mode %v, current state: %v, new state: %v",
		mode,
		currentWorkflowState,
		newWorkflowState,
	),
	)
}

func newInvalidConflictResolveWorkflowMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow conflict resolve mode %v, reset state: %v",
		mode,
		resetWorkflowState,
	),
	)
}

func newInvalidConflictResolveWorkflowWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState executiongenproto.WorkflowExecutionState,
	newWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v",
		mode,
		resetWorkflowState,
		newWorkflowState,
	),
	)
}

func newInvalidConflictResolveWorkflowWithCurrentMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState executiongenproto.WorkflowExecutionState,
	currentWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, current state: %v",
		mode,
		resetWorkflowState,
		currentWorkflowState,
	),
	)
}

func newInvalidConflictResolveWorkflowWithCurrentWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState executiongenproto.WorkflowExecutionState,
	newWorkflowState executiongenproto.WorkflowExecutionState,
	currentWorkflowState executiongenproto.WorkflowExecutionState,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v, current state: %v",
		mode,
		resetWorkflowState,
		newWorkflowState,
		currentWorkflowState,
	),
	)
}
