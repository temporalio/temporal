// Copyright (c) 2019 Uber Technologies, Inc.
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

	workflow "github.com/uber/cadence/.gen/go/shared"
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
		if workflowState == WorkflowStateZombie ||
			workflowState == WorkflowStateCompleted {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	case CreateWorkflowModeZombie:
		if workflowState == WorkflowStateCreated ||
			workflowState == WorkflowStateRunning ||
			workflowState == WorkflowStateCompleted {
			return newInvalidCreateWorkflowMode(
				mode,
				workflowState,
			)
		}
		return nil

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
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
	var newWorkflowState *int
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
			if currentWorkflowState == WorkflowStateZombie {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == WorkflowStateCreated ||
			currentWorkflowState == WorkflowStateRunning ||
			*newWorkflowState == WorkflowStateZombie ||
			*newWorkflowState == WorkflowStateCompleted {
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
			if currentWorkflowState == WorkflowStateCreated ||
				currentWorkflowState == WorkflowStateRunning {
				return newInvalidUpdateWorkflowMode(mode, currentWorkflowState)
			}
			return nil
		}

		// case 2
		if currentWorkflowState == WorkflowStateCreated ||
			currentWorkflowState == WorkflowStateRunning ||
			*newWorkflowState == WorkflowStateCreated ||
			*newWorkflowState == WorkflowStateRunning ||
			*newWorkflowState == WorkflowStateCompleted {
			return newInvalidUpdateWorkflowWithNewMode(
				mode,
				currentWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
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
	var newWorkflowState *int
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionInfo.State
		if err := checkWorkflowState(*newWorkflowState); err != nil {
			return err
		}
	}
	var currentWorkflowState *int
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
				if resetWorkflowState == WorkflowStateZombie {
					return newInvalidConflictResolveWorkflowMode(
						mode,
						resetWorkflowState,
					)
				}
				return nil
			}

			// case 2
			if resetWorkflowState == WorkflowStateCreated ||
				resetWorkflowState == WorkflowStateRunning ||
				resetWorkflowState == WorkflowStateZombie ||
				*newWorkflowState == WorkflowStateZombie ||
				*newWorkflowState == WorkflowStateCompleted {
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
			if *currentWorkflowState == WorkflowStateCreated ||
				*currentWorkflowState == WorkflowStateRunning ||
				resetWorkflowState == WorkflowStateZombie {
				return newInvalidConflictResolveWorkflowWithCurrentMode(
					mode,
					resetWorkflowState,
					*currentWorkflowState,
				)
			}
			return nil
		}

		// case 4
		if *currentWorkflowState == WorkflowStateCreated ||
			*currentWorkflowState == WorkflowStateRunning ||
			resetWorkflowState == WorkflowStateCreated ||
			resetWorkflowState == WorkflowStateRunning ||
			resetWorkflowState == WorkflowStateZombie ||
			*newWorkflowState == WorkflowStateZombie ||
			*newWorkflowState == WorkflowStateCompleted {
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
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow conflict resolve mode %v, encounter current workflow",
					mode,
				),
			}
		}

		// case 1
		if newWorkflowState == nil {
			if resetWorkflowState == WorkflowStateCreated ||
				resetWorkflowState == WorkflowStateRunning {
				return newInvalidConflictResolveWorkflowMode(
					mode,
					resetWorkflowState,
				)
			}
			return nil
		}

		// case 2
		if resetWorkflowState == WorkflowStateCreated ||
			resetWorkflowState == WorkflowStateRunning ||
			resetWorkflowState == WorkflowStateZombie ||
			*newWorkflowState == WorkflowStateCreated ||
			*newWorkflowState == WorkflowStateRunning ||
			*newWorkflowState == WorkflowStateCompleted {
			return newInvalidConflictResolveWorkflowWithNewMode(
				mode,
				resetWorkflowState,
				*newWorkflowState,
			)
		}
		return nil

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
	}
}

func checkWorkflowState(state int) error {
	switch state {
	case WorkflowStateCreated,
		WorkflowStateRunning,
		WorkflowStateZombie,
		WorkflowStateCompleted,
		WorkflowStateCorrupted:
		return nil
	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown workflow state: %v", state),
		}
	}
}

func newInvalidCreateWorkflowMode(
	mode CreateWorkflowMode,
	workflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow create mode %v, state: %v",
			mode,
			workflowState,
		),
	}
}

func newInvalidUpdateWorkflowMode(
	mode UpdateWorkflowMode,
	currentWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow update mode %v, state: %v",
			mode,
			currentWorkflowState,
		),
	}
}

func newInvalidUpdateWorkflowWithNewMode(
	mode UpdateWorkflowMode,
	currentWorkflowState int,
	newWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow update mode %v, current state: %v, new state: %v",
			mode,
			currentWorkflowState,
			newWorkflowState,
		),
	}
}

func newInvalidConflictResolveWorkflowMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow conflict resolve mode %v, reset state: %v",
			mode,
			resetWorkflowState,
		),
	}
}

func newInvalidConflictResolveWorkflowWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState int,
	newWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v",
			mode,
			resetWorkflowState,
			newWorkflowState,
		),
	}
}

func newInvalidConflictResolveWorkflowWithCurrentMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState int,
	currentWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow conflict resolve mode %v, reset state: %v, current state: %v",
			mode,
			resetWorkflowState,
			currentWorkflowState,
		),
	}
}

func newInvalidConflictResolveWorkflowWithCurrentWithNewMode(
	mode ConflictResolveWorkflowMode,
	resetWorkflowState int,
	newWorkflowState int,
	currentWorkflowState int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(
			"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v, current state: %v",
			mode,
			resetWorkflowState,
			newWorkflowState,
			currentWorkflowState,
		),
	}
}
