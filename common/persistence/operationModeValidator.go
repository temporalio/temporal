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

// ValidateCreateWorkflowModeState validate workflow creation mode & workflow state
func ValidateCreateWorkflowModeState(
	mode CreateWorkflowMode,
	newWorkflowSnapshot InternalWorkflowSnapshot,
) error {

	workflowState := newWorkflowSnapshot.ExecutionInfo.State

	switch mode {
	case CreateWorkflowModeBrandNew, CreateWorkflowModeWorkflowIDReuse, CreateWorkflowModeContinueAsNew:
		if workflowState == WorkflowStateZombie || workflowState == WorkflowStateCompleted {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow create mode %v, state: %v",
					mode,
					workflowState,
				),
			}
		}

	case CreateWorkflowModeZombie:
		// noop
		if workflowState != WorkflowStateZombie {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow create mode %v, state: %v",
					mode,
					workflowState,
				),
			}
		}

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
	}
	return nil
}

// ValidateUpdateWorkflowModeState validate workflow update mode & workflow state
func ValidateUpdateWorkflowModeState(
	mode UpdateWorkflowMode,
	currentWorkflowMutation InternalWorkflowMutation,
	newWorkflowSnapshot *InternalWorkflowSnapshot,
) error {

	currentWorkflowState := currentWorkflowMutation.ExecutionInfo.State
	var newWorkflowState *int
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionInfo.State
	}

	switch mode {
	case UpdateWorkflowModeUpdateCurrent:
		if (currentWorkflowState == WorkflowStateZombie && newWorkflowState == nil) ||
			(currentWorkflowState == WorkflowStateZombie && newWorkflowState != nil && *newWorkflowState == WorkflowStateZombie) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow update mode %v, state: %v",
					mode,
					currentWorkflowState,
				),
			}
		}

	case UpdateWorkflowModeBypassCurrent:
		if currentWorkflowState == WorkflowStateCreated || currentWorkflowState == WorkflowStateRunning ||
			(newWorkflowState != nil &&
				(*newWorkflowState == WorkflowStateCreated || *newWorkflowState == WorkflowStateRunning)) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow update mode %v, current state: %v, new state: %v",
					mode,
					currentWorkflowState,
					newWorkflowState,
				),
			}
		}

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
	}
	return nil
}

// ValidateConflictResolveWorkflowModeState validate workflow conflict resolve mode & workflow state
func ValidateConflictResolveWorkflowModeState(
	mode ConflictResolveWorkflowMode,
	resetWorkflowSnapshot InternalWorkflowSnapshot,
	newWorkflowSnapshot *InternalWorkflowSnapshot,
	currentWorkflowMutation *InternalWorkflowMutation,
) error {

	resetWorkflowState := resetWorkflowSnapshot.ExecutionInfo.State
	var newWorkflowState *int
	if newWorkflowSnapshot != nil {
		newWorkflowState = &newWorkflowSnapshot.ExecutionInfo.State
	}
	var currentWorkflowState *int
	if currentWorkflowMutation != nil {
		currentWorkflowState = &currentWorkflowMutation.ExecutionInfo.State
	}

	switch mode {
	case ConflictResolveWorkflowModeUpdateCurrent:
		// it is ok that currentWorkflowMutation is null, for 2 DC
		// Note: current workflow mutation can be in zombie state, for the update
		if resetWorkflowState == WorkflowStateZombie ||
			(newWorkflowState != nil && *newWorkflowState == WorkflowStateZombie) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow conflict resolve mode %v, state: %v",
					mode,
					currentWorkflowState,
				),
			}
		}

	case ConflictResolveWorkflowModeBypassCurrent:
		if resetWorkflowState == WorkflowStateCreated || resetWorkflowState == WorkflowStateRunning ||
			(newWorkflowState != nil &&
				(*newWorkflowState == WorkflowStateCreated || *newWorkflowState == WorkflowStateRunning)) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow conflict resolve mode %v, reset state: %v, new state: %v",
					mode,
					resetWorkflowState,
					newWorkflowState,
				),
			}
		}

		if currentWorkflowMutation != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"Invalid workflow conflict resolve mode %v, encounter current workflow",
					mode,
				),
			}
		}

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", mode),
		}
	}
	return nil
}
