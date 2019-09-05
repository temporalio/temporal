// Copyright (c) 2017 Uber Technologies, Inc.
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

var (
	validWorkflowStates = map[int]struct{}{
		WorkflowStateCreated:   {},
		WorkflowStateRunning:   {},
		WorkflowStateCompleted: {},
		WorkflowStateZombie:    {},
	}

	validWorkflowCloseStatuses = map[int]struct{}{
		WorkflowCloseStatusNone:           {},
		WorkflowCloseStatusCompleted:      {},
		WorkflowCloseStatusFailed:         {},
		WorkflowCloseStatusCanceled:       {},
		WorkflowCloseStatusTerminated:     {},
		WorkflowCloseStatusContinuedAsNew: {},
		WorkflowCloseStatusTimedOut:       {},
	}
)

// ValidateCreateWorkflowStateCloseStatus validate workflow state and close status
func ValidateCreateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowCloseStatus(closeStatus); err != nil {
		return err
	}

	// validate workflow state & close status
	if state == WorkflowStateCompleted || closeStatus != WorkflowCloseStatusNone {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Create workflow with invalid state: %v or close status: %v",
				state, closeStatus),
		}
	}
	return nil
}

// ValidateUpdateWorkflowStateCloseStatus validate workflow state and close status
func ValidateUpdateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowCloseStatus(closeStatus); err != nil {
		return err
	}

	// validate workflow state & close status
	if closeStatus == WorkflowCloseStatusNone {
		if state == WorkflowStateCompleted {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Update workflow with invalid state: %v or close status: %v",
					state, closeStatus),
			}
		}
	} else {
		// WorkflowCloseStatusCompleted
		// WorkflowCloseStatusFailed
		// WorkflowCloseStatusCanceled
		// WorkflowCloseStatusTerminated
		// WorkflowCloseStatusContinuedAsNew
		// WorkflowCloseStatusTimedOut
		if state != WorkflowStateCompleted {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Update workflow with invalid state: %v or close status: %v",
					state, closeStatus),
			}
		}
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state int,
) error {

	if _, ok := validWorkflowStates[state]; !ok {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Invalid workflow state: %v", state),
		}
	}

	return nil
}

// validateWorkflowCloseStatus validate workflow close status
func validateWorkflowCloseStatus(
	closeStatus int,
) error {

	if _, ok := validWorkflowCloseStatuses[closeStatus]; !ok {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Invalid workflow close status: %v", closeStatus),
		}
	}

	return nil
}

// ToThriftWorkflowExecutionCloseStatus convert persistence representation of close status to thrift representation
func ToThriftWorkflowExecutionCloseStatus(
	closeStatus int,
) workflow.WorkflowExecutionCloseStatus {

	switch closeStatus {
	case WorkflowCloseStatusCompleted:
		return workflow.WorkflowExecutionCloseStatusCompleted
	case WorkflowCloseStatusFailed:
		return workflow.WorkflowExecutionCloseStatusFailed
	case WorkflowCloseStatusCanceled:
		return workflow.WorkflowExecutionCloseStatusCanceled
	case WorkflowCloseStatusTerminated:
		return workflow.WorkflowExecutionCloseStatusTerminated
	case WorkflowCloseStatusContinuedAsNew:
		return workflow.WorkflowExecutionCloseStatusContinuedAsNew
	case WorkflowCloseStatusTimedOut:
		return workflow.WorkflowExecutionCloseStatusTimedOut
	default:
		panic("Invalid value for enum WorkflowExecutionCloseStatus")
	}
}
