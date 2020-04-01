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

	"go.temporal.io/temporal-proto/enums"
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

	validWorkflowStatuses = map[enums.WorkflowExecutionStatus]struct{}{
		enums.WorkflowExecutionStatusRunning:        {},
		enums.WorkflowExecutionStatusCompleted:      {},
		enums.WorkflowExecutionStatusFailed:         {},
		enums.WorkflowExecutionStatusCanceled:       {},
		enums.WorkflowExecutionStatusTerminated:     {},
		enums.WorkflowExecutionStatusContinuedAsNew: {},
		enums.WorkflowExecutionStatusTimedOut:       {},
	}
)

// ValidateCreateWorkflowStateStatus validate workflow state and close status
func ValidateCreateWorkflowStateStatus(
	state int,
	status enums.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if state == WorkflowStateCompleted || status != enums.WorkflowExecutionStatusRunning {
		return serviceerror.NewInternal(fmt.Sprintf("Create workflow with invalid state: %v or close status: %v", state, status))
	}
	return nil
}

// ValidateUpdateWorkflowStateStatus validate workflow state and close status
func ValidateUpdateWorkflowStateStatus(
	state int,
	status enums.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if status == enums.WorkflowExecutionStatusRunning {
		if state == WorkflowStateCompleted {
			return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or close status: %v", state, status))
		}
	} else {
		// enums.WorkflowExecutionStatusCompleted
		// enums.WorkflowExecutionStatusFailed
		// enums.WorkflowExecutionStatusCanceled
		// enums.WorkflowExecutionStatusTerminated
		// enums.WorkflowExecutionStatusContinuedAsNew
		// enums.WorkflowExecutionStatusTimedOut
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
	status enums.WorkflowExecutionStatus,
) error {

	if _, ok := validWorkflowStatuses[status]; !ok {
		return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow close status: %v", status))
	}

	return nil
}
