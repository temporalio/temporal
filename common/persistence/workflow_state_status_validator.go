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
	if (state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) ||
		(state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		return serviceerror.NewInternal(fmt.Sprintf("Create workflow with invalid state: %v or status: %v", state, status))
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
	if (state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) ||
		(state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or status: %v", state, status))
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state enumsspb.WorkflowExecutionState,
) error {

	if _, ok := validWorkflowStates[state]; !ok {
		return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow state: %v", state))
	}

	return nil
}

// validateWorkflowStatus validate workflow status
func validateWorkflowStatus(
	status enumspb.WorkflowExecutionStatus,
) error {

	if _, ok := validWorkflowStatuses[status]; !ok {
		return serviceerror.NewInternal(fmt.Sprintf("Invalid workflow status: %v", status))
	}

	return nil
}
