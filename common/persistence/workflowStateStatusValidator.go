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

	executiongenpb "github.com/temporalio/temporal/.gen/proto/execution"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"
)

var (
	validWorkflowStates = map[executiongenpb.WorkflowExecutionState]struct{}{
		executiongenpb.WORKFLOW_EXECUTION_STATE_CREATED:   {},
		executiongenpb.WORKFLOW_EXECUTION_STATE_RUNNING:   {},
		executiongenpb.WORKFLOW_EXECUTION_STATE_COMPLETED: {},
		executiongenpb.WORKFLOW_EXECUTION_STATE_ZOMBIE:    {},
		executiongenpb.WORKFLOW_EXECUTION_STATE_CORRUPTED: {},
	}

	validWorkflowStatuses = map[executionpb.WorkflowExecutionStatus]struct{}{
		executionpb.WORKFLOW_EXECUTION_STATUS_RUNNING:        {},
		executionpb.WORKFLOW_EXECUTION_STATUS_COMPLETED:      {},
		executionpb.WORKFLOW_EXECUTION_STATUS_FAILED:         {},
		executionpb.WORKFLOW_EXECUTION_STATUS_CANCELED:       {},
		executionpb.WORKFLOW_EXECUTION_STATUS_TERMINATED:     {},
		executionpb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: {},
		executionpb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:       {},
	}
)

// ValidateCreateWorkflowStateStatus validate workflow state and close status
func ValidateCreateWorkflowStateStatus(
	state executiongenpb.WorkflowExecutionState,
	status executionpb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if state == executiongenpb.WORKFLOW_EXECUTION_STATE_COMPLETED || status != executionpb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return serviceerror.NewInternal(fmt.Sprintf("Create workflow with invalid state: %v or close status: %v", state, status))
	}
	return nil
}

// ValidateUpdateWorkflowStateStatus validate workflow state and close status
func ValidateUpdateWorkflowStateStatus(
	state executiongenpb.WorkflowExecutionState,
	status executionpb.WorkflowExecutionStatus,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowStatus(status); err != nil {
		return err
	}

	// validate workflow state & close status
	if status == executionpb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		if state == executiongenpb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or close status: %v", state, status))
		}
	} else {
		// executionpb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		// executionpb.WORKFLOW_EXECUTION_STATUS_FAILED
		// executionpb.WORKFLOW_EXECUTION_STATUS_CANCELED
		// executionpb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		// executionpb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
		// executionpb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
		if state != executiongenpb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return serviceerror.NewInternal(fmt.Sprintf("Update workflow with invalid state: %v or close status: %v", state, status))
		}
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state executiongenpb.WorkflowExecutionState,
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
