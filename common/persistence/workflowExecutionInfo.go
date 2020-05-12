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
	state executiongenpb.WorkflowExecutionState,
	status executionpb.WorkflowExecutionStatus,
) error {

	switch e.State {
	case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Void:
		// no validation
	case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created:
		switch state {
		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Running:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed:
			if status != executionpb.WorkflowExecutionStatus_Terminated &&
				status != executionpb.WorkflowExecutionStatus_TimedOut &&
				status != executionpb.WorkflowExecutionStatus_ContinuedAsNew {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Zombie:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Running:
		switch state {
		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Running:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed:
			if status == executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Zombie:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed:
		switch state {
		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Running:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed:
			if status != e.Status {
				return e.createInvalidStateTransitionErr(e.State, state, status)

			}
		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Zombie:
			return e.createInvalidStateTransitionErr(e.State, state, status)

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Zombie:
		switch state {
		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Running:
			if status != executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed:
			if status == executionpb.WorkflowExecutionStatus_Running {
				return e.createInvalidStateTransitionErr(e.State, state, status)
			}

		case executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Zombie:
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
	currentState executiongenpb.WorkflowExecutionState,
	targetState executiongenpb.WorkflowExecutionState,
	targetStatus executionpb.WorkflowExecutionStatus,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(invalidStateTransitionMsg, currentState, targetState, targetStatus))
}
