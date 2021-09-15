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

package workflow

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	// indicate invalid workflow state transition
	invalidStateTransitionMsg = "unable to change workflow state from %v to %v, status %v"
)

// setStateStatus sets state and status in WorkflowExecutionState.
func setStateStatus(
	e *persistencespb.WorkflowExecutionState,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {
	switch e.GetState() {
	case enumsspb.WORKFLOW_EXECUTION_STATE_VOID:
		// no validation
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED &&
				status != enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT &&
				status != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			return invalidStateTransitionErr(e.GetState(), state, status)

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status != e.GetStatus() {
				return invalidStateTransitionErr(e.GetState(), state, status)

			}
		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			return invalidStateTransitionErr(e.GetState(), state, status)

		default:
			return serviceerror.NewInternal(fmt.Sprintf("unknown workflow state: %v", state))
		}
	case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		switch state {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
			}

		case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return invalidStateTransitionErr(e.GetState(), state, status)
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

func invalidStateTransitionErr(
	currentState enumsspb.WorkflowExecutionState,
	targetState enumsspb.WorkflowExecutionState,
	targetStatus enumspb.WorkflowExecutionStatus,
) error {
	return serviceerror.NewInternal(fmt.Sprintf(
		invalidStateTransitionMsg,
		currentState,
		targetState,
		targetStatus,
	))
}
