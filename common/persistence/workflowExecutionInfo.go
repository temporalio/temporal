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

// UpdateWorkflowStateCloseStatus update the workflow state
func (e *WorkflowExecutionInfo) UpdateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	switch e.State {
	case WorkflowStateVoid:
		// no validation
	case WorkflowStateCreated:
		switch state {
		case WorkflowStateCreated:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus != WorkflowCloseStatusTerminated &&
				closeStatus != WorkflowCloseStatusTimedOut &&
				closeStatus != WorkflowCloseStatusContinuedAsNew {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateRunning:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateCompleted:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateRunning:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateCompleted:
			if closeStatus != e.CloseStatus {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

			}
		case WorkflowStateZombie:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateZombie:
		switch state {
		case WorkflowStateCreated:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown workflow state: %v", state),
		}
	}

	e.State = state
	e.CloseStatus = closeStatus
	return nil

}

// UpdateWorkflowStateCloseStatus update the workflow state
func (e *WorkflowExecutionInfo) createInvalidStateTransitionErr(
	currentState int,
	targetState int,
	targetCloseStatus int,
) error {
	return &workflow.InternalServiceError{
		Message: fmt.Sprintf(invalidStateTransitionMsg, currentState, targetState, targetCloseStatus),
	}
}
