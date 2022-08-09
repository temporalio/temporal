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

package executions

import (
	"context"
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/versionhistory"
)

const (
	mutableStateActivityIDFailureType      = "mutable_state_id_validator_activity"
	mutableStateTimerIDFailureType         = "mutable_state_id_validator_timer"
	mutableStateChildWorkflowIDFailureType = "mutable_state_id_validator_child_workflow"
	mutableStateRequestCancelIDFailureType = "mutable_state_id_validator_request_cancel"
	mutableStateSignalIDFailureType        = "mutable_state_id_validator_signal"
)

type (
	// mutableStateIDValidator is a validator that does shallow checks that
	// * ID >= common.FirstEventID
	// * ID <= last event ID
	mutableStateIDValidator struct{}
)

var _ Validator = (*mutableStateIDValidator)(nil)

// NewMutableStateIDValidator returns new instance.
func NewMutableStateIDValidator() *mutableStateIDValidator {
	return &mutableStateIDValidator{}
}

// Validate does shallow correctness check of IDs in mutable state.
func (v *mutableStateIDValidator) Validate(
	ctx context.Context,
	mutableState *MutableState,
) ([]MutableStateValidationResult, error) {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, err
	}

	var results []MutableStateValidationResult

	results = append(results, v.validateActivity(
		mutableState.ActivityInfos,
		lastItem.GetEventId())...,
	)

	results = append(results, v.validateTimer(
		mutableState.TimerInfos,
		lastItem.GetEventId())...,
	)

	results = append(results, v.validateChildWorkflow(
		mutableState.ChildExecutionInfos,
		lastItem.GetEventId())...,
	)

	results = append(results, v.validateRequestCancel(
		mutableState.RequestCancelInfos,
		lastItem.GetEventId())...,
	)

	results = append(results, v.validateSignal(
		mutableState.SignalInfos,
		lastItem.GetEventId())...,
	)

	return results, nil
}

func (v *mutableStateIDValidator) validateActivity(
	activityInfos map[int64]*persistencespb.ActivityInfo,
	lastEventID int64,
) []MutableStateValidationResult {
	var results []MutableStateValidationResult
	for activityEventID := range activityInfos {
		if v.validateID(activityEventID, lastEventID) {
			continue
		}
		results = append(results, MutableStateValidationResult{
			failureType: mutableStateActivityIDFailureType,
			failureDetails: fmt.Sprintf(
				"ActivityEventID: %d is not less than last event ID: %d",
				activityEventID,
				lastEventID,
			),
		})
	}
	return results
}

func (v *mutableStateIDValidator) validateTimer(
	timerInfos map[string]*persistencespb.TimerInfo,
	lastEventID int64,
) []MutableStateValidationResult {
	var results []MutableStateValidationResult
	for _, timer := range timerInfos {
		if v.validateID(timer.StartedEventId, lastEventID) {
			continue
		}
		results = append(results, MutableStateValidationResult{
			failureType: mutableStateTimerIDFailureType,
			failureDetails: fmt.Sprintf(
				"TimerEventID: %d is not less than last event ID: %d",
				timer.StartedEventId,
				lastEventID,
			),
		})
	}
	return results
}

func (v *mutableStateIDValidator) validateChildWorkflow(
	childExecutionInfos map[int64]*persistencespb.ChildExecutionInfo,
	lastEventID int64,
) []MutableStateValidationResult {
	var results []MutableStateValidationResult
	for childWorkflowEventID := range childExecutionInfos {
		if v.validateID(childWorkflowEventID, lastEventID) {
			continue
		}
		results = append(results, MutableStateValidationResult{
			failureType: mutableStateChildWorkflowIDFailureType,
			failureDetails: fmt.Sprintf(
				"ChildWorkflowEventID: %d is not less than last event ID: %d",
				childWorkflowEventID,
				lastEventID,
			),
		})
	}
	return results
}

func (v *mutableStateIDValidator) validateRequestCancel(
	requestCancelInfos map[int64]*persistencespb.RequestCancelInfo,
	lastEventID int64,
) []MutableStateValidationResult {
	var results []MutableStateValidationResult
	for requestCancelEventID := range requestCancelInfos {
		if v.validateID(requestCancelEventID, lastEventID) {
			continue
		}
		results = append(results, MutableStateValidationResult{
			failureType: mutableStateRequestCancelIDFailureType,
			failureDetails: fmt.Sprintf(
				"RequestCancelEventID: %d is not less than last event ID: %d",
				requestCancelEventID,
				lastEventID,
			),
		})
	}
	return results
}

func (v *mutableStateIDValidator) validateSignal(
	signalInfos map[int64]*persistencespb.SignalInfo,
	lastEventID int64,
) []MutableStateValidationResult {
	var results []MutableStateValidationResult
	for signalEventID := range signalInfos {
		if v.validateID(signalEventID, lastEventID) {
			continue
		}
		results = append(results, MutableStateValidationResult{
			failureType: mutableStateSignalIDFailureType,
			failureDetails: fmt.Sprintf(
				"SignalEventID: %d is not less than last event ID: %d",
				signalEventID,
				lastEventID,
			),
		})
	}
	return results
}

func (v *mutableStateIDValidator) validateID(
	eventID int64,
	lastEventID int64,
) bool {
	return common.FirstEventID <= eventID && eventID <= lastEventID
}
