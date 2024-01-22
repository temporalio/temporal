// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package callbacks

import (
	"fmt"
	"net/url"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/statemachines"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adapter struct{}

func (adapter) GetState(data *persistencespb.CallbackInfo) enumspb.CallbackState {
	return data.PublicInfo.State
}

func (adapter) SetState(data *persistencespb.CallbackInfo, state enumspb.CallbackState) {
	data.PublicInfo.State = state
}

func (adapter) OnTransition(data *persistencespb.CallbackInfo, from, to enumspb.CallbackState, env statemachines.Environment) {
	// TODO: consider moving this into the "framework".
	data.Version = env.GetVersion()
	if from == enumspb.CALLBACK_STATE_SCHEDULED {
		// Reset all of previous attempt's information.
		data.PublicInfo.Attempt++
		data.PublicInfo.LastAttemptCompleteTime = timestamppb.New(env.GetCurrentTime())
		data.PublicInfo.LastAttemptFailure = nil
	}

}

// EventMarkedReady is triggered when a callback is triggered but is not yet ready to be scheduled, e.g. it is
// waiting for another callback to complete.
type EventMarkedReady struct{}

var TransitionMarkedReady = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventMarkedReady]{
	Adapter: adapter{},
	Src:     []enumspb.CallbackState{enumspb.CALLBACK_STATE_STANDBY},
	Dst:     enumspb.CALLBACK_STATE_READY,
}

// EventBlocked is triggered when a triggered callback cannot be scheduled due to a large backlog for the
// callback's namespace and destination.
type EventBlocked struct{}

var TransitionBlocked = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventBlocked]{
	Adapter: adapter{},
	Src: []enumspb.CallbackState{
		enumspb.CALLBACK_STATE_STANDBY,
		enumspb.CALLBACK_STATE_READY,
		enumspb.CALLBACK_STATE_BACKING_OFF,
	},
	Dst: enumspb.CALLBACK_STATE_BLOCKED,
}

// EventScheduled is triggered when the callback is meant to be scheduled, either immediately after triggering
// or after backing off from a previous attempt.
type EventScheduled struct{}

var TransitionScheduled = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventScheduled]{
	Adapter: adapter{},
	Src: []enumspb.CallbackState{
		enumspb.CALLBACK_STATE_BLOCKED,
		enumspb.CALLBACK_STATE_STANDBY,
		enumspb.CALLBACK_STATE_READY,
		enumspb.CALLBACK_STATE_BACKING_OFF,
	},
	Dst: enumspb.CALLBACK_STATE_SCHEDULED,
	After: func(data *persistencespb.CallbackInfo, _ EventScheduled, env statemachines.Environment) error {
		data.PublicInfo.NextAttemptScheduleTime = nil

		var destination string
		switch v := data.PublicInfo.Callback.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			u, err := url.Parse(data.PublicInfo.Callback.GetNexus().Url)
			if err != nil {
				return fmt.Errorf("failed to parse URL: %v", &data.PublicInfo)
			}
			destination = u.Host
		default:
			return fmt.Errorf("unsupported callback variant %v", v)
		}

		env.Schedule(&tasks.CallbackTask{
			CallbackID:         data.Id,
			Attempt:            data.PublicInfo.Attempt,
			DestinationAddress: destination,
		})
		return nil
	},
}

// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
type EventAttemptFailed error

var TransitionAttemptFailed = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventAttemptFailed]{
	Adapter: adapter{},
	Src:     []enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	Dst:     enumspb.CALLBACK_STATE_BACKING_OFF,
	After: func(data *persistencespb.CallbackInfo, err EventAttemptFailed, env statemachines.Environment) error {
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		// TODO: Make the retry policy intial interval configurable.
		nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(data.PublicInfo.Attempt))
		nextAttemptScheduleTime := env.GetCurrentTime().Add(nextDelay)
		data.PublicInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
			Message: err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		env.Schedule(&tasks.CallbackBackoffTask{
			CallbackID:          data.Id,
			Attempt:             data.PublicInfo.Attempt,
			VisibilityTimestamp: nextAttemptScheduleTime,
		})
		return nil
	},
}

// EventFailed is triggered when an attempt is failed with a non retryable error.
type EventFailed error

var TransitionFailed = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventFailed]{
	Adapter: adapter{},
	Src:     []enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	Dst:     enumspb.CALLBACK_STATE_FAILED,
	After: func(data *persistencespb.CallbackInfo, err EventFailed, env statemachines.Environment) error {
		data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
			Message: err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return nil
	},
}

// EventSucceeded is triggered when an attempt succeeds.
type EventSucceeded struct{}

var TransitionSucceeded = statemachines.Transition[*persistencespb.CallbackInfo, enumspb.CallbackState, EventSucceeded]{
	Adapter: adapter{},
	Src:     []enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	Dst:     enumspb.CALLBACK_STATE_SUCCEEDED,
}
