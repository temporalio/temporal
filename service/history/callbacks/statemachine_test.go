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

package callbacks_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/callbacks"
	"go.temporal.io/server/service/history/statemachines"
	"go.temporal.io/server/service/history/tasks"
)

func TestMarkedReady(t *testing.T) {
	env := &statemachines.MockEnvironment{
		Version: 3,
	}
	info := &persistencespb.CallbackInfo{
		PublicInfo: &workflowpb.CallbackInfo{
			State: enumspb.CALLBACK_STATE_STANDBY,
		},
	}
	err := callbacks.TransitionMarkedReady.Apply(info, callbacks.EventMarkedReady{}, env)
	require.NoError(t, err)
	// Use this test to verify version and state are synced.
	require.Equal(t, int64(3), info.Version)
	require.Equal(t, enumspb.CALLBACK_STATE_READY, info.PublicInfo.State)
}

func TestValidTransitions(t *testing.T) {
	// Setup
	env := &statemachines.MockEnvironment{
		CurrentTime: time.Now().UTC(),
	}
	info := &persistencespb.CallbackInfo{
		Id: "ID",
		PublicInfo: &workflowpb.CallbackInfo{
			Callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://address:666",
					},
				},
			},
			State: enumspb.CALLBACK_STATE_SCHEDULED,
		},
	}
	// AttemptFailed
	err := callbacks.TransitionAttemptFailed.Apply(info, callbacks.EventAttemptFailed(fmt.Errorf("test")), env)
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	require.False(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	dt := env.CurrentTime.Add(time.Second).Sub(info.PublicInfo.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(env.ScheduledTasks))
	boTask := env.ScheduledTasks[0].(*tasks.CallbackBackoffTask)
	require.Equal(t, info.PublicInfo.Attempt, boTask.Attempt)
	require.Equal(t, "ID", boTask.CallbackID)
	require.Equal(t, info.PublicInfo.NextAttemptScheduleTime.AsTime(), boTask.VisibilityTimestamp)

	// Scheduled
	err = callbacks.TransitionScheduled.Apply(info, callbacks.EventScheduled{}, env)
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Equal(t, 2, len(env.ScheduledTasks))
	cbTask := env.ScheduledTasks[1].(*tasks.CallbackTask)
	require.Equal(t, info.PublicInfo.Attempt, cbTask.Attempt)
	require.Equal(t, "ID", cbTask.CallbackID)
	require.Equal(t, "address:666", cbTask.DestinationAddress)

	// Store the pre-succeeded state to test Failed later
	infoDup := common.CloneProto(info)

	// Succeeded
	env.CurrentTime = env.CurrentTime.Add(time.Second)
	err = callbacks.TransitionSucceeded.Apply(info, callbacks.EventSucceeded{}, env)
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Nil(t, info.PublicInfo.LastAttemptFailure)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks were generated
	require.Equal(t, 2, len(env.ScheduledTasks))

	// Reset back to scheduled
	info = infoDup
	info.PublicInfo.State = enumspb.CALLBACK_STATE_SCHEDULED

	// Failed
	err = callbacks.TransitionFailed.Apply(info, callbacks.EventFailed(fmt.Errorf("failed")), env)
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_FAILED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Equal(t, "failed", info.PublicInfo.LastAttemptFailure.Message)
	require.True(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks were generated
	require.Equal(t, 2, len(env.ScheduledTasks))
}
