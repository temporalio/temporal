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
	"go.temporal.io/server/components/callbacks"
)

func TestValidTransitions(t *testing.T) {
	// Setup
	currentTime := time.Now().UTC()
	callback := callbacks.Callback{
		&persistencespb.CallbackInfo{
			PublicInfo: &workflowpb.CallbackInfo{
				Callback: &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: "http://address:666/path/to/callback?query=string",
						},
					},
				},
				State: enumspb.CALLBACK_STATE_SCHEDULED,
			},
		},
	}
	// AttemptFailed
	out, err := callbacks.TransitionAttemptFailed.Apply(callback, callbacks.EventAttemptFailed{Time: currentTime, Err: fmt.Errorf("test")})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, callback.PublicInfo.State)
	require.Equal(t, int32(1), callback.PublicInfo.Attempt)
	require.Equal(t, "test", callback.PublicInfo.LastAttemptFailure.Message)
	require.False(t, callback.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.PublicInfo.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(callback.PublicInfo.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(out.Tasks))
	boTask := out.Tasks[0].(callbacks.BackoffTask)
	require.Equal(t, callback.PublicInfo.NextAttemptScheduleTime.AsTime(), boTask.Deadline)

	// Rescheduled
	out, err = callbacks.TransitionRescheduled.Apply(callback, callbacks.EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, callback.PublicInfo.State)
	require.Equal(t, int32(1), callback.PublicInfo.Attempt)
	require.Equal(t, "test", callback.PublicInfo.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, callback.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.PublicInfo.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Equal(t, 1, len(out.Tasks))
	cbTask := out.Tasks[0].(callbacks.InvocationTask)
	require.Equal(t, "http://address:666", cbTask.Destination)

	// Store the pre-succeeded state to test Failed later
	dup := callbacks.Callback{common.CloneProto(callback.CallbackInfo)}

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	out, err = callbacks.TransitionSucceeded.Apply(callback, callbacks.EventSucceeded{Time: currentTime})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, callback.PublicInfo.State)
	require.Equal(t, int32(2), callback.PublicInfo.Attempt)
	require.Nil(t, callback.PublicInfo.LastAttemptFailure)
	require.Equal(t, currentTime, callback.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))

	// Reset back to scheduled
	callback = dup
	// Increment the time to ensure it's updated in the transition
	currentTime = currentTime.Add(time.Second)

	// Failed
	out, err = callbacks.TransitionFailed.Apply(callback, callbacks.EventFailed{Time: currentTime, Err: fmt.Errorf("failed")})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_FAILED, callback.PublicInfo.State)
	require.Equal(t, int32(2), callback.PublicInfo.Attempt)
	require.Equal(t, "failed", callback.PublicInfo.LastAttemptFailure.Message)
	require.True(t, callback.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))
}
