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
	"go.temporal.io/server/plugins/callbacks"
)

func TestValidTransitions(t *testing.T) {
	// Setup
	currentTime := time.Now().UTC()
	info := callbacks.Callback{
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
	out, err := callbacks.TransitionAttemptFailed.Apply(info, callbacks.EventAttemptFailed{Time: currentTime, Err: fmt.Errorf("test")})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	require.False(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(info.PublicInfo.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(out.Tasks))
	boTask := out.Tasks[0].(callbacks.BackoffTask)
	require.Equal(t, info.PublicInfo.NextAttemptScheduleTime.AsTime(), boTask.Deadline)

	// Rescheduled
	out, err = callbacks.TransitionRescheduled.Apply(info, callbacks.EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Equal(t, 1, len(out.Tasks))
	cbTask := out.Tasks[0].(callbacks.InvocationTask)
	require.Equal(t, "http://address:666", cbTask.Destination)

	// Store the pre-succeeded state to test Failed later
	infoDup := callbacks.Callback{common.CloneProto(info.CallbackInfo)}

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	out, err = callbacks.TransitionSucceeded.Apply(info, callbacks.EventSucceeded{Time: currentTime})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Nil(t, info.PublicInfo.LastAttemptFailure)
	require.Equal(t, currentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))

	// Reset back to scheduled
	info = infoDup
	info.PublicInfo.State = enumspb.CALLBACK_STATE_SCHEDULED
	currentTime = currentTime.Add(time.Second)

	// Failed
	out, err = callbacks.TransitionFailed.Apply(info, callbacks.EventFailed{Time: currentTime, Err: fmt.Errorf("failed")})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_FAILED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Equal(t, "failed", info.PublicInfo.LastAttemptFailure.Message)
	require.True(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))
}
