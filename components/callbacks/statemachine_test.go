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
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/hsm"
)

func TestValidTransitions(t *testing.T) {
	// Setup
	currentTime := time.Now().UTC()
	callback := callbacks.Callback{
		&persistencespb.CallbackInfo{
			Callback: &persistencespb.Callback{
				Variant: &persistencespb.Callback_Nexus_{
					Nexus: &persistencespb.Callback_Nexus{
						Url: "http://address:666/path/to/callback?query=string",
					},
				},
			},
			State: enumsspb.CALLBACK_STATE_SCHEDULED,
		},
	}
	// AttemptFailed
	out, err := callbacks.TransitionAttemptFailed.Apply(callback, callbacks.EventAttemptFailed{
		Time:        currentTime,
		Err:         fmt.Errorf("test"), // nolint:goerr113
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, callback.State())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	require.False(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(callback.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(out.Tasks))
	boTask := out.Tasks[0].(callbacks.BackoffTask)
	require.Equal(t, callback.NextAttemptScheduleTime.AsTime(), boTask.Deadline)

	// Rescheduled
	out, err = callbacks.TransitionRescheduled.Apply(callback, callbacks.EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumsspb.CALLBACK_STATE_SCHEDULED, callback.State())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

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
	require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, callback.State())
	require.Equal(t, int32(2), callback.Attempt)
	require.Nil(t, callback.LastAttemptFailure)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

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
	require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, callback.State())
	require.Equal(t, int32(2), callback.Attempt)
	require.Equal(t, "failed", callback.LastAttemptFailure.Message)
	require.True(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))
}

func TestCompareState(t *testing.T) {
	reg := hsm.NewRegistry()
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	def, ok := reg.Machine(callbacks.StateMachineType)
	require.True(t, ok)

	cases := []struct {
		name                 string
		s1, s2               enumsspb.CallbackState
		attempts1, attempts2 int32
		sign                 int
		expectError          bool
	}{
		{
			name: "standby < scheduled",
			s1:   enumsspb.CALLBACK_STATE_STANDBY,
			s2:   enumsspb.CALLBACK_STATE_SCHEDULED,
			sign: 1,
		},
		{
			name:        "succeeded not comparable to failed",
			s1:          enumsspb.CALLBACK_STATE_SUCCEEDED,
			s2:          enumsspb.CALLBACK_STATE_FAILED,
			expectError: true,
		},
		{
			name: "backing off < failed",
			s1:   enumsspb.CALLBACK_STATE_BACKING_OFF,
			s2:   enumsspb.CALLBACK_STATE_FAILED,
			sign: 1,
		},
		{
			name: "backing off > scheduled",
			s1:   enumsspb.CALLBACK_STATE_BACKING_OFF,
			s2:   enumsspb.CALLBACK_STATE_SCHEDULED,
			sign: -1,
		},
		{
			name:      "backing off < scheduled with greater attempt",
			s1:        enumsspb.CALLBACK_STATE_BACKING_OFF,
			s2:        enumsspb.CALLBACK_STATE_SCHEDULED,
			attempts2: 1,
			sign:      1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s1 := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					State:   tc.s1,
					Attempt: tc.attempts1,
				},
			}
			s2 := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					State:   tc.s2,
					Attempt: tc.attempts2,
				},
			}
			res, err := def.CompareState(s1, s2)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.sign == 0 {
				require.Equal(t, 0, res)
			} else if tc.sign > 0 {
				require.Greater(t, res, 0)
			} else {
				require.Greater(t, 0, res)
			}
		})
	}
}
