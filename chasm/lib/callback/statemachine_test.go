package callback

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/proto"
)

func TestCallbackDestination(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cb      *callbackspb.Callback
		want    string
		wantErr string
	}{
		{
			name: "nexus",
			cb: &callbackspb.Callback{Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{Url: "http://address:666/path/to/callback?query=string"},
			}},
			want: "http://address:666",
		},
		{
			name: "nexus with invalid url",
			cb: &callbackspb.Callback{Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{Url: "http://invalid url/path"},
			}},
			wantErr: "failed to parse URL:",
		},
		{
			name: "nexus handler",
			cb: &callbackspb.Callback{Variant: &callbackspb.Callback_NexusHandler_{
				NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "completions-task-queue"},
			}},
			want: "nexus-handler://completions-task-queue",
		},
		{
			// A variant this server does not recognize, e.g. one persisted by a newer server.
			name: "unrecognized variant",
			cb:   &callbackspb.Callback{},
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := callbackDestination(tc.cb)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

// Scheduling a NexusHandler callback succeeds and routes its invocation task to the target task
// queue. Invoking it is not implemented yet, so the invocation task itself fails; scheduling must
// not, since it runs as part of the execution's close transaction.
func TestTransitionScheduled_NexusHandler(t *testing.T) {
	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_NexusHandler_{
					NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "completions-task-queue"},
				},
			},
		},
	}
	cb.SetStateMachineState(callbackspb.CALLBACK_STATUS_STANDBY)

	mctx := &chasm.MockMutableContext{}
	require.NoError(t, TransitionScheduled.Apply(cb, mctx, EventScheduled{}))

	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, cb.StateMachineState())
	require.Len(t, mctx.Tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.Tasks[0].Payload)
	require.Equal(t, "nexus-handler://completions-task-queue", mctx.Tasks[0].Attributes.Destination)
}

func TestValidTransitions(t *testing.T) {
	// Setup
	currentTime := time.Now().UTC()
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://address:666/path/to/callback?query=string",
					},
				},
			},
		},
	}
	callback.SetStateMachineState(callbackspb.CALLBACK_STATUS_SCHEDULED)

	// AttemptFailed
	mctx := &chasm.MockMutableContext{}
	err := TransitionAttemptFailed.Apply(callback, mctx, EventAttemptFailed{
		Time:        currentTime,
		Err:         errors.New("test"),
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, callback.StateMachineState())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	require.False(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(callback.NextAttemptScheduleTime.AsTime())
	require.Less(t, dt, time.Millisecond*200)

	// Assert backoff task is generated
	require.Len(t, mctx.Tasks, 1)
	require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)

	// Rescheduled
	mctx = &chasm.MockMutableContext{}
	err = TransitionRescheduled.Apply(callback, mctx, EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.StateMachineState())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Len(t, mctx.Tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.Tasks[0].Payload)

	// Store the pre-succeeded state to test Failed later
	dup := &Callback{
		CallbackState: proto.Clone(callback.CallbackState).(*callbackspb.CallbackState),
	}
	dup.Status = callback.StateMachineState()

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	mctx = &chasm.MockMutableContext{}
	err = TransitionSucceeded.Apply(callback, mctx, EventSucceeded{Time: currentTime})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, callback.StateMachineState())
	require.Equal(t, int32(2), callback.Attempt)
	require.Nil(t, callback.LastAttemptFailure)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert no task is generated on success transition
	require.Empty(t, mctx.Tasks)

	// Reset back to scheduled
	callback = dup
	// Increment the time to ensure it's updated in the transition
	currentTime = currentTime.Add(time.Second)

	// failed
	mctx = &chasm.MockMutableContext{}
	err = TransitionFailed.Apply(callback, mctx, EventFailed{Time: currentTime, Err: errors.New("failed")})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, callback.StateMachineState())
	require.Equal(t, int32(2), callback.Attempt)
	require.Equal(t, "failed", callback.LastAttemptFailure.Message)
	require.True(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert task is not generated, failed is terminal
	require.Empty(t, mctx.Tasks)
}
