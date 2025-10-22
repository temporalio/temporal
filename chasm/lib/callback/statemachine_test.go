package callback

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// testMutableContext is a minimal test helper for capturing tasks
type testMutableContext struct {
	*chasm.MockMutableContext
	tasks []testTask
}

type testTask struct {
	component  chasm.Component
	attributes chasm.TaskAttributes
	message    any
}

func newTestMutableContext(t *testing.T) *testMutableContext {
	ctrl := gomock.NewController(t)
	mockCtx := chasm.NewMockMutableContext(ctrl)
	tmc := &testMutableContext{
		MockMutableContext: mockCtx,
	}

	// Set up expectations
	mockCtx.EXPECT().AddTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(component chasm.Component, attributes chasm.TaskAttributes, message any) {
			tmc.tasks = append(tmc.tasks, testTask{
				component:  component,
				attributes: attributes,
				message:    message,
			})
		},
	).AnyTimes()

	mockCtx.EXPECT().Now(gomock.Any()).Return(time.Now()).AnyTimes()
	mockCtx.EXPECT().Ref(gomock.Any()).Return(nil, nil).AnyTimes()

	return tmc
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
	callback.SetStatus(callbackspb.CALLBACK_STATUS_SCHEDULED)

	// AttemptFailed
	mctx := newTestMutableContext(t)
	err := TransitionAttemptFailed.Apply(mctx, callback, EventAttemptFailed{
		Time:        currentTime,
		Err:         errors.New("test"),
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, callback.Status())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	require.False(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(callback.NextAttemptScheduleTime.AsTime())
	require.Less(t, dt, time.Millisecond*200)

	// Assert backoff task is generated
	require.Len(t, mctx.tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.tasks[0].message)

	// Rescheduled
	mctx = newTestMutableContext(t)
	err = TransitionRescheduled.Apply(mctx, callback, EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Len(t, mctx.tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.tasks[0].message)

	// Store the pre-succeeded state to test Failed later
	dup := &Callback{
		CallbackState: proto.Clone(callback.CallbackState).(*callbackspb.CallbackState),
	}
	dup.CallbackState.Status = callback.Status()

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	mctx = newTestMutableContext(t)
	err = TransitionSucceeded.Apply(mctx, callback, EventSucceeded{Time: currentTime})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, callback.Status())
	require.Equal(t, int32(2), callback.Attempt)
	require.Nil(t, callback.LastAttemptFailure)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert task is generated (success transitions also add tasks in chasm)
	require.Len(t, mctx.tasks, 1)

	// Reset back to scheduled
	callback = dup
	// Increment the time to ensure it's updated in the transition
	currentTime = currentTime.Add(time.Second)

	// failed
	mctx = newTestMutableContext(t)
	err = TransitionFailed.Apply(mctx, callback, EventFailed{Time: currentTime, Err: errors.New("failed")})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, callback.Status())
	require.Equal(t, int32(2), callback.Attempt)
	require.Equal(t, "failed", callback.LastAttemptFailure.Message)
	require.True(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert task is generated (failed transitions also add tasks in chasm)
	require.Len(t, mctx.tasks, 1)
}
