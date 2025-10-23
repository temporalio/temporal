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

func (c *testMutableContext) AddTask(component chasm.Component, attributes chasm.TaskAttributes, payload any) {
	c.tasks = append(c.tasks, testTask{component, attributes, payload})
}

func (c *testMutableContext) Now(_ chasm.Component) time.Time {
	return time.Now()
}

func (c *testMutableContext) Ref(_ chasm.Component) ([]byte, error) {
	return nil, nil
}

type testTask struct {
	component  chasm.Component
	attributes chasm.TaskAttributes
	payload    any
}

func newTestMutableContext(t *testing.T) *testMutableContext {
	return &testMutableContext{
		MockMutableContext: chasm.NewMockMutableContext(
			gomock.NewController(t),
		),
	}
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
	callback.SetState(callbackspb.CALLBACK_STATUS_SCHEDULED)

	// AttemptFailed
	mctx := newTestMutableContext(t)
	err := TransitionAttemptFailed.Apply(mctx, callback, EventAttemptFailed{
		Time:        currentTime,
		Err:         errors.New("test"),
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, callback.State())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	require.False(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(callback.NextAttemptScheduleTime.AsTime())
	require.Less(t, dt, time.Millisecond*200)

	// Assert backoff task is generated
	require.Len(t, mctx.tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.tasks[0].payload)

	// Rescheduled
	mctx = newTestMutableContext(t)
	err = TransitionRescheduled.Apply(mctx, callback, EventRescheduled{})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.State())
	require.Equal(t, int32(1), callback.Attempt)
	require.Equal(t, "test", callback.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Len(t, mctx.tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mctx.tasks[0].payload)

	// Store the pre-succeeded state to test Failed later
	dup := &Callback{
		CallbackState: proto.Clone(callback.CallbackState).(*callbackspb.CallbackState),
	}
	dup.Status = callback.State()

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	mctx = newTestMutableContext(t)
	err = TransitionSucceeded.Apply(mctx, callback, EventSucceeded{Time: currentTime})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, callback.State())
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
	require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, callback.State())
	require.Equal(t, int32(2), callback.Attempt)
	require.Equal(t, "failed", callback.LastAttemptFailure.Message)
	require.True(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Assert task is generated (failed transitions also add tasks in chasm)
	require.Len(t, mctx.tasks, 1)
}
