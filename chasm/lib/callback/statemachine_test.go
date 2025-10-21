package callback

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestBackoffTaskExecutor_StateTransition tests that BackoffTaskExecutor correctly transitions
// callback from BACKING_OFF to SCHEDULED state and clears the next attempt schedule time.
func TestBackoffTaskExecutor_StateTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := log.NewNoopLogger()
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now().UTC()
	timeSource.Update(currentTime)

	nodeBackend := chasm.NewMockNodeBackend(ctrl)
	tv := testvars.New(t)
	nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	nodeBackend.EXPECT().IsWorkflow().Return(false).AnyTimes()
	nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()

	registry := chasm.NewRegistry(logger)
	tree := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger)

	// Create callback in BACKING_OFF state with next attempt scheduled
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId: "test-request",
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus{
					Nexus: &callbackspb.Nexus{
						Url: "http://localhost:8080/callback",
					},
				},
			},
			Status:                  callbackspb.CALLBACK_STATUS_BACKING_OFF,
			Attempt:                 1,
			NextAttemptScheduleTime: timestamppb.Now(),
		},
	}

	tree.SetRootComponent(callback)

	executor := NewBackoffTaskExecutor(BackoffTaskExecutorOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		Logger:         logger,
	})

	ctx := chasm.NewMutableContext(context.Background(), tree)
	err := executor.Execute(ctx, callback, chasm.TaskAttributes{}, &callbackspb.BackoffTask{})
	require.NoError(t, err)

	// Verify state transitioned to SCHEDULED and next attempt schedule time cleared
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status)
	require.Nil(t, callback.NextAttemptScheduleTime)
}

func TestBackoffTaskExecutor_ValidateStateGuard(t *testing.T) {
	logger := log.NewNoopLogger()
	executor := NewBackoffTaskExecutor(BackoffTaskExecutorOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		Logger:         logger,
	})

	// Test valid state
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Status: callbackspb.CALLBACK_STATUS_BACKING_OFF,
		},
	}
	valid, err := executor.Validate(nil, callback, chasm.TaskAttributes{}, &callbackspb.BackoffTask{})
	require.NoError(t, err)
	require.True(t, valid)

	// Test invalid state
	callback.Status = callbackspb.CALLBACK_STATUS_SCHEDULED
	valid, err = executor.Validate(nil, callback, chasm.TaskAttributes{}, &callbackspb.BackoffTask{})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestInvocationTaskExecutor_ValidateStateGuard(t *testing.T) {
	logger := log.NewNoopLogger()
	executor := NewInvocationTaskExecutor(InvocationTaskExecutorOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		Logger:         logger,
	})

	// Test valid state
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
	}
	valid, err := executor.Validate(nil, callback, chasm.TaskAttributes{}, &callbackspb.InvocationTask{})
	require.NoError(t, err)
	require.True(t, valid)

	// Test invalid state
	callback.Status = callbackspb.CALLBACK_STATUS_BACKING_OFF
	valid, err = executor.Validate(nil, callback, chasm.TaskAttributes{}, &callbackspb.InvocationTask{})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestBackoffTaskExecutor_DestinationExtraction(t *testing.T) {
	logger := log.NewNoopLogger()
	executor := NewBackoffTaskExecutor(BackoffTaskExecutorOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		Logger:         logger,
	})

	// Test valid URL with path and query parameters
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus{
					Nexus: &callbackspb.Nexus{
						Url: "http://localhost:8080/path/to/callback?query=string",
					},
				},
			},
		},
	}

	invocationTask, taskAttrs, err := executor.generateInvocationTask(callback)
	require.NoError(t, err)
	require.NotNil(t, invocationTask)
	require.Equal(t, "http://localhost:8080/path/to/callback?query=string", invocationTask.Url)
	require.Equal(t, "http://localhost:8080", taskAttrs.Destination)

	// Test invalid URL
	callback.Callback.Variant.(*callbackspb.Callback_Nexus).Nexus.Url = "not a valid url ://"
	_, _, err = executor.generateInvocationTask(callback)
	require.Error(t, err)
}
