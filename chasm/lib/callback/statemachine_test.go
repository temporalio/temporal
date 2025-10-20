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
			NextAttemptScheduleTime: nil, // Will be set below
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

	// Verify state transitioned to SCHEDULED
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status)
	// Verify next attempt schedule time was cleared
	require.Nil(t, callback.NextAttemptScheduleTime)
}

// TestBackoffTaskExecutor_ValidateStateGuard tests that BackoffTask can only execute
// when the callback is in BACKING_OFF state.
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

	tests := []struct {
		name     string
		status   callbackspb.CallbackStatus
		expected bool
	}{
		{
			name:     "valid_backing_off",
			status:   callbackspb.CALLBACK_STATUS_BACKING_OFF,
			expected: true,
		},
		{
			name:     "invalid_standby",
			status:   callbackspb.CALLBACK_STATUS_STANDBY,
			expected: false,
		},
		{
			name:     "invalid_scheduled",
			status:   callbackspb.CALLBACK_STATUS_SCHEDULED,
			expected: false,
		},
		{
			name:     "invalid_succeeded",
			status:   callbackspb.CALLBACK_STATUS_SUCCEEDED,
			expected: false,
		},
		{
			name:     "invalid_failed",
			status:   callbackspb.CALLBACK_STATUS_FAILED,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					Status: tc.status,
				},
			}

			valid, err := executor.Validate(
				nil, // context not used in validator
				callback,
				chasm.TaskAttributes{},
				&callbackspb.BackoffTask{},
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

// TestInvocationTaskExecutor_ValidateStateGuard tests that InvocationTask can only execute
// when the callback is in SCHEDULED state.
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

	tests := []struct {
		name     string
		status   callbackspb.CallbackStatus
		expected bool
	}{
		{
			name:     "valid_scheduled",
			status:   callbackspb.CALLBACK_STATUS_SCHEDULED,
			expected: true,
		},
		{
			name:     "invalid_standby",
			status:   callbackspb.CALLBACK_STATUS_STANDBY,
			expected: false,
		},
		{
			name:     "invalid_backing_off",
			status:   callbackspb.CALLBACK_STATUS_BACKING_OFF,
			expected: false,
		},
		{
			name:     "invalid_succeeded",
			status:   callbackspb.CALLBACK_STATUS_SUCCEEDED,
			expected: false,
		},
		{
			name:     "invalid_failed",
			status:   callbackspb.CALLBACK_STATUS_FAILED,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					Status: tc.status,
				},
			}

			valid, err := executor.Validate(
				nil, // context not used in validator
				callback,
				chasm.TaskAttributes{},
				&callbackspb.InvocationTask{},
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

// TestBackoffTaskExecutor_DestinationExtraction tests that BackoffTaskExecutor
// correctly generates InvocationTask with proper destination extraction.
// This tests the scheme + host extraction matching HSM's behavior.
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

	tests := []struct {
		name                string
		url                 string
		expectedDestination string
		expectedURL         string
		expectError         bool
	}{
		{
			name:                "standard_http_url",
			url:                 "http://localhost:8080/path/to/callback?query=string",
			expectedDestination: "http://localhost:8080",
			expectedURL:         "http://localhost:8080/path/to/callback?query=string",
			expectError:         false,
		},
		{
			name:                "https_url",
			url:                 "https://example.com:443/callback",
			expectedDestination: "https://example.com:443",
			expectedURL:         "https://example.com:443/callback",
			expectError:         false,
		},
		{
			name:                "url_without_port",
			url:                 "http://example.com/callback",
			expectedDestination: "http://example.com",
			expectedURL:         "http://example.com/callback",
			expectError:         false,
		},
		{
			name:                "hsm_test_url",
			url:                 "http://address:666/path/to/callback?query=string",
			expectedDestination: "http://address:666",
			expectedURL:         "http://address:666/path/to/callback?query=string",
			expectError:         false,
		},
		{
			name:        "invalid_url",
			url:         "not a valid url ://",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus{
							Nexus: &callbackspb.Nexus{
								Url: tc.url,
							},
						},
					},
				},
			}

			invocationTask, taskAttrs, err := executor.generateInvocationTask(callback)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, invocationTask)
			require.Equal(t, tc.expectedURL, invocationTask.Url)
			require.Equal(t, tc.expectedDestination, taskAttrs.Destination)
		})
	}
}
