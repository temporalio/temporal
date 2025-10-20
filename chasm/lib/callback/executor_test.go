package callback

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockNexusCompletionGetter implements CanGetNexusCompletion for testing
type mockNexusCompletionGetter struct {
	completion nexusrpc.OperationCompletion
	err        error
}

func (m *mockNexusCompletionGetter) GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error) {
	return m.completion, m.err
}

func TestCallback_Invoke_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		caller                HTTPCaller
		expectedMetricOutcome string
		expectedStatus        callbackspb.CallbackStatus
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:200",
			expectedStatus:        callbackspb.CALLBACK_STATUS_SUCCEEDED,
		},
		{
			name: "network-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			expectedMetricOutcome: "unknown-error",
			expectedStatus:        callbackspb.CALLBACK_STATUS_BACKING_OFF,
		},
		{
			name: "retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:500",
			expectedStatus:        callbackspb.CALLBACK_STATUS_BACKING_OFF,
		},
		{
			name: "non-retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:400",
			expectedStatus:        callbackspb.CALLBACK_STATUS_FAILED,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup mocks
			ns := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:   "namespace-id",
					Name: "namespace-name",
				},
				Config: &persistencespb.NamespaceConfig{},
			})

			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))
			metricsHandler.EXPECT().Timer(RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))

			// Create a mock completion
			completion, err := nexusrpc.NewOperationCompletionSuccessful(nil, nexusrpc.OperationCompletionSuccessfulOptions{})
			require.NoError(t, err)

			// Create callback with pre-populated fields for invocation
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId: "request-id",
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus_{
							Nexus: &callbackspb.Callback_Nexus{
								Url: "http://localhost",
							},
						},
					},
					Status:      callbackspb.CALLBACK_STATUS_SCHEDULED,
					Attempt:     1,
					WorkflowId:  "workflow-id",
					RunId:       "run-id",
					NamespaceId: "namespace-id",
				},
				// Pre-populate the fields needed for invoke()
				completion: completion,
				nexus: &callbackspb.Callback_Nexus{
					Url: "http://localhost",
				},
			}

			// Create executor
			executor := &InvocationTaskExecutor{
				InvocationTaskExecutorOptions: InvocationTaskExecutorOptions{
					Config: &Config{
						RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
						RetryPolicy: func() backoff.RetryPolicy {
							return backoff.NewExponentialRetryPolicy(time.Second)
						},
					},
					MetricsHandler: metricsHandler,
					Logger:         log.NewNoopLogger(),
					HTTPCallerProvider: func(nid queues.NamespaceIDAndDestination) HTTPCaller {
						return tc.caller
					},
				},
			}

			// Test the invoke method directly
			result := callback.invoke(
				context.Background(),
				ns,
				executor,
				chasm.TaskAttributes{
					Destination: "http://localhost",
				},
				&callbackspb.InvocationTask{
					Url: "http://localhost",
				},
			)

			// Verify the result
			require.Equal(t, tc.expectedStatus, result)
		})
	}
}

func TestBackoffTaskExecutor_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := log.NewNoopLogger()
	registry := chasm.NewRegistry(logger)

	nodeBackend := chasm.NewMockNodeBackend(ctrl)
	nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().IsWorkflow().Return(false).AnyTimes()
	nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	tree := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger)

	// Create callback in BACKING_OFF state
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.New(timeSource.Now()),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://localhost",
					},
				},
			},
			Status:                  callbackspb.CALLBACK_STATUS_BACKING_OFF,
			NextAttemptScheduleTime: timestamppb.New(timeSource.Now().Add(time.Minute)),
			Attempt:                 1,
		},
	}

	tree.SetRootComponent(callback)

	executor := &BackoffTaskExecutor{
		BackoffTaskExecutorOptions: BackoffTaskExecutorOptions{
			Config: &Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         logger,
		},
	}

	ctx := chasm.NewMutableContext(context.Background(), tree)

	err := executor.Execute(
		ctx,
		callback,
		chasm.TaskAttributes{},
		&callbackspb.BackoffTask{},
	)
	require.NoError(t, err)

	// Verify state transition from BACKING_OFF to SCHEDULED
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status)
	require.Nil(t, callback.NextAttemptScheduleTime)
}

func TestBackoffTaskExecutor_Validate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := &BackoffTaskExecutor{
		BackoffTaskExecutorOptions: BackoffTaskExecutorOptions{
			Config: &Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         log.NewNoopLogger(),
		},
	}

	tests := []struct {
		name     string
		status   callbackspb.CallbackStatus
		expected bool
	}{
		{
			name:     "valid-backing-off",
			status:   callbackspb.CALLBACK_STATUS_BACKING_OFF,
			expected: true,
		},
		{
			name:     "invalid-scheduled",
			status:   callbackspb.CALLBACK_STATUS_SCHEDULED,
			expected: false,
		},
		{
			name:     "invalid-succeeded",
			status:   callbackspb.CALLBACK_STATUS_SUCCEEDED,
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
				nil,
				callback,
				chasm.TaskAttributes{},
				&callbackspb.BackoffTask{},
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestInvocationTaskExecutor_Validate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := &InvocationTaskExecutor{
		InvocationTaskExecutorOptions: InvocationTaskExecutorOptions{
			Config: &Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         log.NewNoopLogger(),
		},
	}

	tests := []struct {
		name     string
		status   callbackspb.CallbackStatus
		expected bool
	}{
		{
			name:     "valid-scheduled",
			status:   callbackspb.CALLBACK_STATUS_SCHEDULED,
			expected: true,
		},
		{
			name:     "invalid-backing-off",
			status:   callbackspb.CALLBACK_STATUS_BACKING_OFF,
			expected: false,
		},
		{
			name:     "invalid-succeeded",
			status:   callbackspb.CALLBACK_STATUS_SUCCEEDED,
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
				nil,
				callback,
				chasm.TaskAttributes{},
				&callbackspb.InvocationTask{},
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestBackoffTaskExecutor_GenerateInvocationTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := &BackoffTaskExecutor{
		BackoffTaskExecutorOptions: BackoffTaskExecutorOptions{
			Config: &Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
			MetricsHandler: metrics.NewMockHandler(ctrl),
			Logger:         log.NewNoopLogger(),
		},
	}

	// Create a callback with Nexus variant
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://localhost:8080/callback",
					},
				},
			},
		},
	}

	// Test task generation works correctly
	task, err := executor.generateInvocationTask(callback)
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, "http://localhost:8080/callback", task.Url)
}
