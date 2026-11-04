package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRequestDeduplicationAfterTerminalState(t *testing.T) {
	t.Run("CancellationAfterCanceled", func(t *testing.T) {
		const requestID = "cancel-request-id"

		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
				CancelState: &activitypb.ActivityCancelState{
					RequestId: requestID,
				},
			},
		}

		_, err := activity.handleCancellationRequested(ctx, &activitypb.RequestCancelActivityExecutionRequest{
			FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{
				RequestId: requestID,
			},
		})
		require.NoError(t, err)
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)
	})

	t.Run("NewCancellationAfterCanceled", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
				CancelState: &activitypb.ActivityCancelState{
					RequestId: "original-request-id",
				},
			},
		}

		_, err := activity.handleCancellationRequested(ctx, &activitypb.RequestCancelActivityExecutionRequest{
			FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{
				RequestId: "new-request-id",
			},
		})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		require.EqualError(t, err, "no running activity execution: it closed with status Canceled")
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)
	})

	t.Run("TerminationAfterTerminated", func(t *testing.T) {
		const requestID = "terminate-request-id"

		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				TerminateState: &activitypb.ActivityTerminateState{
					RequestId: requestID,
				},
			},
		}

		_, err := activity.Terminate(ctx, chasm.TerminateComponentRequest{
			RequestID: requestID,
		})
		require.NoError(t, err)
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	})

	t.Run("NewTerminationAfterTerminated", func(t *testing.T) {
		const requestID = "terminate-request-id"

		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				TerminateState: &activitypb.ActivityTerminateState{
					RequestId: requestID,
				},
			},
		}

		_, err := activity.Terminate(ctx, chasm.TerminateComponentRequest{
			RequestID: "new-request-id",
		})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		require.EqualError(t, err, "no running activity execution: it closed with status Terminated")
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	})

	// A request that carries no request id cannot be recognized as a retry of the one that
	// terminated the activity, however the activity was terminated.
	t.Run("TerminationWithoutRequestIDAfterTerminated", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:         activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				TerminateState: &activitypb.ActivityTerminateState{},
			},
		}

		_, err := activity.Terminate(ctx, chasm.TerminateComponentRequest{})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	})

	t.Run("PauseAfterTerminated", func(t *testing.T) {
		const requestID = "pause-request-id"

		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				LastPauseState: &activitypb.ActivityPauseState{
					RequestId: requestID,
				},
			},
		}

		_, err := activity.handlePauseRequested(ctx, &activitypb.PauseActivityExecutionRequest{
			FrontendRequest: &workflowservice.PauseActivityExecutionRequest{
				RequestId: requestID,
			},
		})
		require.NoError(t, err)
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	})

	t.Run("NewPauseAfterTerminated", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				LastPauseState: &activitypb.ActivityPauseState{
					RequestId: "pause-request-id",
				},
			},
		}

		_, err := activity.handlePauseRequested(ctx, &activitypb.PauseActivityExecutionRequest{
			FrontendRequest: &workflowservice.PauseActivityExecutionRequest{
				RequestId: "new-request-id",
			},
		})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		require.EqualError(t, err, "no running activity execution: it closed with status Terminated")
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	})
}

// Check that we do not emit a StartToCloseLatency metric when cancelling an activity that has no
// attempt in progress. Cancelling a SCHEDULED or PAUSED activity transitions straight to Canceled,
// and the status captured before the CancelRequested transition must be forwarded so that the
// metric is not emitted.
func TestHandleCancellationRequestedDirectCancelMetrics(t *testing.T) {
	testCases := []struct {
		name   string
		status activitypb.ActivityExecutionStatus
	}{
		{
			name:   "scheduled during retry backoff, stale StartedTime",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:   "paused during retry backoff, stale StartedTime",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()

			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					HandleNamespaceEntry: testNamespaceEntry,
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
						},
					}),
				},
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					Status:                 tc.status,
					ScheduleTime:           timestamppb.New(defaultTime),
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
				},
				// A stale StartedTime from a prior attempt: no attempt is currently running, so
				// StartToCloseLatency must not be recorded on cancellation.
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       1,
					StartedTime: timestamppb.New(defaultTime),
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			_, err := activity.handleCancellationRequested(ctx, &activitypb.RequestCancelActivityExecutionRequest{
				FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{Reason: "test"},
			})
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)

			snapshot := capture.Snapshot()
			require.NotEmpty(t, snapshot[metrics.ActivityCancel.Name()])
			require.NotEmpty(t, snapshot[metrics.ActivityScheduleToCloseLatency.Name()])
			require.Empty(t, snapshot[metrics.ActivityStartToCloseLatency.Name()],
				"no attempt was running; StartToCloseLatency must not be recorded")
		})
	}
}

func TestUpdateStartedActivityExecutionOptionsDoesNotBumpStartedStamp(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow:            func(chasm.Component) time.Time { return testTime },
			HandleNamespaceEntry: testNamespaceEntry,
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
			}),
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
			StartToCloseTimeout:    durationpb.New(3 * time.Minute),
			HeartbeatTimeout:       durationpb.New(time.Minute),
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:        1,
			Stamp:        7,
			StartedStamp: 7,
		}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}
	attempt := activity.LastAttempt.Get(ctx)
	originalStamp := attempt.GetStamp()
	originalStartedStamp := attempt.GetStartedStamp()

	_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
		FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId: "test-activity-id",
			RequestId:  "update-request-id",
			ActivityOptions: &apiactivitypb.ActivityOptions{
				HeartbeatTimeout: durationpb.New(2 * time.Minute),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		},
	})
	require.NoError(t, err)

	require.Equal(t, originalStamp+1, attempt.GetStamp())
	require.Equal(t, originalStartedStamp, attempt.GetStartedStamp())
}

func TestHandlePauseRequestedDedupBeforeValidation(t *testing.T) {
	for _, status := range []activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status: status,
					LastPauseState: &activitypb.ActivityPauseState{
						RequestId: "pause-request-id",
					},
				},
			}

			_, err := activity.handlePauseRequested(
				&chasm.MockMutableContext{},
				&activitypb.PauseActivityExecutionRequest{
					FrontendRequest: &workflowservice.PauseActivityExecutionRequest{
						RequestId: "pause-request-id",
					},
				},
			)
			require.NoError(t, err)
			require.Equal(t, status, activity.GetStatus())
		})
	}
}

func TestHandleUnpauseRequestedRequestID(t *testing.T) {
	t.Run("deduplicates latest request ID", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
				Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				LastUnpauseRequestId:   "unpause-request-id",
				ScheduleToStartTimeout: durationpb.New(time.Minute),
			},
			LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 3}),
		}

		_, err := activity.handleUnpauseRequested(ctx, &activitypb.UnpauseActivityExecutionRequest{
			FrontendRequest: &workflowservice.UnpauseActivityExecutionRequest{
				RequestId: "unpause-request-id",
			},
		})
		require.NoError(t, err)
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED, activity.GetStatus())
		require.Equal(t, int32(3), activity.LastAttempt.Get(ctx).GetCount())
	})

	t.Run("does not persist request ID when unpause fails", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			},
		}

		_, err := activity.handleUnpauseRequested(ctx, &activitypb.UnpauseActivityExecutionRequest{
			FrontendRequest: &workflowservice.UnpauseActivityExecutionRequest{
				RequestId: "unpause-request-id",
			},
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Empty(t, activity.GetLastUnpauseRequestId(), "a failed unpause must not be recorded for de-dup")
	})

	t.Run("does not record failed request ID", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:               activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				LastUnpauseRequestId: "previous-unpause-request-id",
			},
		}

		_, err := activity.handleUnpauseRequested(ctx, &activitypb.UnpauseActivityExecutionRequest{
			FrontendRequest: &workflowservice.UnpauseActivityExecutionRequest{
				RequestId: "failed-unpause-request-id",
			},
		})
		require.Error(t, err)
		require.Equal(t, "previous-unpause-request-id", activity.GetLastUnpauseRequestId())
	})
}

func TestHandleResetRequestID(t *testing.T) {
	t.Run("deduplicates latest request ID", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType:       &commonpb.ActivityType{Name: "test-activity-type"},
				Status:             activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
				TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				LastResetRequestId: "reset-request-id",
			},
			LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 3}),
		}

		_, err := activity.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{
			FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
				RequestId: "reset-request-id",
			},
		})
		require.NoError(t, err)
		require.Equal(t, int32(3), activity.LastAttempt.Get(ctx).GetCount())
	})

	t.Run("records successful request ID", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
				Status:       activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			},
			LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 3}),
		}

		_, err := activity.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{
			FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
				RequestId: "reset-request-id",
			},
		})
		require.NoError(t, err)
		require.Equal(t, "reset-request-id", activity.GetLastResetRequestId())
	})

	t.Run("does not record failed request ID", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType:       &commonpb.ActivityType{Name: "test-activity-type"},
				Status:             activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				LastResetRequestId: "previous-reset-request-id",
			},
		}

		_, err := activity.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{
			FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
				RequestId: "failed-reset-request-id",
			},
		})
		require.Error(t, err)
		require.Equal(t, "previous-reset-request-id", activity.GetLastResetRequestId())
	})
}

func TestUpdateActivityExecutionOptionsRequestID(t *testing.T) {
	t.Run("deduplicates latest request ID", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:                     activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				TaskQueue:                  &taskqueuepb.TaskQueue{Name: "current-task-queue"},
				LastUpdateOptionsRequestId: "update-request-id",
			},
		}

		resp, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
			FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
				RequestId: "update-request-id",
			},
		})
		require.NoError(t, err)
		require.Equal(t, "current-task-queue", resp.GetFrontendResponse().GetActivityOptions().GetTaskQueue().GetName())
	})

	t.Run("records successful request ID", func(t *testing.T) {
		ctx := newOperatorCommandTestContext(t)
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
				Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
				ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
				StartToCloseTimeout:    durationpb.New(3 * time.Minute),
				HeartbeatTimeout:       durationpb.New(time.Minute),
			},
			LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
				Count:        1,
				Stamp:        7,
				StartedStamp: 7,
			}),
			Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		}

		_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
			FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
				RequestId: "update-request-id",
				ActivityOptions: &apiactivitypb.ActivityOptions{
					HeartbeatTimeout: durationpb.New(2 * time.Minute),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
			},
		})
		require.NoError(t, err)
		require.Equal(t, "update-request-id", activity.GetLastUpdateOptionsRequestId())
	})

	t.Run("does not record failed request ID", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:                     activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				LastUpdateOptionsRequestId: "previous-update-request-id",
			},
		}

		_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
			FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
				RequestId: "failed-update-request-id",
			},
		})
		require.Error(t, err)
		require.Equal(t, "previous-update-request-id", activity.GetLastUpdateOptionsRequestId())
	})
}

func newOperatorCommandTestContext(t *testing.T) *chasm.MockMutableContext {
	t.Helper()
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow:            func(chasm.Component) time.Time { return time.Unix(0, 0) },
			HandleNamespaceEntry: testNamespaceEntry,
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
			}),
		},
	}
}

func TestMergeActivityOptionsRejectsInvalidMergedRetryPolicy(t *testing.T) {
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "T"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "Q"},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			ScheduleToStartTimeout: durationpb.New(20 * time.Second),
			StartToCloseTimeout:    durationpb.New(10 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(10 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(30 * time.Second),
				MaximumAttempts:    5,
			},
		},
	}

	_, err := activity.mergeActivityOptions(nil, &workflowservice.UpdateActivityExecutionOptionsRequest{
		ActivityId: "act",
		ActivityOptions: &apiactivitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(60 * time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
	})
	require.ErrorContains(t, err, "MaximumInterval cannot be less than InitialInterval")
}

func TestShouldRecalculateCurrentRetryInterval(t *testing.T) {
	retryInterval := 2 * time.Second

	testCases := []struct {
		name                 string
		status               activitypb.ActivityExecutionStatus
		restoreOriginal      bool
		updateFields         map[string]struct{}
		currentRetryInterval *durationpb.Duration
		retryIntervalSource  activitypb.ActivityRetryIntervalSource
		expectRecalculate    bool
	}{
		{
			name:                 "retry policy subfield update with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "retry policy replacement with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "restore original with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			restoreOriginal:      true,
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "unrelated update preserves retry interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"heartbeatTimeout": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
		},
		{
			name:                 "worker override is preserved regardless of value",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE,
		},
		{
			name:                 "unspecified source (pre-existing attempt state) is preserved",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED,
		},
		{
			name:                 "started activity is not in retry backoff",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
		},
		{
			name:         "missing retry interval",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields: map[string]struct{}{"retryPolicy.initialInterval": {}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status: tc.status,
				},
			}
			attempt := &activitypb.ActivityAttemptState{
				CurrentRetryInterval:       tc.currentRetryInterval,
				CurrentRetryIntervalSource: tc.retryIntervalSource,
			}

			got := activity.shouldRecalculateCurrentRetryInterval(
				attempt,
				tc.restoreOriginal,
				tc.updateFields,
				false,
			)

			require.Equal(t, tc.expectRecalculate, got)
		})
	}
}

// TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions verifies
// that missing or incomplete original options are rejected without changing the activity.
func TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name            string
		originalOptions *apiactivitypb.ActivityOptions
		expectedError   string
	}{
		{
			name:          "missing snapshot",
			expectedError: "cannot restore original options: no original task queue was recorded for this activity",
		},
		{
			name: "incomplete phase timeout coverage",
			originalOptions: &apiactivitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "original-task-queue"},
				AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{
					StartToHandoffTimeout: durationpb.New(time.Minute),
				},
			},
			expectedError: "cannot restore original options: a positive schedule-to-close or start-to-close timeout, or both handoff phase timeouts, is required",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
				},
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
					ActivityType:           &commonpb.ActivityType{Name: "T"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "current-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
					StartToCloseTimeout:    durationpb.New(10 * time.Second),
					OriginalOptions:        testCase.originalOptions,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
			}

			_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
				FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
					ActivityId:      "act",
					RestoreOriginal: true,
					RequestId:       "failed-update-request-id",
				},
			})

			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.ErrorContains(t, err, testCase.expectedError)
			require.Empty(t, activity.GetLastUpdateOptionsRequestId())
			require.Equal(t, "current-task-queue", activity.GetTaskQueue().GetName())
			require.Equal(t, 30*time.Second, activity.GetScheduleToCloseTimeout().AsDuration())
			require.Equal(t, 10*time.Second, activity.GetStartToCloseTimeout().AsDuration())
		})
	}
}

// TestHandleReset_RestoreOriginalOptions_RejectsMissingOriginalOptions covers the equivalent gap
// on the Reset(RestoreOriginalOptions=true) path, for an activity with no original_options
// snapshot (see the Update test above for the scenario this mimics).
func TestHandleReset_RestoreOriginalOptions_RejectsMissingOriginalOptions(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			ActivityType:           &commonpb.ActivityType{Name: "T"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "current-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			StartToCloseTimeout:    durationpb.New(10 * time.Second),
			OriginalOptions:        nil,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
	}

	_, err := activity.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{
		FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
			ActivityId:             "act",
			RestoreOriginalOptions: true,
			RequestId:              "failed-reset-request-id",
		},
	})

	require.Error(t, err)
	require.Empty(t, activity.GetLastResetRequestId())
	require.Equal(t, "current-task-queue", activity.GetTaskQueue().GetName())
}

// TestHandoffTimeoutUpdates verifies that timeout edits use the existing pickup or handoff time,
// preserve task tokens and retry policies, and invalidate superseded timers.
func TestHandoffTimeoutUpdates(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                     string
		backoff, handedOff       bool
		field                    string
		timeout                  time.Duration
		expectedBudget           time.Duration
		expectedHeartbeatTimeout time.Duration
		expectedPhaseTimeout     time.Duration
	}{
		{name: "shorten handoff phase", field: "async_handoff_options.start_to_handoff_timeout", timeout: 5 * time.Second, expectedBudget: time.Minute, expectedHeartbeatTimeout: 5 * time.Second, expectedPhaseTimeout: 5 * time.Second},
		{name: "shorten completion phase", handedOff: true, field: "async_handoff_options.handoff_to_close_timeout", timeout: 5 * time.Second, expectedBudget: time.Minute, expectedHeartbeatTimeout: 5 * time.Second, expectedPhaseTimeout: 5 * time.Second},
		{name: "edit pre handoff heartbeat", field: "async_handoff_options.heartbeat_timeout", timeout: 5 * time.Second, expectedBudget: time.Minute, expectedHeartbeatTimeout: 5 * time.Second},
		{name: "shorten carried budget during backoff", backoff: true, field: "start_to_close_timeout", timeout: 5 * time.Second, expectedBudget: 5 * time.Second},
		{name: "extend carried budget during backoff", backoff: true, field: "start_to_close_timeout", timeout: 2 * time.Minute, expectedBudget: 2 * time.Minute},
		{name: "disable budget without clearing its anchor", field: "start_to_close_timeout"},
		{name: "pre handoff heartbeat remains independent", field: "heartbeat_timeout", timeout: 5 * time.Second, expectedBudget: time.Minute, expectedHeartbeatTimeout: 20 * time.Second},
		{name: "post handoff heartbeat uses handoff anchor", handedOff: true, field: "heartbeat_timeout", timeout: 5 * time.Second, expectedBudget: time.Minute, expectedHeartbeatTimeout: 5 * time.Second},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)

			// Timeout edits must not replace an existing unlimited policy with changed namespace defaults.
			a.AsyncHandoffOptions.RetryPolicy.MaximumAttempts = 0
			expectedRetryPolicy := common.CloneProto(a.GetAsyncHandoffOptions().GetRetryPolicy())
			defaults := retrypolicy.DefaultDefaultRetrySettings
			defaults.MaximumAttempts = 99
			activityContextFromChasm(ctx).config.DefaultActivityRetryPolicy = dynamicconfig.GetTypedPropertyFnFilteredByNamespace(defaults)

			a.AsyncHandoffOptions.HeartbeatTimeout = durationpb.New(20 * time.Second)
			a.HeartbeatTimeout = durationpb.New(30 * time.Second)
			a.AwaitingCompletion = testCase.handedOff
			if testCase.handedOff {
				a.AsyncHandoffTime = timestamppb.New(defaultTime.Add(20 * time.Second))
				ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime.Add(40 * time.Second) }
			}

			attempt := a.LastAttempt.Get(ctx)
			if testCase.backoff {
				_, err := a.tryReschedule(ctx, retryEvent{failure: &failurepb.Failure{Message: "retry"}})
				require.NoError(t, err)
			}

			before := common.CloneProto(attempt)
			heartbeatStartedAt := before.GetStartedTime().AsTime()
			if testCase.handedOff {
				heartbeatStartedAt = a.GetAsyncHandoffTime().AsTime()
			}

			ctx.Tasks = nil
			options := &apiactivitypb.ActivityOptions{}
			switch testCase.field {
			case "start_to_close_timeout":
				options.StartToCloseTimeout = durationpb.New(testCase.timeout)
			case "heartbeat_timeout":
				options.HeartbeatTimeout = durationpb.New(testCase.timeout)
			case "async_handoff_options.start_to_handoff_timeout":
				options.AsyncHandoffOptions = &apiactivitypb.AsyncHandoffOptions{StartToHandoffTimeout: durationpb.New(testCase.timeout)}
			case "async_handoff_options.handoff_to_close_timeout":
				options.AsyncHandoffOptions = &apiactivitypb.AsyncHandoffOptions{HandoffToCloseTimeout: durationpb.New(testCase.timeout)}
			case "async_handoff_options.heartbeat_timeout":
				options.AsyncHandoffOptions = &apiactivitypb.AsyncHandoffOptions{HeartbeatTimeout: durationpb.New(testCase.timeout)}
			default:
				t.Fatalf("unsupported timeout field %q", testCase.field)
			}

			response, err := a.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
				Namespace: "test-namespace", ActivityId: "test-activity", ActivityOptions: options,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{testCase.field}},
			}})
			require.NoError(t, err)
			require.NotNil(t, response.GetFrontendResponse().GetActivityOptions().GetAsyncHandoffOptions())
			protorequire.ProtoEqual(t, expectedRetryPolicy, a.GetAsyncHandoffOptions().GetRetryPolicy())
			require.Equal(t, before.GetCount(), attempt.GetCount())
			require.Equal(t, before.GetAsyncHandoffState().GetCount(), attempt.GetAsyncHandoffState().GetCount())
			protorequire.ProtoEqual(t, before.GetAsyncHandoffState().GetStartToCloseStartedTime(), attempt.GetAsyncHandoffState().GetStartToCloseStartedTime())
			require.Equal(t, before.GetStartedStamp(), attempt.GetStartedStamp())
			require.Equal(t, before.GetAsyncHandoffState().GetStartToCloseStamp()+1, attempt.GetAsyncHandoffState().GetStartToCloseStamp())

			var budget, heartbeat, phase time.Time
			for _, task := range ctx.Tasks {
				switch task.Payload.(type) {
				case *activitypb.HandoffStartToCloseTimeoutTask:
					budget = task.Attributes.ScheduledTime
				case *activitypb.HeartbeatTimeoutTask:
					heartbeat = task.Attributes.ScheduledTime
				case *activitypb.StartToHandoffTimeoutTask, *activitypb.HandoffToCloseTimeoutTask:
					phase = task.Attributes.ScheduledTime
				default:
					continue
				}
			}

			if testCase.expectedBudget > 0 {
				require.Equal(t, before.GetAsyncHandoffState().GetStartToCloseStartedTime().AsTime().Add(testCase.expectedBudget), budget)
			} else {
				require.True(t, budget.IsZero())
			}

			if testCase.expectedHeartbeatTimeout > 0 {
				require.Equal(t, heartbeatStartedAt.Add(testCase.expectedHeartbeatTimeout), heartbeat)
			} else if testCase.backoff {
				require.True(t, heartbeat.IsZero())
			}

			if testCase.expectedPhaseTimeout > 0 {
				require.Equal(t, heartbeatStartedAt.Add(testCase.expectedPhaseTimeout), phase)
				invocation := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: phase}}
				var valid bool
				var err error
				if testCase.handedOff {
					valid, err = newHandoffToCloseTimeoutTaskHandler().Validate(ctx, a, invocation, &activitypb.HandoffToCloseTimeoutTask{Stamp: before.GetStamp()})
				} else {
					valid, err = newStartToHandoffTimeoutTaskHandler().Validate(ctx, a, invocation, &activitypb.StartToHandoffTimeoutTask{Stamp: before.GetStamp()})
				}
				require.NoError(t, err)
				require.False(t, valid, "the old delivery generation must not enforce edited phase timeouts")
			}

			valid, err := newHandoffStartToCloseTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{}, &activitypb.HandoffStartToCloseTimeoutTask{Stamp: before.GetAsyncHandoffState().GetStartToCloseStamp()})
			require.NoError(t, err)
			require.False(t, valid)
		})
	}
}

// TestLiveHandoffOptionsUpdate checks exact clock conversion and atomic rejection of invalid state.
// Public RPC tests cover the lifecycle; these cases inspect timer deadlines and unchanged worker credentials.
func TestLiveHandoffOptionsUpdate(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                                              string
		setup                                             func(*Activity, *chasm.MockMutableContext)
		options                                           *apiactivitypb.ActivityOptions
		paths                                             []string
		expectedHandoffCount                              int32
		expectedStartedAt                                 time.Time
		expectedCompletion                                bool
		expectedStartToCloseTimer, expectedHeartbeatTimer time.Time
		invalid                                           bool
		invalidState                                      bool
	}{
		{
			name: "add to running task", paths: []string{"async_handoff_options"}, expectedHandoffCount: 1, expectedStartedAt: defaultTime,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.AsyncHandoffOptions = nil
				a.LastAttempt.Get(ctx).AsyncHandoffState = nil
			},
		},
		{
			name: "add after reporting-only handoff", paths: []string{"async_handoff_options", "start_to_close_timeout", "schedule_to_close_timeout"}, expectedHandoffCount: 1, expectedStartedAt: defaultTime, expectedCompletion: true,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{StartToHandoffTimeout: durationpb.New(5 * time.Second), HandoffToCloseTimeout: durationpb.New(120 * time.Second)}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.AsyncHandoffOptions = nil
				a.LastAttempt.Get(ctx).AsyncHandoffState = nil
				a.AwaitingCompletion = true
				a.AsyncHandoffTime = timestamppb.New(defaultTime.Add(10 * time.Second))
				ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime.Add(60 * time.Second) }
			},
		},
		{
			name: "remove from completion phase", paths: []string{"async_handoff_options"}, options: &apiactivitypb.ActivityOptions{}, expectedCompletion: true,
			expectedStartToCloseTimer: defaultTime.Add(65 * time.Second), expectedHeartbeatTimer: defaultTime.Add(25 * time.Second),
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				// The current pickup differs from both the first pickup and accepted handoff.
				a.LastAttempt.Get(ctx).StartedTime = timestamppb.New(defaultTime.Add(5 * time.Second))
				a.HeartbeatTimeout = durationpb.New(20 * time.Second)
				a.AwaitingCompletion = true
				a.AsyncHandoffTime = timestamppb.New(defaultTime.Add(7 * time.Second))
			},
		},

		{
			name: "edit after handoff redispatch preserves first pickup", paths: []string{"async_handoff_options.heartbeat_timeout"},
			expectedHandoffCount: 2, expectedStartedAt: defaultTime,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{HeartbeatTimeout: durationpb.New(time.Second)}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.LastAttempt.Get(ctx).StartedTime = timestamppb.New(defaultTime.Add(5 * time.Second))
			},
		},
		{
			name: "new activity retry waits for pickup", paths: []string{"async_handoff_options.heartbeat_timeout"}, expectedHandoffCount: 1,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
				attempt := a.LastAttempt.Get(ctx)
				attempt.AsyncHandoffState.Count = 1
				attempt.AsyncHandoffState.StartToCloseStartedTime = nil
				attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY
				attempt.CurrentRetryInterval = durationpb.New(time.Second)
			},
		},
		{
			name: "missing running clock rejects update atomically", paths: []string{"async_handoff_options"}, invalidState: true,
			options: &apiactivitypb.ActivityOptions{},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.LastAttempt.Get(ctx).AsyncHandoffState.StartToCloseStartedTime = nil
			},
		},
		{
			name: "completion without a delivery rejects update atomically", paths: []string{"async_handoff_options"}, invalidState: true,
			options: &apiactivitypb.ActivityOptions{},
			setup: func(a *Activity, _ *chasm.MockMutableContext) {
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
				a.AwaitingCompletion = true
				a.AsyncHandoffTime = timestamppb.New(defaultTime.Add(time.Second))
			},
		},
		{
			name: "add while queued", paths: []string{"async_handoff_options"}, expectedHandoffCount: 1,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
				a.AsyncHandoffTime = timestamppb.New(defaultTime.Add(time.Second))
				a.AsyncHandoffOptions = nil
				a.LastAttempt.Get(ctx).AsyncHandoffState = nil
			},
		},
		{
			name: "add while paused", paths: []string{"async_handoff_options"}, expectedHandoffCount: 1,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
				a.AsyncHandoffOptions = nil
				a.LastAttempt.Get(ctx).AsyncHandoffState = nil
			},
		},
		{
			name: "phase-only removal requires a legacy bound", paths: []string{"async_handoff_options"}, options: &apiactivitypb.ActivityOptions{}, invalid: true,
			setup: func(a *Activity, _ *chasm.MockMutableContext) {
				a.StartToCloseTimeout = nil
				a.ScheduleToCloseTimeout = nil
			},
		},
		{
			name: "disabled admission leaves current task unchanged", paths: []string{"async_handoff_options"}, invalid: true,
			options: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			setup: func(a *Activity, ctx *chasm.MockMutableContext) {
				a.AsyncHandoffOptions = nil
				a.LastAttempt.Get(ctx).AsyncHandoffState = nil
				activityContextFromChasm(ctx).config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			config := activityContextFromChasm(ctx).config
			config.StandaloneAsyncHandoffEnabled = func(namespaceName string) bool { return namespaceName == testNamespaceEntry().Name().String() }
			config.DefaultActivityRetryPolicy = dynamicconfig.GetTypedPropertyFnFilteredByNamespace(retrypolicy.DefaultDefaultRetrySettings)
			testCase.setup(a, ctx)
			beforeState := common.CloneProto(a.ActivityState)
			beforeAttempt := common.CloneProto(a.LastAttempt.Get(ctx))
			request := &workflowservice.UpdateActivityExecutionOptionsRequest{
				ActivityId: "activity", ActivityOptions: common.CloneProto(testCase.options),
				UpdateMask: &fieldmaskpb.FieldMask{Paths: testCase.paths},
			}
			beforeRequest := common.CloneProto(request)

			_, err := a.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{FrontendRequest: request})
			protorequire.ProtoEqual(t, beforeRequest, request)
			if testCase.invalid || testCase.invalidState {
				if testCase.invalidState {
					require.ErrorAs(t, err, new(*serviceerror.Internal))
				} else {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				}
				protorequire.ProtoEqual(t, beforeState, a.ActivityState)
				protorequire.ProtoEqual(t, beforeAttempt, a.LastAttempt.Get(ctx))
				require.Empty(t, ctx.Tasks)
				return
			}

			require.NoError(t, err)
			attempt := a.LastAttempt.Get(ctx)
			require.Equal(t, beforeAttempt.GetCount(), attempt.GetCount())
			require.Equal(t, beforeAttempt.GetStartedStamp(), attempt.GetStartedStamp())
			require.Equal(t, testCase.expectedHandoffCount, attempt.GetAsyncHandoffState().GetCount())
			if testCase.expectedStartedAt.IsZero() {
				require.Nil(t, attempt.GetAsyncHandoffState().GetStartToCloseStartedTime())
			} else {
				require.Equal(t, testCase.expectedStartedAt, attempt.GetAsyncHandoffState().GetStartToCloseStartedTime().AsTime())
			}

			if !a.hasAttemptInProgress() {
				for _, task := range ctx.Tasks {
					switch task.Payload.(type) {
					case *activitypb.StartToHandoffTimeoutTask, *activitypb.HandoffToCloseTimeoutTask, *activitypb.HeartbeatTimeoutTask:
						t.Fatalf("inactive delivery issued %T", task.Payload)
					default:
					}
				}
			}

			require.Equal(t, testCase.expectedCompletion, a.GetAwaitingCompletion())
			protorequire.ProtoEqual(t, beforeState.GetAsyncHandoffTime(), a.GetAsyncHandoffTime())
			protorequire.ProtoEqual(t, beforeState.GetOriginalOptions(), a.GetOriginalOptions())

			if !testCase.expectedStartToCloseTimer.IsZero() {
				var startToClose, heartbeat time.Time
				for _, task := range ctx.Tasks {
					switch task.Payload.(type) {
					case *activitypb.StartToCloseTimeoutTask:
						startToClose = task.Attributes.ScheduledTime
					case *activitypb.HeartbeatTimeoutTask:
						heartbeat = task.Attributes.ScheduledTime
					default:
					}
				}
				require.Equal(t, testCase.expectedStartToCloseTimer, startToClose)
				require.Equal(t, testCase.expectedHeartbeatTimer, heartbeat)
			}

			if testCase.name == "add after reporting-only handoff" {
				require.Len(t, ctx.Tasks, 1)
				require.IsType(t, &activitypb.HandoffToCloseTimeoutTask{}, ctx.Tasks[0].Payload)
				require.Equal(t, defaultTime.Add(130*time.Second), ctx.Tasks[0].Attributes.ScheduledTime)
			}
		})
	}
}

func TestHandoffRetryOptionsUpdate(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name               string
		removeAndReadd     bool
		editAtReadd        bool
		removeOnly         bool
		mainPolicy         bool
		legacyRetry        bool
		wholeOptions       bool
		preserveUnlimited  bool
		restoreOriginal    bool
		originalHasHandoff bool
		source             activitypb.ActivityRetryIntervalSource
		expectedInterval   time.Duration
	}{
		// Upcoming handoff 4 means failed handoff 3: 20s * 3^(3-1) = 180s, below the one-hour cap.
		{name: "governing handoff policy", preserveUnlimited: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 3 * time.Minute},
		{name: "worker override is preserved", source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE, expectedInterval: 40 * time.Second},
		{name: "unknown interval source is preserved", expectedInterval: 40 * time.Second},
		{name: "legacy main provenance uses upcoming count", mainPolicy: true, legacyRetry: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 20 * time.Second},
		{name: "defaulted whole options preserve pending delay", wholeOptions: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 40 * time.Second},
		{name: "re-added policy uses new sequence", removeAndReadd: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 20 * time.Second},
		{name: "policy supplied while re-adding uses new sequence", removeAndReadd: true, editAtReadd: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 20 * time.Second},
		{name: "removed handoff policy never uses edited main policy", removeOnly: true, mainPolicy: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 40 * time.Second},
		{name: "restored handoff policy recalculates pending delay", restoreOriginal: true, originalHasHandoff: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 3 * time.Minute},
		{name: "restore without handoff options preserves pending retry", restoreOriginal: true, source: activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, expectedInterval: 40 * time.Second},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			config := activityContextFromChasm(ctx).config
			config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			config.DefaultActivityRetryPolicy = dynamicconfig.GetTypedPropertyFnFilteredByNamespace(retrypolicy.DefaultDefaultRetrySettings)
			if testCase.preserveUnlimited {
				a.AsyncHandoffOptions.RetryPolicy.MaximumAttempts = 0
				defaults := retrypolicy.DefaultDefaultRetrySettings
				defaults.MaximumAttempts = 99
				config.DefaultActivityRetryPolicy = dynamicconfig.GetTypedPropertyFnFilteredByNamespace(defaults)
			}

			a.AsyncHandoffOptions.RetryPolicy.MaximumInterval = durationpb.New(time.Hour)
			a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
			attempt := a.LastAttempt.Get(ctx)
			attempt.AsyncHandoffState.Count = 4
			attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_HANDOFF
			attempt.CurrentRetryInterval = durationpb.New(40 * time.Second)
			attempt.CurrentRetryIntervalSource = testCase.source
			attempt.CompleteTime = timestamppb.New(defaultTime.Add(10 * time.Second))
			attempt.DispatchTime = timestamppb.New(defaultTime.Add(50 * time.Second))
			if testCase.legacyRetry {
				attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
			}

			update := func(options *apiactivitypb.ActivityOptions, path string) {
				t.Helper()
				_, err := a.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
					ActivityId: "activity", ActivityOptions: options, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}},
				}})
				require.NoError(t, err)
			}

			if testCase.removeAndReadd || testCase.removeOnly {
				update(&apiactivitypb.ActivityOptions{}, "async_handoff_options")
				require.Equal(t, activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, attempt.GetPendingRetryType())
				require.Equal(t, 40*time.Second, attempt.GetCurrentRetryInterval().AsDuration())
			}

			if testCase.removeAndReadd && !testCase.editAtReadd {
				update(&apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}}, "async_handoff_options")
				require.EqualValues(t, 1, attempt.GetAsyncHandoffState().GetCount())
				require.Equal(t, 40*time.Second, attempt.GetCurrentRetryInterval().AsDuration())
			}

			updatedInitialInterval := 20 * time.Second
			if testCase.mainPolicy {
				// An accidental main-policy recalculation must differ from the existing 40-second delay.
				updatedInitialInterval = 10 * time.Second
			}

			policy := &commonpb.RetryPolicy{InitialInterval: durationpb.New(updatedInitialInterval)}
			switch {
			case testCase.restoreOriginal:
				a.OriginalOptions = &apiactivitypb.ActivityOptions{
					TaskQueue:              common.CloneProto(a.GetTaskQueue()),
					ScheduleToCloseTimeout: common.CloneProto(a.GetScheduleToCloseTimeout()),
					ScheduleToStartTimeout: common.CloneProto(a.GetScheduleToStartTimeout()),
					StartToCloseTimeout:    common.CloneProto(a.GetStartToCloseTimeout()),
					RetryPolicy:            common.CloneProto(a.GetRetryPolicy()),
					AsyncHandoffOptions:    common.CloneProto(a.GetAsyncHandoffOptions()),
				}

				// Using the restored activity policy must produce a different delay from the pending handoff retry.
				a.OriginalOptions.RetryPolicy.InitialInterval = durationpb.New(time.Minute)
				if testCase.originalHasHandoff {
					a.OriginalOptions.AsyncHandoffOptions.RetryPolicy.InitialInterval = policy.InitialInterval
				} else {
					a.OriginalOptions.AsyncHandoffOptions = nil
				}

				completedAt := common.CloneProto(attempt.GetCompleteTime())
				ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime.Add(20 * time.Second) }
				_, err := a.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
					FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{ActivityId: "activity", RestoreOriginal: true},
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, a.GetOriginalOptions().GetAsyncHandoffOptions(), a.GetAsyncHandoffOptions())
				require.Equal(t, activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, attempt.GetPendingRetryType())
				protorequire.ProtoEqual(t, completedAt, attempt.GetCompleteTime())
			case testCase.editAtReadd:
				update(&apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{RetryPolicy: policy}}, "async_handoff_options")
			case testCase.mainPolicy:
				update(&apiactivitypb.ActivityOptions{RetryPolicy: policy}, "retry_policy.initial_interval")
			case testCase.wholeOptions:
				update(&apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}}, "async_handoff_options")
			default:
				update(&apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{RetryPolicy: policy}}, "async_handoff_options.retry_policy.initial_interval")
			}

			if testCase.preserveUnlimited {
				require.Zero(t, a.GetAsyncHandoffOptions().GetRetryPolicy().GetMaximumAttempts(), "a leaf edit must preserve unselected unlimited retries")
			}

			require.Equal(t, testCase.expectedInterval, attempt.GetCurrentRetryInterval().AsDuration())
			require.Equal(t, attempt.GetCompleteTime().AsTime().Add(testCase.expectedInterval), attempt.GetDispatchTime().AsTime())
			require.EqualValues(t, 3, attempt.GetCount())
			require.EqualValues(t, 7, attempt.GetStartedStamp())
			if testCase.removeAndReadd {
				require.EqualValues(t, 1, attempt.GetAsyncHandoffState().GetCount())
				require.Nil(t, attempt.GetAsyncHandoffState().GetStartToCloseStartedTime())
			}
		})
	}
}

func TestHandoffOptionsRestorationAdmission(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                                 string
		update, enabled, waiting, keepPaused bool
	}{
		{name: "update", update: true},
		{name: "reset"},
		{name: "accepted deferred reset", enabled: true},
		{name: "immediate queued reset", enabled: true, waiting: true},
		{name: "reset while paused", enabled: true, waiting: true, keepPaused: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			a.OriginalOptions = &apiactivitypb.ActivityOptions{
				TaskQueue:              common.CloneProto(a.GetTaskQueue()),
				ScheduleToCloseTimeout: common.CloneProto(a.GetScheduleToCloseTimeout()),
				StartToCloseTimeout:    common.CloneProto(a.GetStartToCloseTimeout()),
				RetryPolicy:            common.CloneProto(a.GetRetryPolicy()),
				AsyncHandoffOptions:    common.CloneProto(a.GetAsyncHandoffOptions()),
			}
			a.AsyncHandoffOptions = nil
			a.LastAttempt.Get(ctx).AsyncHandoffState = nil
			config := activityContextFromChasm(ctx).config
			config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(testCase.enabled)
			if testCase.waiting {
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
				if testCase.keepPaused {
					a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
				}
			}
			before := common.CloneProto(a.ActivityState)

			var err error
			if testCase.update {
				_, err = a.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{ActivityId: "activity", RestoreOriginal: true}})
			} else {
				_, err = a.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{FrontendRequest: &workflowservice.ResetActivityExecutionRequest{ActivityId: "activity", RestoreOriginalOptions: true, KeepPaused: testCase.keepPaused}})
			}

			if !testCase.enabled {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				protorequire.ProtoEqual(t, before, a.ActivityState)
				require.Empty(t, ctx.Tasks)
				return
			}

			require.NoError(t, err)
			if !testCase.waiting {
				require.True(t, a.GetResetRestoreOptions())
				config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
				_, err = a.tryReschedule(ctx, retryEvent{failure: &failurepb.Failure{Message: "worker yielded"}})
				require.NoError(t, err)
			}

			expectedStatus := activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
			if testCase.keepPaused {
				expectedStatus = activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
			}
			require.Equal(t, expectedStatus, a.GetStatus())
			require.False(t, a.GetResetRestoreOptions())
			require.NotNil(t, a.GetAsyncHandoffOptions())
			require.EqualValues(t, 1, a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetCount())
			require.Nil(t, a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetStartToCloseStartedTime())
		})
	}

	// A successful opt-in replay returns current options even after a reset removed them.
	t.Run("OptInReplayAfterResetAndDisablement", func(t *testing.T) {
		t.Parallel()

		a, ctx := newHandoffRetryTestActivity(t)
		config := activityContextFromChasm(ctx).config
		config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
		config.DefaultActivityRetryPolicy = dynamicconfig.GetTypedPropertyFnFilteredByNamespace(retrypolicy.DefaultDefaultRetrySettings)
		a.AsyncHandoffOptions = nil
		a.LastAttempt.Get(ctx).AsyncHandoffState = nil
		a.OriginalOptions = &apiactivitypb.ActivityOptions{
			TaskQueue:           common.CloneProto(a.GetTaskQueue()),
			StartToCloseTimeout: common.CloneProto(a.GetStartToCloseTimeout()),
			RetryPolicy:         common.CloneProto(a.GetRetryPolicy()),
		}
		request := &activitypb.UpdateActivityExecutionOptionsRequest{FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId: "activity", RequestId: "accepted-opt-in",
			ActivityOptions: &apiactivitypb.ActivityOptions{AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{}},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"async_handoff_options"}},
		}}
		_, err := a.UpdateActivityExecutionOptions(ctx, request)
		require.NoError(t, err)

		_, err = a.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{FrontendRequest: &workflowservice.ResetActivityExecutionRequest{ActivityId: "activity", RestoreOriginalOptions: true}})
		require.NoError(t, err)
		_, err = a.tryReschedule(ctx, retryEvent{failure: &failurepb.Failure{Message: "worker yielded"}})
		require.NoError(t, err)
		require.Nil(t, a.GetAsyncHandoffOptions())
		config.StandaloneAsyncHandoffEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
		before := common.CloneProto(a.ActivityState)
		beforeAttempt := common.CloneProto(a.LastAttempt.Get(ctx))
		ctx.Tasks = nil

		response, err := a.UpdateActivityExecutionOptions(ctx, request)
		require.NoError(t, err)
		require.Nil(t, response.GetFrontendResponse().GetActivityOptions().GetAsyncHandoffOptions())
		protorequire.ProtoEqual(t, before, a.ActivityState)
		protorequire.ProtoEqual(t, beforeAttempt, a.LastAttempt.Get(ctx))
		require.Empty(t, ctx.Tasks)
	})
}
