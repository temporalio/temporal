package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.EqualError(t, err, "activity is in terminal state Canceled")
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
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.EqualError(t, err, "already terminated with request ID "+requestID)
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
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.EqualError(t, err, "activity is in non-pausable state Terminated")
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

	err := activity.mergeActivityOptions(&workflowservice.UpdateActivityExecutionOptionsRequest{
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
			)

			require.Equal(t, tc.expectRecalculate, got)
		})
	}
}

// TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions mimics an
// activity persisted before original_options existed (e.g. created by a binary predating this
// field's introduction). Restoring such an activity's options has nothing valid to fall back to
// and must be rejected rather than silently wiping TaskQueue and both close/start timeouts.
func TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions(t *testing.T) {
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

	_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
		FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId:      "act",
			RestoreOriginal: true,
			RequestId:       "failed-update-request-id",
		},
	})

	require.Error(t, err)
	require.Empty(t, activity.GetLastUpdateOptionsRequestId())
	require.Equal(t, "current-task-queue", activity.GetTaskQueue().GetName())
	require.Equal(t, 30*time.Second, activity.GetScheduleToCloseTimeout().AsDuration())
	require.Equal(t, 10*time.Second, activity.GetStartToCloseTimeout().AsDuration())
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
