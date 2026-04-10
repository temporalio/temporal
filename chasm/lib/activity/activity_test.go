package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHandleStarted(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	testRequestID := "test-request-id"
	testStamp := int32(1)

	testCases := []struct {
		name           string
		activityStatus activitypb.ActivityExecutionStatus
		attemptStamp   int32
		requestStamp   int32
		startRequestID string
		requestID      string
		checkOutcome   func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error)
	}{
		{
			name:           "successful transition from scheduled",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.Equal(t, int32(1), response.Attempt)
				require.NoError(t, err)
			},
		},
		{
			name:           "idempotent retry - same request ID",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			startRequestID: testRequestID,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.Equal(t, int32(1), response.Attempt)
				require.NoError(t, err)
			},
		},
		{
			name:           "error - already started with different request ID",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			startRequestID: "different-request-id",
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
		{
			name:           "error - stamp mismatch",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp + 1,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
		{
			name:           "error - invalid transition from completed",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock context
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return testTime },
					HandleExecutionKey: func() chasm.ExecutionKey {
						return chasm.ExecutionKey{
							BusinessID: "test-activity-id",
							RunID:      "test-run-id",
						}
					},
				},
			}

			// Setup activity state
			attemptState := &activitypb.ActivityAttemptState{
				Count:          1,
				Stamp:          tc.attemptStamp,
				StartRequestId: tc.startRequestID,
			}
			if tc.activityStatus == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED {
				attemptState.StartedTime = timestamppb.New(testTime.Add(-1 * time.Minute))
			}

			// Determine heartbeat timeout based on test case
			heartbeatTimeout := 1 * time.Minute
			if tc.name == "successful transition without heartbeat timeout" {
				heartbeatTimeout = 0
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					Status:                 tc.activityStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
					StartToCloseTimeout:    durationpb.New(3 * time.Minute),
					HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
					ScheduleTime:           timestamppb.New(testTime.Add(-30 * time.Second)),
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{{Data: []byte("test-input")}},
					},
					Header: &commonpb.Header{
						Fields: map[string]*commonpb.Payload{
							"test-header": {Data: []byte("test-value")},
						},
					},
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			// Create request
			request := &historyservice.RecordActivityTaskStartedRequest{
				Stamp:     tc.requestStamp,
				RequestId: tc.requestID,
			}

			// Execute HandleStarted
			response, err := activity.HandleStarted(ctx, request)

			tc.checkOutcome(t, response, err)
		})
	}
}

func TestActivityTerminate(t *testing.T) {
	testCases := []struct {
		name           string
		activityStatus activitypb.ActivityExecutionStatus
		expectErr      string
	}{
		{
			name:           "terminate scheduled activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:           "terminate started activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			name:           "terminate cancel-requested activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "error on completed activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			expectErr:      "invalid transition from Completed",
		},
		{
			name:           "no-op on already terminated activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "error on failed activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
			expectErr:      "invalid transition from Failed",
		},
		{
			name:           "error on timed out activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			expectErr:      "invalid transition from TimedOut",
		},
		{
			name:           "error on canceled activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
			expectErr:      "invalid transition from Canceled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-namespace"), nil).AnyTimes()

			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
						},
						namespaceRegistry: nsRegistry,
					}),
				},
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					Status:                 tc.activityStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
					StartToCloseTimeout:    durationpb.New(3 * time.Minute),
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
				Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			_, err := activity.Terminate(ctx, chasm.TerminateComponentRequest{
				Reason: "Delete activity execution",
			})

			if tc.expectErr != "" {
				require.EqualError(t, err, tc.expectErr)
				require.Equal(t, tc.activityStatus, activity.Status, "expected no state change on error")
			} else {
				require.NoError(t, err)
				require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
			}
		})
	}
}

func TestApplyActivityOptionsAcceptance(t *testing.T) {
	updateOptions := &apiactivitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

	testCases := []struct {
		name       string
		updateOpts *apiactivitypb.ActivityOptions
		expected   *apiactivitypb.ActivityOptions
		mask       *fieldmaskpb.FieldMask
	}{
		{
			name:       "Top-level fields with CamelCase",
			updateOpts: updateOptions,
			expected:   updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"Priority",
					"RetryPolicy",
				},
			},
		},
		{
			name:       "Top-level fields with snake_case",
			updateOpts: updateOptions,
			expected:   updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"start_to_close_timeout",
					"heartbeat_timeout",
					"priority",
					"retry_policy",
				},
			},
		},
		{
			name: "Sub-fields",
			updateOpts: &apiactivitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			expected: &apiactivitypb.ActivityOptions{
				// Timeouts unchanged from the initial Activity state (all set to 30 minutes below).
				ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
				ScheduleToStartTimeout: durationpb.New(30 * time.Minute),
				StartToCloseTimeout:    durationpb.New(30 * time.Minute),
				HeartbeatTimeout:       durationpb.New(30 * time.Minute),
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"priority.priority_key",
					"priority.fairness_key",
					"priority.fairness_weight",
					"retry_policy.backoff_coefficient",
					"retry_policy.initial_interval",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(30 * time.Minute),
					StartToCloseTimeout:    durationpb.New(30 * time.Minute),
					HeartbeatTimeout:       durationpb.New(30 * time.Minute),
					Priority: &commonpb.Priority{
						PriorityKey:    10,
						FairnessKey:    "oldKey",
						FairnessWeight: 1.0,
					},
					RetryPolicy: &commonpb.RetryPolicy{},
				},
			}

			req := &workflowservice.UpdateActivityExecutionOptionsRequest{
				ActivityId:      "test-activity-id",
				ActivityOptions: tc.updateOpts,
				UpdateMask:      tc.mask,
			}

			err := activity.mergeActivityOptions(req)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected.RetryPolicy.GetInitialInterval(), activity.RetryPolicy.GetInitialInterval(), "RetryInitialInterval")
			assert.Equal(t, tc.expected.RetryPolicy.GetMaximumInterval(), activity.RetryPolicy.GetMaximumInterval(), "RetryMaximumInterval")
			assert.Equal(t, tc.expected.RetryPolicy.GetBackoffCoefficient(), activity.RetryPolicy.GetBackoffCoefficient(), "RetryBackoffCoefficient")
			assert.Equal(t, tc.expected.RetryPolicy.GetMaximumAttempts(), activity.RetryPolicy.GetMaximumAttempts(), "RetryMaximumAttempts")
			assert.Equal(t, tc.expected.TaskQueue, activity.TaskQueue, "TaskQueue")
			assert.Equal(t, tc.expected.ScheduleToCloseTimeout, activity.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
			assert.Equal(t, tc.expected.ScheduleToStartTimeout, activity.ScheduleToStartTimeout, "ScheduleToStartTimeout")
			assert.Equal(t, tc.expected.StartToCloseTimeout, activity.StartToCloseTimeout, "StartToCloseTimeout")
			assert.Equal(t, tc.expected.HeartbeatTimeout, activity.HeartbeatTimeout, "HeartbeatTimeout")
			assert.Equal(t, tc.expected.Priority, activity.Priority, "Priority")
		})
	}
}

func TestApplyActivityOptionsErrors(t *testing.T) {
	makeActivity := func() *Activity {
		return &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
			},
		}
	}
	makeReq := func(opts *apiactivitypb.ActivityOptions, paths ...string) *workflowservice.UpdateActivityExecutionOptionsRequest {
		return &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId:      "test-id",
			ActivityOptions: opts,
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: paths},
		}
	}
	emptyOpts := &apiactivitypb.ActivityOptions{}

	var err error
	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "retry_policy.maximum_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "retry_policy.maximum_attempts"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "retry_policy.backoff_coefficient"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "retry_policy.initial_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "taskQueue.name"))
	require.ErrorContains(t, err, "TaskQueue is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "priority.priority_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "priority.fairness_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = makeActivity().mergeActivityOptions(makeReq(emptyOpts, "priority.fairness_weight"))
	require.ErrorContains(t, err, "Priority is not provided")
}
