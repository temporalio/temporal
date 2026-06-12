package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
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

func TestContextMetadata(t *testing.T) {
	t.Run("returns activity type and task queue", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "my-activity"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "my-task-queue"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-type":       "my-activity",
			"standalone-activity-task-queue": "my-task-queue",
		}, md)
	})

	t.Run("returns only activity type when task queue is empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "my-activity"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-type": "my-activity",
		}, md)
	})

	t.Run("returns only task queue when activity type is empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "my-task-queue"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-task-queue": "my-task-queue",
		}, md)
	})

	t.Run("returns nil when both are empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{},
		}

		md := activity.ContextMetadata(ctx)
		require.Nil(t, md)
	})
}

// TestNewStandaloneActivity_UserMetadataDualWrite verifies that user metadata
// supplied on a StartActivityExecution request is persisted to BOTH the
// framework-level ChasmComponentAttributes (the authoritative new location)
// and the legacy ActivityRequestData.user_metadata field. The dual-write is
// load-bearing for rollback safety: a binary rolled back to pre-migration code
// only knows how to read the legacy field, so dropping it would silently empty
// the user metadata on Describe for any activity created during the new-deploy
// window.
func TestNewStandaloneActivity_UserMetadataDualWrite(t *testing.T) {
	md := &sdkpb.UserMetadata{
		Summary: &commonpb.Payload{Data: []byte("summary-blob")},
		Details: &commonpb.Payload{Data: []byte("details-blob")},
	}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "act", RunID: "run"}
			},
		},
	}

	activity, err := NewStandaloneActivity(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    "ns",
		ActivityId:   "act",
		ActivityType: &commonpb.ActivityType{Name: "T"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "Q"},
		RequestId:    "req-id",
		UserMetadata: md,
	})
	require.NoError(t, err)

	// New location: SetUserMetadata recorded against the activity.
	require.Contains(t, ctx.UserMetadataByComponent, chasm.Component(activity))
	require.Same(t, md, ctx.UserMetadataByComponent[activity])

	// Legacy location: ActivityRequestData also carries it so rolled-back code
	// can still surface user metadata via the old field.
	require.Same(t, md, activity.RequestData.Get(ctx).GetUserMetadata()) //nolint:staticcheck // exercising legacy field
}

// TestEffectiveUserMetadata_PrefersFrameworkLocation ensures the helper used by
// readers (Describe, etc.) returns the framework-level user metadata when both
// the new and legacy locations are populated.
func TestEffectiveUserMetadata_PrefersFrameworkLocation(t *testing.T) {
	frameworkMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("new")}}
	legacyMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("legacy")}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleUserMetadata: func(chasm.Component) *sdkpb.UserMetadata {
				return frameworkMD
			},
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{},
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			UserMetadata: legacyMD, //nolint:staticcheck // exercising legacy field
		}),
	}

	got := activity.effectiveUserMetadata(ctx)
	require.Same(t, frameworkMD, got)
}

// TestAttachLinks_SameRequestIDIsNoOp verifies that calling attachLinks with a
// requestID that already has links recorded is a no-op — the request is
// treated as an idempotent retry regardless of payload.
func TestAttachLinks_SameRequestIDIsNoOp(t *testing.T) {
	linkA := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}
	linkB := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "b", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleLinks: func(chasm.Component) []*commonpb.Link {
				var all []*commonpb.Link
				for _, ls := range stored {
					all = append(all, ls...)
				}
				return all
			},
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED},
	}

	// First call records the request's links verbatim (no intra-batch dedup, matching
	// the workflow start path).
	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{linkA, linkA}, "req-1", validator, "ns"))
	stored["req-1"] = ctx.LinksByRequest[activity]["req-1"]
	require.Equal(t, []*commonpb.Link{linkA, linkA}, stored["req-1"])

	// Retry with the same requestID (even with different links) is a no-op.
	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{linkB}, "req-1", validator, "ns"))
	require.Equal(t, []*commonpb.Link{linkA, linkA}, ctx.LinksByRequest[activity]["req-1"])
}

// TestAttachLinks_RejectsClosedActivity verifies that attachLinks returns a
// FailedPrecondition error when the activity is already in a terminal state and
// the requestID has not been seen before.
func TestAttachLinks_RejectsClosedActivity(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleRequestLinks: func(chasm.Component, string) ([]*commonpb.Link, error) {
				return nil, nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
	}

	err := activity.attachLinks(ctx, []*commonpb.Link{link}, "req-new", validator, "ns")
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

// TestAttachLinks_IdempotentAfterClose verifies that a retry with a previously
// seen requestID is a no-op even after the activity has closed — protecting
// against the case where the original attach succeeded but the response was
// lost before the client could observe it.
func TestAttachLinks_IdempotentAfterClose(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{"req-1": {link}}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
	}

	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{link}, "req-1", validator, "ns"))
}

// TestAttachLinks_RejectsWhenComponentCapExceeded verifies that an attach
// against a still-running activity is rejected with FailedPrecondition when the
// combined existing + incoming link count would exceed the per-component cap.
func TestAttachLinks_RejectsWhenComponentCapExceeded(t *testing.T) {
	existingLink := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "existing", RunId: "run"},
	}}
	newLink := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "new", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{"req-existing": {existingLink}}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleLinks: func(chasm.Component) []*commonpb.Link {
				var all []*commonpb.Link
				for _, ls := range stored {
					all = append(all, ls...)
				}
				return all
			},
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	// per-component cap = 1; activity already has 1 link, so any additional must fail.
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 1 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED},
	}

	err := activity.attachLinks(ctx, []*commonpb.Link{newLink}, "req-new", validator, "ns")
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

// TestEffectiveUserMetadata_FallsBackToLegacy ensures that activities persisted
// before the migration (no ChasmComponentAttributes.user_metadata; only the
// legacy ActivityRequestData.user_metadata is populated) still surface their
// user metadata to readers.
func TestEffectiveUserMetadata_FallsBackToLegacy(t *testing.T) {
	legacyMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("legacy")}}

	ctx := &chasm.MockMutableContext{} // HandleUserMetadata nil → returns nil, mimicking absent new field.
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{},
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			UserMetadata: legacyMD, //nolint:staticcheck // exercising legacy field
		}),
	}

	got := activity.effectiveUserMetadata(ctx)
	require.Same(t, legacyMD, got)
}
