package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildActivityExecutionInfo_IncludeLastDeploymentVersion(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			LastDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: "test-deployment",
				BuildId:        "test-build-1",
			},
		}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{}),
		Visibility:  chasm.NewComponentField(ctx, &chasm.Visibility{}),
	}

	resp, err := activity.buildDescribeActivityExecutionResponse(ctx, &activitypb.DescribeActivityExecutionRequest{
		FrontendRequest: &workflowservice.DescribeActivityExecutionRequest{},
	})

	require.NoError(t, err)
	protorequire.ProtoEqual(t, &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: "test-deployment",
		BuildId:        "test-build-1",
	}, resp.FrontendResponse.GetInfo().GetLastDeploymentVersion())
}

func TestBuildActivityExecutionInfo_EagerExecutionAccepted(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	for _, test := range []struct {
		name     string
		accepted bool
	}{
		{name: "accepted", accepted: true},
		{name: "not accepted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
					EagerExecutionAccepted: test.accepted,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{}),
				Visibility:  chasm.NewComponentField(ctx, &chasm.Visibility{}),
			}

			info := activity.buildActivityExecutionInfo(ctx, &workflowservice.DescribeActivityExecutionRequest{})
			require.Equal(t, test.accepted, info.GetEagerExecutionAccepted())
		})
	}
}

func TestEagerActivityTaskDataSnapshotsState(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{BusinessID: "activity-id", RunID: "run-id"}
			},
		},
	}

	activityType := &commonpb.ActivityType{Name: "activity-type"}
	header := &commonpb.Header{Fields: map[string]*commonpb.Payload{"header": {Data: []byte("header")}}}
	input := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("input")}}}
	heartbeatDetails := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("heartbeat")}}}
	scheduledTime := timestamppb.New(time.Unix(10, 0))
	dispatchTime := timestamppb.New(time.Unix(11, 0))
	startedTime := timestamppb.New(time.Unix(12, 0))
	scheduleToCloseTimeout := durationpb.New(time.Minute)
	startToCloseTimeout := durationpb.New(2 * time.Minute)
	heartbeatTimeout := durationpb.New(3 * time.Minute)
	retryPolicy := &commonpb.RetryPolicy{MaximumAttempts: 2}
	priority := &commonpb.Priority{PriorityKey: 1}
	propagatedNexusSerializationContext := &nexuspb.PropagatedSerializationContext{Endpoint: "endpoint", Service: "service", Operation: "operation"}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           activityType,
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			ScheduleTime:           scheduledTime,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			StartToCloseTimeout:    startToCloseTimeout,
			HeartbeatTimeout:       heartbeatTimeout,
			RetryPolicy:            retryPolicy,
			Priority:               priority,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:          1,
			StartRequestId: "request-id",
			StartedTime:    startedTime,
			DispatchTime:   dispatchTime,
		}),
		LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{Details: heartbeatDetails}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Header:                              header,
			Input:                               input,
			PropagatedNexusSerializationContext: propagatedNexusSerializationContext,
		}),
	}

	data, err := activity.eagerActivityTaskData(ctx, eagerActivityTaskRequest{
		namespaceID: "namespace-id",
		namespace:   "namespace",
		requestID:   "request-id",
	})
	require.NoError(t, err)
	require.NotNil(t, data)

	activityType.Name = "changed-activity-type"
	header.Fields["header"].Data = []byte("changed-header")
	input.Payloads[0].Data = []byte("changed-input")
	heartbeatDetails.Payloads[0].Data = []byte("changed-heartbeat")
	scheduledTime.Seconds++
	dispatchTime.Seconds++
	startedTime.Seconds++
	scheduleToCloseTimeout.Seconds++
	startToCloseTimeout.Seconds++
	heartbeatTimeout.Seconds++
	retryPolicy.MaximumAttempts++
	priority.PriorityKey++
	propagatedNexusSerializationContext.Endpoint = "changed-endpoint"

	response, err := data.response([]byte("component-ref"))
	require.NoError(t, err)
	require.Equal(t, "activity-type", response.GetActivityType().GetName())
	require.Equal(t, []byte("header"), response.GetHeader().GetFields()["header"].GetData())
	require.Equal(t, []byte("input"), response.GetInput().GetPayloads()[0].GetData())
	require.Equal(t, []byte("heartbeat"), response.GetHeartbeatDetails().GetPayloads()[0].GetData())
	require.Equal(t, int64(10), response.GetScheduledTime().GetSeconds())
	require.Equal(t, int64(11), response.GetCurrentAttemptScheduledTime().GetSeconds())
	require.Equal(t, int64(12), response.GetStartedTime().GetSeconds())
	require.Equal(t, int64(time.Minute/time.Second), response.GetScheduleToCloseTimeout().GetSeconds())
	require.Equal(t, int64(2*time.Minute/time.Second), response.GetStartToCloseTimeout().GetSeconds())
	require.Equal(t, int64(3*time.Minute/time.Second), response.GetHeartbeatTimeout().GetSeconds())
	require.EqualValues(t, 2, response.GetRetryPolicy().GetMaximumAttempts())
	require.EqualValues(t, 1, response.GetPriority().GetPriorityKey())
	require.Equal(t, "endpoint", response.GetPropagatedNexusSerializationContext().GetEndpoint())
}

func TestBuildActivityExecutionInfoHandoff(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                 string
		status               activitypb.ActivityExecutionStatus
		keepPaused, awaiting bool
		options              *apiactivitypb.AsyncHandoffOptions
		wantState            enumspb.PendingActivityState
		wantAwaiting         bool
	}{
		{
			name:      "without handoff metadata",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			wantState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
		{
			name:      "before handoff",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			options:   &apiactivitypb.AsyncHandoffOptions{},
			wantState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
		{
			name:         "awaiting external completion",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			awaiting:     true,
			wantState:    enumspb.PENDING_ACTIVITY_STATE_AWAITING_COMPLETION,
			wantAwaiting: true,
			options: &apiactivitypb.AsyncHandoffOptions{
				StartToHandoffTimeout: durationpb.New(time.Minute),
				HandoffToCloseTimeout: durationpb.New(time.Hour),
				HeartbeatTimeout:      durationpb.New(30 * time.Second),
				RetryPolicy:           &commonpb.RetryPolicy{MaximumAttempts: 5},
			},
		},
		{
			name:         "handoff without options",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			awaiting:     true,
			wantState:    enumspb.PENDING_ACTIVITY_STATE_AWAITING_COMPLETION,
			wantAwaiting: true,
		},
		{
			name:      "pending reset before handoff",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			options:   &apiactivitypb.AsyncHandoffOptions{},
			wantState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
		{
			name:         "pause request takes precedence",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			awaiting:     true,
			options:      &apiactivitypb.AsyncHandoffOptions{},
			wantState:    enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
			wantAwaiting: true,
		},
		{
			name:         "cancellation request takes precedence",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			awaiting:     true,
			options:      &apiactivitypb.AsyncHandoffOptions{},
			wantState:    enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
			wantAwaiting: true,
		},
		{
			name:         "pending reset retains phase",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			awaiting:     true,
			options:      &apiactivitypb.AsyncHandoffOptions{},
			wantState:    enumspb.PENDING_ACTIVITY_STATE_AWAITING_COMPLETION,
			wantAwaiting: true,
		},
		{
			name:         "keep-paused reset takes precedence",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			keepPaused:   true,
			awaiting:     true,
			options:      &apiactivitypb.AsyncHandoffOptions{},
			wantState:    enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
			wantAwaiting: true,
		},
		{
			name:      "queued retry retains historical handoff",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			awaiting:  true,
			options:   &apiactivitypb.AsyncHandoffOptions{},
			wantState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			name:      "fully paused activity has no active phase",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			awaiting:  true,
			options:   &apiactivitypb.AsyncHandoffOptions{},
			wantState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
		},
		{
			name:      "closed activity retains only historical handoff",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			awaiting:  true,
			options:   &apiactivitypb.AsyncHandoffOptions{},
			wantState: enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
					HandleExecutionInfo: func() chasm.ExecutionInfo {
						return chasm.ExecutionInfo{CloseTime: defaultTime.Add(time.Minute)}
					},
				},
			}

			var handoffTime *timestamppb.Timestamp
			if tc.awaiting {
				handoffTime = timestamppb.New(defaultTime)
			}
			attempt := &activitypb.ActivityAttemptState{Count: 3}
			if tc.options != nil || tc.awaiting {
				attempt.AsyncHandoffState = &activitypb.ActivityAttemptState_AsyncHandoffState{Count: 2}
			}

			a := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:              tc.status,
					ResetShouldPause:    tc.keepPaused,
					AwaitingCompletion:  tc.awaiting,
					AsyncHandoffTime:    handoffTime,
					AsyncHandoffOptions: tc.options,
					ScheduleTime:        timestamppb.New(defaultTime),
				},
				LastAttempt: chasm.NewDataField(ctx, attempt),
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{}),
				Visibility:  chasm.NewComponentField(ctx, &chasm.Visibility{}),
			}

			info := a.buildActivityExecutionInfo(ctx, &workflowservice.DescribeActivityExecutionRequest{})
			wantCount := int32(0)
			if tc.options != nil {
				wantCount = 2
			}
			require.Equal(t, tc.wantState, info.GetRunState())
			require.Equal(t, tc.wantAwaiting, info.GetAwaitingCompletion())
			require.EqualValues(t, 3, info.GetAttempt())
			require.Equal(t, wantCount, info.GetHandoffAttempt())
			protorequire.ProtoEqual(t, tc.options, info.GetAsyncHandoffOptions())
			protorequire.ProtoEqual(t, handoffTime, info.GetAsyncHandoffTime())

			if tc.options.GetHeartbeatTimeout() != nil {
				info.AsyncHandoffOptions.HeartbeatTimeout.Seconds++
				protorequire.ProtoEqual(t, durationpb.New(30*time.Second), a.GetAsyncHandoffOptions().GetHeartbeatTimeout())
			}
		})
	}
}
