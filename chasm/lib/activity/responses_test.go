package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
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
			Header: header,
			Input:  input,
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
}
