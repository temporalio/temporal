package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/testing/protorequire"
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
