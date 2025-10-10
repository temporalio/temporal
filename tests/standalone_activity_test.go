package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type standaloneActivityTestSuite struct {
	testcore.FunctionalTestBase
}

func TestStandaloneActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(standaloneActivityTestSuite))
}

func (s *standaloneActivityTestSuite) TestStartActivityExecution() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)

	activityId := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

	r, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		Identity:   "test-identity",
		ActivityId: activityId,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
	})
	require.NoError(t, err)

	resp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityId,
		RunId:      r.RunId,
	})
	require.NoError(t, err)

	fmt.Println(resp)

	pollResponse, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, pollResponse.GetActivityId())
}
