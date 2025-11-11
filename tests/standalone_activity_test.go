package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
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

	activityID := testcore.RandomizeStr(t.Name())
	activityType := &commonpb.ActivityType{
		Name: "test-activity-type",
	}
	input := createDefaultInput()
	taskQueue := uuid.New().String()

	resp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: activityType,
		Input:        input,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRunId())
	require.True(t, resp.Started)

	pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test-identity",
	})

	require.NoError(t, err)
	require.Equal(t, activityID, pollResp.GetActivityId())
	require.True(t, proto.Equal(activityType, pollResp.GetActivityType()))
	require.EqualValues(t, 1, pollResp.Attempt)
	require.True(t, proto.Equal(input, pollResp.GetInput()))
}

func (s *standaloneActivityTestSuite) TestScheduleToStartShouldTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func (s *standaloneActivityTestSuite) TestScheduleToCloseShouldTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func (s *standaloneActivityTestSuite) TestStartToCloseShouldTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func createDefaultInput() *commonpb.Payloads {
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte(`{"name":"test-user","count":11}`),
			},
		},
	}
}
