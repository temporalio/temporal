package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
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

	resp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: testcore.RandomizeStr(t.Name()),
		ActivityType: &commonpb.ActivityType{
			Name: "TestActivity",
		},
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: s.TaskQueue(),
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRunId())
	require.True(t, resp.Started)
}
