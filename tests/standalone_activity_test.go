package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
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
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)

	activityId := testcore.RandomizeStr(t.Name())

	r, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		Identity:   s.GetNamespaceID(s.Namespace().String()),
		ActivityId: activityId,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: s.TaskQueue(),
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
	})
	require.NoError(t, err)

	resp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &activitypb.ActivityExecution{
			ActivityId: activityId,
			RunId:      fmt.Sprintf("%s|%s", r.RunId, s.GetNamespaceID(s.Namespace().String())),
		},
	})
	require.NoError(t, err)

	fmt.Println(resp)
}
