package tests

import (
	"context"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/util"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/tests/testcore"
)

type PollerScalingIntegSuite struct {
	testcore.FunctionalTestSuite
	sdkClient sdkclient.Client
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PollerScalingIntegSuite))
}

func (s *PollerScalingIntegSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Force one partition so we can reliably see the backlog
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():     1,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key():    1,
		dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp.Key(): 50 * time.Millisecond,
	}
	s.FunctionalTestBase.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PollerScalingIntegSuite) SetupTest() {
	s.FunctionalTestSuite.SetupTest()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
}

func (s *PollerScalingIntegSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *PollerScalingIntegSuite) TestPollerScalingSimpleBacklog() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	tq := testcore.RandomizeStr(s.T().Name())

	// Queue up a couple workflows
	runHandles := make([]sdkclient.WorkflowRun, 0)
	for i := 0; i < 5; i++ {
		run, err := s.sdkClient.ExecuteWorkflow(
			ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
		runHandles = append(runHandles, run)
	}
	// Wait for the backlog age to pass the config threshold
	time.Sleep(50 * time.Millisecond)

	// Poll for a tasks and see attached decision is to scale up b/c of backlog
	feClient := s.FrontendClient()
	resp, err := feClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(resp.PollerScalingDecision)
	s.Assert().GreaterOrEqual(int32(1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)
}

// Here we verify that, even with multiple partitions, pollers see scaling decisions at least often enough
// that SDKs have information to work with. Benchmark style testing that exists on the SDK side is better at ensuring
// that the desired outcomes actually happen.
func (s *PollerScalingIntegSuite) TestPollerScalingDecisionsAreSeenProbabilistically() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 5)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 5)

	longctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	ctx, startWfCancel := context.WithCancel(longctx)
	tq := testcore.RandomizeStr(s.T().Name())

	// Fire off workflows until polling stops
	go func() {
		for {
			_, _ = s.sdkClient.ExecuteWorkflow(
				longctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}
	}()

	allScaleDecisions := make([]*taskqueuepb.PollerScalingDecision, 0, 15)
	for i := 0; i < 15; i++ {
		resp, _ := s.FrontendClient().PollWorkflowTaskQueue(longctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		if resp != nil {
			allScaleDecisions = append(allScaleDecisions, resp.PollerScalingDecision)
		}
		// Poll slightly less frequently than we insert tasks
		time.Sleep(300 * time.Millisecond)
	}
	startWfCancel()

	// We must have seen at least a handful of non-nil scaling decisions
	nonNilDecisions := util.FilterSlice(allScaleDecisions, func(d *taskqueuepb.PollerScalingDecision) bool { return d != nil })
	s.Assert().GreaterOrEqual(len(nonNilDecisions), 3)
}
