package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type TaskQueueSuite struct {
	testcore.FunctionalTestBase
}

func TestTaskQueueSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TaskQueueSuite))
}

func (s *TaskQueueSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 4,
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  4,
	}
	s.FunctionalTestBase.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// Not using RunTestWithMatchingBehavior because I want to pass different expected drain times for different configurations
func (s *TaskQueueSuite) TestTaskQueueRateLimit() {
	s.RunTaskQueueRateLimitTest(1, 1, 12*time.Second, true)  // ~0.75s avg
	s.RunTaskQueueRateLimitTest(1, 1, 12*time.Second, false) // ~1.1s avg

	// Testing multiple partitions with insufficient pollers is too flaky, because token recycling
	// depends on a process being available to accept the token, so I'm not testing it
	s.RunTaskQueueRateLimitTest(4, 8, 24*time.Second, true)  // ~1.6s avg
	s.RunTaskQueueRateLimitTest(4, 8, 24*time.Second, false) // ~6s avg
}

func (s *TaskQueueSuite) RunTaskQueueRateLimitTest(nPartitions, nWorkers int, timeToDrain time.Duration, useNewMatching bool) {
	s.Run(s.testTaskQueueRateLimitName(nPartitions, nWorkers, useNewMatching), func() { s.taskQueueRateLimitTest(nPartitions, nWorkers, timeToDrain, useNewMatching) })
}

func (s *TaskQueueSuite) taskQueueRateLimitTest(nPartitions, nWorkers int, timeToDrain time.Duration, useNewMatching bool) {
	if useNewMatching {
		s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)
	}
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, nPartitions)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, nPartitions)

	// exclude the effect of the default forwarding rate limit (10)
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxRatePerSecond, 1000)

	// 30 tasks at 1 task per second is 30 seconds.
	// if invalid tasks are NOT using the rate limit, then this should take well below that long.
	// task forwarding between task queue partitions is rate-limited by default to 10 rps.
	s.OverrideDynamicConfig(dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, 1)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0)

	const maxBacklog = 30
	tv := testvars.New(s.T())

	helloRateLimitTest := func(ctx workflow.Context, name string) (string, error) {
		return "Hello " + name + " !", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// start workflows to create a backlog
	for wfidx := 0; wfidx < maxBacklog; wfidx++ {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf%d", wfidx),
		}, helloRateLimitTest, "Donna")
		s.NoError(err)
	}

	// wait for backlog to be >= maxBacklog
	wfBacklogCount := int64(0)
	s.Eventually(
		func() bool {
			wfBacklogCount = s.getBacklogCount(ctx, tv)
			return wfBacklogCount >= maxBacklog
		},
		5*time.Second,
		200*time.Millisecond,
	)

	// terminate all those workflow executions so that all the tasks in the backlog are invalid
	var wfList []*workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			listResp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("TaskQueue = '%s'", tv.TaskQueue().GetName()),
			})
			s.NoError(err)
			wfList = listResp.GetExecutions()
			return len(wfList) == maxBacklog
		},
		5*time.Second,
		200*time.Millisecond,
	)

	for _, exec := range wfList {
		_, err := s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: exec.GetExecution().GetWorkflowId(), RunId: exec.GetExecution().GetRunId()},
			Reason:            "test",
			Identity:          tv.ClientIdentity(),
		})
		s.NoError(err)
	}

	// start some workers
	workers := make([]worker.Worker, nWorkers)
	for i := 0; i < nWorkers; i++ {
		workers[i] = worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
		workers[i].RegisterWorkflow(helloRateLimitTest)
		err := workers[i].Start()
		s.NoError(err)
	}

	// wait for backlog to be 0
	s.Eventually(
		func() bool {
			wfBacklogCount = s.getBacklogCount(ctx, tv)
			return wfBacklogCount == 0
		},
		timeToDrain,
		500*time.Millisecond,
	)

}

func (s *TaskQueueSuite) getBacklogCount(ctx context.Context, tv *testvars.TestVars) int64 {
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:   s.Namespace().String(),
		TaskQueue:   tv.TaskQueue(),
		ApiMode:     enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		ReportStats: true,
	})
	s.NoError(err)
	return resp.GetVersionsInfo()[""].GetTypesInfo()[sdkclient.TaskQueueTypeWorkflow].GetStats().GetApproximateBacklogCount()
}

func (s *TaskQueueSuite) testTaskQueueRateLimitName(nPartitions, nWorkers int, useNewMatching bool) string {
	ret := fmt.Sprintf("%vPartitions_%vWorkers", nPartitions, nWorkers)
	if useNewMatching {
		return "NewMatching_" + ret
	}
	return "OldMatching_" + ret
}
