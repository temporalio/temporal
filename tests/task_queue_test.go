package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
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
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
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

// TestTaskQueueAPIRateLimitOverridesWorkerLimit tests that the API rate limit overrides the worker rate limit.
// It sets the API rate limit on a task queue to 5 RPS and then launches 25 activities.
// Burst = 5 i.e max(int(math.Ceil(effectiveRPSPartitionWise)), r.config.MinTaskThrottlingBurstSize())
// The expected time for all activities to complete is ~ 4 seconds ((25 - 5)/5) +/- 1 second buffer.
// The first five activities should run immediately, and the rest should be throttled to 5 RPS.
// The test verifies that the total time taken for all activities to complete is within the expected range
// To avoid test flakiness, the test uses a buffer of 1 second for the expected total time.
func (s *TaskQueueSuite) TestTaskQueueAPIRateLimitOverridesWorkerLimit() {
	const (
		apiRPS            = 5.0
		taskCount         = 25
		buffer            = time.Second
		activityTaskQueue = "RateLimitTest"
	)
	expectedTotal := time.Duration(float64(taskCount-int(apiRPS))/apiRPS) * time.Second
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	// Set a very low TTL for task queue info cache to ensure the rate limiter stats
	// are refreshed frequently, avoiding stale data during the test.
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)
	var (
		mu       sync.Mutex
		runTimes []time.Time
		wg       sync.WaitGroup
	)
	const (
		workerRPS = 50.0
		// Test typically completes in ~6.2 seconds on average.
		// Timeout is set to 10s to reduce flakiness.
		drainTimeout = 10 * time.Second
		activityName = "trackableActivity"
	)
	tv := testvars.New(s.T())
	// Apply API rate limit on `activityTaskQueue`
	_, err := s.FrontendClient().UpdateTaskQueueConfig(context.Background(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     activityTaskQueue,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: apiRPS},
			Reason:    "Test API override",
		},
	})
	s.NoError(err)

	wg.Add(taskCount)
	// Track activity run times
	activityFunc := func(context.Context) error {
		defer wg.Done()
		mu.Lock()
		runTimes = append(runTimes, time.Now())
		mu.Unlock()
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			// Route activity tasks to a dedicated task queue named `activityTaskQueue`.
			// This isolates the test by ensuring that only the dedicated worker polls the queue
			TaskQueue:           activityTaskQueue,
			StartToCloseTimeout: 5 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return workflow.ExecuteActivity(ctx, activityName).Get(ctx, nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	// Start the activity worker
	activityWorker := worker.New(s.SdkClient(), activityTaskQueue, worker.Options{
		// Setting rate limit at worker level (this will be ignored in favor of the limit set through the api)
		TaskQueueActivitiesPerSecond: workerRPS,
	})
	activityWorker.RegisterActivityWithOptions(activityFunc, activity.RegisterOptions{Name: activityName})
	s.NoError(activityWorker.Start())
	defer activityWorker.Stop()

	// Start the workflow worker
	wfWorker := worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	wfWorker.RegisterWorkflow(workflowFn)
	s.NoError(wfWorker.Start())
	defer wfWorker.Stop()

	// Launch workflows
	for i := range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf-%d", i),
		}, workflowFn)
		s.NoError(err)
	}

	// Wait for all activities to complete
	s.True(common.AwaitWaitGroup(&wg, drainTimeout), "timeout waiting for activities to complete")
	s.Len(runTimes, taskCount)

	totalGap := runTimes[len(runTimes)-1].Sub(runTimes[0])
	s.GreaterOrEqual(totalGap, expectedTotal-buffer, "Activity run time too short — API rate limit override not taking effect over the worker rate limit")
	s.LessOrEqual(totalGap, expectedTotal+buffer, "Activity run time too long — API rate limit override not enforced as expected")
}

// TestUpdateAndDescribeTaskQueueConfig tests the update and describe task queue config functionality.
// It updates the task queue config via the frontend API and then describes the task queue to verify,
// that the updated configuration is reflected correctly.
func (s *TaskQueueSuite) TestUpdateAndDescribeTaskQueueConfig() {
	tv := testvars.New(s.T())
	taskQueueName := tv.TaskQueue().Name
	namespace := s.Namespace().String()
	taskQueueType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	updateRPS := float32(42)
	updateReason := "frontend-update-test"
	updateIdentity := "test-identity"
	updateReq := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     namespace,
		Identity:      updateIdentity,
		TaskQueue:     taskQueueName,
		TaskQueueType: taskQueueType,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{
				RequestsPerSecond: updateRPS,
			},
			Reason: updateReason,
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{
				RequestsPerSecond: updateRPS,
			},
			Reason: updateReason,
		},
	}
	updateResp, err := s.FrontendClient().UpdateTaskQueueConfig(testcore.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.Config)
	s.Equal(updateRPS, updateResp.Config.QueueRateLimit.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, updateResp.Config.QueueRateLimit.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.QueueRateLimit.Metadata.UpdateIdentity)
	s.Equal(updateRPS, updateResp.Config.FairnessKeysRateLimitDefault.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.UpdateIdentity)

	describeReq := &workflowservice.DescribeTaskQueueRequest{
		Namespace:     namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: taskQueueName},
		TaskQueueType: taskQueueType,
		ReportConfig:  true,
	}
	describeResp, err := s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), describeReq)
	s.NoError(err)
	s.NotNil(describeResp)
	s.NotNil(describeResp.Config)
	s.NotNil(describeResp.Config.QueueRateLimit)
	s.Equal(updateRPS, describeResp.Config.QueueRateLimit.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, describeResp.Config.QueueRateLimit.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.QueueRateLimit.Metadata.UpdateIdentity)
	s.Equal(updateRPS, describeResp.Config.FairnessKeysRateLimitDefault.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, describeResp.Config.FairnessKeysRateLimitDefault.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.UpdateIdentity)
}

func (s *TaskQueueSuite) TestUpdateUnsetAndDescribeTaskQueueConfig() {
	tv := testvars.New(s.T())
	taskQueueName := tv.TaskQueue().Name
	namespace := s.Namespace().String()
	taskQueueType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	updateRPS := float32(42)
	updateReason := "TestUpdateUnsetAndDescribeTaskQueueConfig"
	unsetReasonQueue := "unset queue rate limit"
	unsetReasonFairness := "unset fairness key rate limit"
	updateIdentity := "test-identity"
	// Set rate limit via UpdateTaskQueueConfig api.
	updateReq := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     namespace,
		Identity:      updateIdentity,
		TaskQueue:     taskQueueName,
		TaskQueueType: taskQueueType,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{
				RequestsPerSecond: updateRPS,
			},
			Reason: updateReason,
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{
				RequestsPerSecond: updateRPS,
			},
			Reason: updateReason,
		},
	}
	updateResp, err := s.FrontendClient().UpdateTaskQueueConfig(testcore.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.Config)
	s.Equal(updateRPS, updateResp.Config.QueueRateLimit.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, updateResp.Config.QueueRateLimit.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.QueueRateLimit.Metadata.UpdateIdentity)
	s.Equal(updateRPS, updateResp.Config.FairnessKeysRateLimitDefault.RateLimit.RequestsPerSecond)
	s.Equal(updateReason, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.UpdateIdentity)

	// Unset rate limit via UpdateTaskQueueConfig api.
	unsetReq := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     namespace,
		Identity:      updateIdentity,
		TaskQueue:     taskQueueName,
		TaskQueueType: taskQueueType,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: nil,
			Reason:    unsetReasonQueue,
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: nil,
			Reason:    unsetReasonFairness,
		},
	}
	unsetResp, err := s.FrontendClient().UpdateTaskQueueConfig(testcore.NewContext(), unsetReq)
	s.NoError(err)
	s.NotNil(unsetResp)
	s.NotNil(unsetResp.Config)
	s.Nil(unsetResp.Config.QueueRateLimit.RateLimit)
	s.Equal(unsetReasonQueue, unsetResp.Config.QueueRateLimit.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.QueueRateLimit.Metadata.UpdateIdentity)
	s.Nil(unsetResp.Config.FairnessKeysRateLimitDefault.RateLimit)
	s.Equal(unsetReasonFairness, unsetResp.Config.FairnessKeysRateLimitDefault.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.UpdateIdentity)

	// Describe the task queue to verify the unset configuration.
	describeReq := &workflowservice.DescribeTaskQueueRequest{
		Namespace:     namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: taskQueueName},
		TaskQueueType: taskQueueType,
		ReportConfig:  true,
	}
	describeResp, err := s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), describeReq)
	s.NoError(err)
	s.NotNil(describeResp)
	s.NotNil(describeResp.Config)
	s.Nil(describeResp.Config.QueueRateLimit.RateLimit)
	s.Equal(unsetReasonQueue, describeResp.Config.QueueRateLimit.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.QueueRateLimit.Metadata.UpdateIdentity)
	s.Nil(describeResp.Config.FairnessKeysRateLimitDefault.RateLimit)
	s.Equal(unsetReasonFairness, describeResp.Config.FairnessKeysRateLimitDefault.Metadata.Reason)
	s.Equal(updateIdentity, updateResp.Config.FairnessKeysRateLimitDefault.Metadata.UpdateIdentity)
}
