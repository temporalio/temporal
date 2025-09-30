package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
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
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
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

// configureRateLimitAndLaunchWorkflows sets up the test environment to validate task queue API rate limiting behavior.
//   - Applies an API-level RPS override on the activity task queue by calling the updateTaskQueueConfig api.
//   - Starts an activity worker with a specified worker-side RPS limit.
//   - Starts a workflow worker that dispatches activities to the activity queue.
//   - Launches a given number of workflows that each execute a single activity.
//   - Tracks the time each activity is executed (via runTimes) for test assertions.
//   - Returns the context, cancel function, and both workers so the caller can manage their lifecycle.
func (s *TaskQueueSuite) configureRateLimitAndLaunchWorkflows(
	activityTaskQueue string,
	activityName string,
	taskCount int,
	workerRPS float64,
	drainTimeout time.Duration,
	runTimes *[]time.Time,
	wg *sync.WaitGroup,
	apiRPS float64,
	mu *sync.Mutex,
) (ctx context.Context, cancel context.CancelFunc, activityWorker worker.Worker, wfWorker worker.Worker) {
	tv := testvars.New(s.T())
	// Apply API rate limit on `activityTaskQueue`
	_, err := s.FrontendClient().UpdateTaskQueueConfig(context.Background(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     activityTaskQueue,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(apiRPS)},
			Reason:    "Test API override",
		},
	})
	s.NoError(err)

	wg.Add(taskCount)
	// Track activity run times
	activityFunc := func(context.Context) error {
		defer wg.Done()
		mu.Lock()
		*runTimes = append(*runTimes, time.Now())
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

	ctx, cancel = context.WithTimeout(context.Background(), drainTimeout)

	// Start the activity worker
	activityWorker = worker.New(s.SdkClient(), activityTaskQueue, worker.Options{
		// Setting rate limit at worker level (this will be ignored in favor of the limit set through the api)
		TaskQueueActivitiesPerSecond: workerRPS,
		// Setting rate limit to throttle the worker to 4 activities per second
		WorkerActivitiesPerSecond: 4,
	})
	activityWorker.RegisterActivityWithOptions(activityFunc, activity.RegisterOptions{Name: activityName})
	s.NoError(activityWorker.Start())

	// Start the workflow worker
	wfWorker = worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	wfWorker.RegisterWorkflow(workflowFn)
	s.NoError(wfWorker.Start())

	// Launch workflows
	for i := range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf-%d", i),
		}, workflowFn)
		s.NoError(err)
	}
	return ctx, cancel, activityWorker, wfWorker
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
		buffer            = time.Duration(3.5 * float64(time.Second)) // High Buffer to account for test flakiness
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

	_, cancel, activityWorker, wfWorker := s.configureRateLimitAndLaunchWorkflows(
		activityTaskQueue,
		activityName,
		taskCount,
		workerRPS,
		drainTimeout,
		&runTimes,
		&wg,
		apiRPS,
		&mu,
	)
	defer cancel()
	defer activityWorker.Stop()
	defer wfWorker.Stop()

	// Wait for all activities to complete
	s.True(common.AwaitWaitGroup(&wg, drainTimeout), "timeout waiting for activities to complete")
	s.Len(runTimes, taskCount)

	totalGap := runTimes[len(runTimes)-1].Sub(runTimes[0])
	s.GreaterOrEqual(totalGap, expectedTotal-buffer, "Activity run time too short — API rate limit override not taking effect over the worker rate limit")
	s.LessOrEqual(totalGap, expectedTotal+buffer, "Activity run time too long — API rate limit override not enforced as expected")
}

// TestTaskQueueAPIRateLimitZero ensures that when the API rate limit is set to 0,
// no activity tasks are dispatched. Also checks if the rate limit is set to 0,
// then the burst is defaulted to 0 to prevent any initial tasks from executing immediately.
func (s *TaskQueueSuite) TestTaskQueueAPIRateLimitZero() {
	const (
		apiRPS            = 0.0
		taskCount         = 10
		drainTimeout      = 3 * time.Second
		activityTaskQueue = "RateLimitTestZero"
	)

	// Override configs
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	var (
		mu       sync.Mutex
		runTimes []time.Time
		wg       sync.WaitGroup
	)

	const (
		workerRPS    = 50.0
		activityName = "noopActivity"
	)

	_, cancel, activityWorker, wfWorker := s.configureRateLimitAndLaunchWorkflows(
		activityTaskQueue,
		activityName,
		taskCount,
		workerRPS,
		drainTimeout,
		&runTimes,
		&wg,
		apiRPS,
		&mu,
	)
	defer cancel()
	defer activityWorker.Stop()
	defer wfWorker.Stop()

	// Wait for the duration and ensure no tasks executed
	s.False(common.AwaitWaitGroup(&wg, drainTimeout), "Some activities unexpectedly completed despite API RPS = 0")
	s.Len(runTimes, 0, "No activities should run when API rate limit is 0")
}

func (s *TaskQueueSuite) TestTaskQueueRateLimit_UpdateFromWorkerConfigAndAPI() {
	const (
		workerSetRPS      = 2.0 // Worker rate limit for activities
		apiSetRPS         = 4.0 // API rate limit for activities set to half of workerSetRPS to test override behavior
		taskCount         = 12  // Number of tasks to launch
		activityTaskQueue = "RateLimitTest_Update"
		drainTimeout      = 15 * time.Second // 5 second additional buffer to prevent flakiness
		buffer            = 2 * time.Second
		activityName      = "timedActivity"
	)

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	var (
		mu       sync.Mutex
		runTimes []time.Time
		wg       sync.WaitGroup
	)

	wg.Add(taskCount)

	// Activity: record run time
	activityFunc := func(ctx context.Context) error {
		defer wg.Done()
		mu.Lock()
		runTimes = append(runTimes, time.Now())
		mu.Unlock()
		return nil
	}

	// Workflow: calls the activity
	workflowFn := func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           activityTaskQueue,
			StartToCloseTimeout: 5 * time.Second,
		})
		return workflow.ExecuteActivity(ctx, activityName).Get(ctx, nil)
	}

	// Start activity worker
	activityWorker := worker.New(s.SdkClient(), activityTaskQueue, worker.Options{
		TaskQueueActivitiesPerSecond: workerSetRPS, // worker set RPS should take effect initially
	})
	activityWorker.RegisterActivityWithOptions(activityFunc, activity.RegisterOptions{Name: activityName})
	s.NoError(activityWorker.Start())
	defer activityWorker.Stop()

	// Start workflow worker
	tv := testvars.New(s.T())
	wfWorker := worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	wfWorker.RegisterWorkflow(workflowFn)
	s.NoError(wfWorker.Start())
	defer wfWorker.Stop()

	// Launch workflows under workerSetRPS
	for i := range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf-dynamic-%d", i),
		}, workflowFn)
		s.NoError(err)
	}

	s.True(common.AwaitWaitGroup(&wg, drainTimeout), "tasks with dynamic config didn't complete")
	s.Len(runTimes, taskCount, "task count mismatch")

	// Measure duration with workerSetRPS config
	firstGap := runTimes[len(runTimes)-1].Sub(runTimes[0])

	// Reset for API override phase
	runTimes = nil
	wg.Add(taskCount)

	//  Apply API rate limit override workerSetRPS to set the effective RPS to apiSetRPS
	_, err := s.FrontendClient().UpdateTaskQueueConfig(context.Background(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     activityTaskQueue,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(apiSetRPS)},
			Reason:    "test api override",
		},
	})
	s.NoError(err)

	require.Eventually(s.T(), func() bool {
		describeResp, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     &taskqueuepb.TaskQueue{Name: activityTaskQueue},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			ReportConfig:  true,
		})
		if err != nil {
			return false
		}
		cfg := describeResp.GetConfig()
		rl := cfg.GetQueueRateLimit().GetRateLimit()
		if rl == nil {
			s.T().Logf("Rate limit not set in Persistence")
			return false
		}
		if rl.GetRequestsPerSecond() == float32(apiSetRPS) {
			s.T().Logf("Rate limit set in Persistence: %v", rl.GetRequestsPerSecond())
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond, "DescribeTaskQueue did not reflect override")

	// Launch workflows under API override
	for i := range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf-api-%d", i),
		}, workflowFn)
		s.NoError(err)
	}

	s.True(common.AwaitWaitGroup(&wg, drainTimeout), "tasks with API override didn't complete")
	s.Len(runTimes, taskCount, "task count mismatch after API override")

	// Measure duration with API override
	secondGap := runTimes[len(runTimes)-1].Sub(runTimes[0])
	s.T().Logf("Completion time for First set of Activity tasks: %v, Second Activity tasks: %v", firstGap, secondGap)

	// first gap must be ideally twice as larger as the effective RPS is doubled
	// To avoid flakiness, we allow a buffer of 1.5x the first gap along with an additional buffer of 2s
	s.Greater(firstGap, time.Duration(1.5*float64(secondGap))-buffer,
		"Expected ~2x span at 2 rps vs 4 rps: first=%v (2rps) second=%v (4rps)",
		firstGap, secondGap)
}

func (s *TaskQueueSuite) TestWholeQueueLimit_TighterThanPerKeyDefault_IsEnforced() {
	const (
		wholeQueueRPS = 10.0 // tighter
		perKeyRPS     = 50.0 // looser than whole queue, should not bind
		tasksPerKey   = 30
		buffer        = 3 * time.Second // CI jitter
	)
	fairnessKeysWithWeight := map[string]float32{"A": 1.0, "B": 1.0, "C": 1.0}
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fast refresh so ratelimiter picks up config quickly.
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	// configure task queue
	_, err := s.FrontendClient().UpdateTaskQueueConfig(ctx, &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     tv.TaskQueue().GetName(),
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(wholeQueueRPS)},
			Reason:    "test: whole-queue limit",
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(perKeyRPS)},
			Reason:    "test: per-key default",
		},
	})
	s.NoError(err)

	_, allTimes := s.runActivitiesWithPriorities(ctx, tv, fairnessKeysWithWeight, tasksPerKey)

	// Measure overall throughput after initial burst, which should be limited by wholeQueueRPS.
	start := allTimes[0]
	end := allTimes[len(allTimes)-1]

	expected := time.Duration(float64(tasksPerKey*len(fairnessKeysWithWeight))/wholeQueueRPS) * time.Second
	actual := end.Sub(start)

	s.T().Logf("Time taken for tasks across fairness keys to drain is %v vs expected %v", actual, expected)
	s.GreaterOrEqual(actual, expected-buffer,
		"whole-queue RPS violated: actual %v < expected %v (-%v buffer)", actual, expected, buffer)

	s.LessOrEqual(actual, expected+buffer,
		"too slow overall: actual %v > expected %v (+%v buffer)", actual, expected, buffer)
}

func (s *TaskQueueSuite) TestPerKeyRateLimit_Default_IsEnforcedAcrossThreeKeys() {
	const (
		perKeyRPS     = 5.0
		wholeQueueRPS = 1000.0 // tighter
		tasksPerKey   = 30
		buffer        = 3 * time.Second // relax for CI jitter
	)
	fairnessKeysWithWeight := map[string]float32{"A": 1.0, "B": 1.0, "C": 1.0}

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fast refresh so ratelimiter picks up config quickly.
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	// configure task queue
	_, err := s.FrontendClient().UpdateTaskQueueConfig(ctx, &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     tv.TaskQueue().GetName(),
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(wholeQueueRPS)},
			Reason:    "test: whole-queue limit",
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(perKeyRPS)},
			Reason:    "test: per-key default",
		},
	})
	s.NoError(err)

	perKeyTimes, _ := s.runActivitiesWithPriorities(ctx, tv, fairnessKeysWithWeight, tasksPerKey)

	for key := range fairnessKeysWithWeight {
		times := perKeyTimes[key]
		s.Len(times, tasksPerKey, "unexpected count for key %s", key)

		start := times[0]
		end := times[len(times)-1]

		expected := time.Duration(float64(tasksPerKey)/perKeyRPS) * time.Second
		actual := end.Sub(start)

		s.T().Logf("Time taken for fairness key %s to drain : %v vs expected %v", key, actual, expected)
		s.GreaterOrEqual(actual, expected-buffer,
			"per-key RPS violated for key %s: actual %v < expected %v (-%v buffer)",
			key, actual, expected)

		s.LessOrEqual(actual, expected+buffer,
			"too slow for key %s: actual %v > expected %v (+%v buffer)", key, actual, expected, buffer)
	}
}

func (s *TaskQueueSuite) TestPerKeyRateLimit_WeightOverride_IsEnforcedAcrossThreeKeys() {
	const (
		perKeyRPS     = 5.0    // base per-key limit
		wholeQueueRPS = 1000.0 // keep high so only per-key gates
		tasksPerKey   = 30
		buffer        = 3 * time.Second
	)

	// Fast refresh so ratelimiter picks up config quickly.
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	// Fairness key overrides take precedence over default.
	// Override A and C to default and make B twice as heavy (ie ~2x effective RPS).
	fairnessKeysWithWeight := map[string]float32{"A": 6666.0, "B": 0.0, "C": 6666.0} // defaults are opposite of override
	fairnessWeightOverrides := map[string]float32{"A": 1.0, "B": 2.0, "C": 1.0}

	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// configure task queue
	_, err := s.FrontendClient().UpdateTaskQueueConfig(ctx, &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     tv.TaskQueue().GetName(),
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(wholeQueueRPS)},
			Reason:    "test: whole-queue limit",
		},
		UpdateFairnessKeyRateLimitDefault: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(perKeyRPS)},
			Reason:    "test: per-key default",
		},
		SetFairnessWeightOverrides: fairnessWeightOverrides,
	})
	s.NoError(err)

	perKeyTimes, _ := s.runActivitiesWithPriorities(ctx, tv, fairnessKeysWithWeight, tasksPerKey)

	for key, fairnessWeightOverride := range fairnessWeightOverrides {
		times := perKeyTimes[key]
		s.Len(times, tasksPerKey, "unexpected count for key %s", key)

		start := times[0]
		end := times[len(times)-1]

		expected := time.Duration(float64(tasksPerKey)/(perKeyRPS*float64(fairnessWeightOverride))) * time.Second
		actual := end.Sub(start)

		s.T().Logf("Time taken for fairness key %s with weight %v to drain : %v vs expected %v",
			key, fairnessWeightOverride, actual, expected)
		s.GreaterOrEqual(actual, expected-buffer,
			"per-key RPS violated for key %s with weight %v: actual %v < expected %v (-%v buffer)",
			key, fairnessWeightOverride, actual, expected)

		s.LessOrEqual(actual, expected+buffer,
			"too slow for key %s with weight %v: actual %v > expected %v (+%v buffer)",
			key, fairnessWeightOverride, actual, expected, buffer)
	}
}

// TestUpdateAndDescribeTaskQueueConfig tests the update and describe task queue config functionality.
// It updates the task queue config via the frontend API and then describes the task queue to verify,
// that the updated configuration is reflected correctly.
func (s *TaskQueueSuite) TestUpdateAndDescribeTaskQueueConfig() {
	// Enforce a smaller limit for max fairness key weight overrides for this test
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxFairnessKeyWeightOverrides, 5)

	// Send update.
	tv := testvars.New(s.T())
	taskQueueName := tv.TaskQueue().Name
	namespace := s.Namespace().String()
	taskQueueType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	updateRPS := float32(42)
	updateReason := "frontend-update-test"
	updateIdentity := "test-identity"
	fairnessOverrides := map[string]float32{"k1": 1.0, "k2": 1.5, "k3": 2.0, "k4": 0.5, "k5": 3.0}
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
		SetFairnessWeightOverrides: fairnessOverrides,
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
	s.Equal(fairnessOverrides, updateResp.Config.FairnessWeightOverrides)

	// Request describe.
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
	s.Equal(fairnessOverrides, describeResp.Config.FairnessWeightOverrides)

	// Attempt to exceed the maximum allowed fairness weight overrides.
	exceedReq := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:                  namespace,
		Identity:                   updateIdentity,
		TaskQueue:                  taskQueueName,
		TaskQueueType:              taskQueueType,
		SetFairnessWeightOverrides: map[string]float32{"k6": 1.0}, // Exceeds the limit of 5
	}
	_, err = s.FrontendClient().UpdateTaskQueueConfig(testcore.NewContext(), exceedReq)
	s.Error(err)
	s.ErrorContains(err, "fairness weight overrides update rejected")

	// Verify no change after rejected update.
	describeResp, err = s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), describeReq)
	s.NoError(err)
	s.Equal(fairnessOverrides, describeResp.Config.FairnessWeightOverrides)
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

// removed: inlined where needed

func (s *TaskQueueSuite) runActivitiesWithPriorities(
	ctx context.Context,
	tv *testvars.TestVars,
	fairnessKeysWithWeight map[string]float32,
	activitiesPerKey int,
) (map[string][]time.Time, []time.Time) {
	fairnessKeys := make([]string, 0, len(fairnessKeysWithWeight))
	for k := range fairnessKeysWithWeight {
		fairnessKeys = append(fairnessKeys, k)
	}
	total := len(fairnessKeys) * activitiesPerKey

	// Enable fairness
	s.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, true)
	// Single partition for simplicity.
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	// generate a unique base so IDs don't collide across shards/tests
	base := uuid.NewString()
	parsePattern := fmt.Sprintf("perkey-wf-%s-%%d", base) // for Sscanf later

	// Start workflows (each will schedule one activity tagged with a fairness key).
	for i := range total {
		wfID := fmt.Sprintf("perkey-wf-%s-%d", base, i)
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   wfID,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// Drain workflow tasks -> schedule activities with Priority.FairnessKey.
	wfHandled := 0
	for wfHandled < total {
		if err := ctx.Err(); err != nil {
			s.T().Fatalf("context deadline while draining workflow tasks: handled=%d/%d: %v", wfHandled, total, err)
		}
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Equal(3, len(task.History.Events))

				var idx int
				_, scanErr := fmt.Sscanf(task.WorkflowExecution.WorkflowId, parsePattern, &idx)
				s.NoError(scanErr)

				key := fairnessKeys[idx%len(fairnessKeys)]
				weight := fairnessKeysWithWeight[key]
				input, encErr := payloads.Encode(key)
				s.NoError(encErr)

				cmd := &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             fmt.Sprintf("act-%d", idx),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
							Priority:               &commonpb.Priority{FairnessKey: key, FairnessWeight: weight},
							Input:                  input,
						},
					},
				}
				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{cmd}}, nil
			},
			taskpoller.WithContext(ctx),
		)
		if err == nil {
			wfHandled++
		}
	}
	s.Equal(total, wfHandled)

	// Drain activity tasks, recording times.
	perKeyTimes := make(map[string][]time.Time, len(fairnessKeys))
	for _, k := range fairnessKeys {
		perKeyTimes[k] = []time.Time{}
	}
	allTimes := make([]time.Time, 0, total)

	actsHandled := 0
	for actsHandled < total {
		if err := ctx.Err(); err != nil {
			s.T().Fatalf("context deadline while draining activity tasks: handled=%d/%d: %v", actsHandled, total, err)
		}
		_, err := s.TaskPoller().PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				var key string
				s.NoError(payloads.Decode(task.Input, &key))
				now := time.Now()
				perKeyTimes[key] = append(perKeyTimes[key], now)
				allTimes = append(allTimes, now)
				nothing, encErr := payloads.Encode()
				s.NoError(encErr)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(ctx),
		)
		if err == nil {
			actsHandled++
		}
	}
	s.Equal(total, actsHandled)

	// perKeyTimes : Used to verify that each key's activities are throttled correctly.
	// allTimes : Used to verify the overall throughput of the task queue.
	return perKeyTimes, allTimes
}
