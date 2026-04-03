package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
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
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, useNewMatching)
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
	for wfidx := range maxBacklog {
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
	for i := range nWorkers {
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
// Note: The flakiness buffer has since been increased to 3.5s, but it was still flaky (err context deadline exceeded),
// so I now increased it to 3.75s and increasing the context deadline. If the buffer is >=4s, the test stops
// testing anything because the test will succeed even if all the activities complete immediately as if there were no rate limit.
// TODO(matching team): Possibly rewrite test if this issue persists.
func (s *TaskQueueSuite) TestTaskQueueAPIRateLimitOverridesWorkerLimit() {
	s.T().Skip("skip until we make it less flaky")
	const (
		apiRPS            = 5.0
		taskCount         = 25
		buffer            = time.Duration(3.75 * float64(time.Second)) // High Buffer to account for test flakiness
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
		// Note: Flaked at 10s, so trying 30s.
		drainTimeout = 30 * time.Second
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
	s.Empty(runTimes, "No activities should run when API rate limit is 0")
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

	s.Require().Eventually(func() bool {
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
	s.T().Skip("skip until we make it less flaky")
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
	s.T().Skip("skip until we make it less flaky")
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
	s.T().Skip("skip until we make it less flaky")
	const (
		perKeyRPS     = 5.0    // base per-key limit
		wholeQueueRPS = 1000.0 // keep high so only per-key gates
		tasksPerKey   = 30
		buffer        = 3 * time.Second
	)

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
				s.Len(task.History.Events, 3)

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

func (s *TaskQueueSuite) TestTaskDispatchLatencyMetric_WorkflowAndActivity() {
	s.testTaskDispatchLatencyMetric(testTaskDispatchLatencyEmitted)
}

func (s *TaskQueueSuite) TestTaskDispatchLatencyMetric_Query() {
	s.testTaskDispatchLatencyMetric(testQueryTaskDispatchLatencyEmitted)
}

func (s *TaskQueueSuite) TestTaskDispatchLatencyMetric_Nexus() {
	s.testTaskDispatchLatencyMetric(testNexusTaskDispatchLatencyEmitted)
}

func (s *TaskQueueSuite) testTaskDispatchLatencyMetric(scenario func(s *testcore.TestEnv, expectedForwarded, expectedSource, expectedPartitionID string, forwardDelay time.Duration)) {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingForwarderMaxChildrenPerNode, 3),
		testcore.WithDynamicConfig(dynamicconfig.MatchingEmitTaskDispatchLatencyAtPoll, true),
	}

	runWithMatchingBehaviors(s.T(), baseOpts, func(s *testcore.TestEnv, b testcore.MatchingBehavior) {

		// When task forwarding is forced, inject a delay so we can verify
		// the latency metric captures forwarding time.
		var forwardDelay time.Duration
		if b.ForceTaskForward {
			forwardDelay = 100 * time.Millisecond
			s.InjectHook(testhooks.NewHook(testhooks.MatchingForwardTaskDelay, forwardDelay))
			forwardDelay *= 2 // two forward hops
		}

		// Determine expected tag values based on matching behavior.
		expectedForwarded := "false"
		expectedPartitionID := "0"
		if b.ForceTaskForward {
			expectedForwarded = "true"
			expectedPartitionID = "11"
		}

		// When async is forced, tasks always go through DB backlog.
		// When sync is allowed, the source depends on timing and is non-deterministic.
		expectedSource := "History"
		if b.ForceAsync {
			expectedSource = "DbBacklog"
		}

		scenario(s, expectedForwarded, expectedSource, expectedPartitionID, forwardDelay)
	})
}

func testTaskDispatchLatencyEmitted(s *testcore.TestEnv, expectedForwarded, expectedSource, expectedPartitionID string, minLatency time.Duration) {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	activityStarted := make(chan struct{})

	// Poll and handle the workflow task: schedule an activity.
	go func() {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
							Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
								ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
									ActivityId:             "act-1",
									ActivityType:           tv.ActivityType(),
									TaskQueue:              tv.TaskQueue(),
									ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
								},
							},
						},
					},
				}, nil
			},
		)
		s.NoError(err)
	}()

	// Poll and handle the activity task.
	go func() {
		_, err := s.TaskPoller().PollAndHandleActivityTask(tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				activityStarted <- struct{}{}
				return &workflowservice.RespondActivityTaskCompletedRequest{
					Result: tv.Any().Payloads(),
				}, nil
			},
		)
		s.NoError(err)
	}()

	// Wait for pollers to arrive at root partition 0 for both task queue types
	// before starting the workflow to ensure deterministic sync match.
	for _, tqType := range []enumspb.TaskQueueType{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
	} {
		tqType := tqType
		s.Eventually(func() bool {
			resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
				ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
					NamespaceId: s.NamespaceID().String(),
					TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
						TaskQueue:     tv.TaskQueue().GetName(),
						TaskQueueType: tqType,
					},
					Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
					ReportPollers: true,
				},
			)
			if err != nil {
				return false
			}
			for _, vi := range resp.GetVersionsInfoInternal() {
				if len(vi.GetPhysicalTaskQueueInfo().GetPollers()) > 0 {
					return true
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "pollers did not arrive at root partition")
	}

	// Start a workflow to generate a workflow task.
	_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     tv.ClientIdentity(),
		Priority: &commonpb.Priority{
			PriorityKey: 2,
		},
	})
	s.NoError(err)

	<-activityStarted
	snap := capture.Snapshot()

	// Filter recordings for our specific task queue to avoid interference from other activity.
	tqName := tv.TaskQueue().GetName()
	var recordings []*metricstest.CapturedRecording
	for _, rec := range snap["task_dispatch_latency"] {
		if rec.Tags["taskqueue"] == tqName {
			recordings = append(recordings, rec)
		}
	}
	s.NotEmpty(recordings, "expected task_dispatch_latency metric to be recorded")

	// Verify no redundant emissions: expect exactly one recording per task type
	// (1 workflow task + 1 activity task). The metric is emitted only at the partition
	// serving the poll (forwarded-poll intermediaries skip already-started tasks,
	// and matchers skip emission when EmitTaskDispatchLatencyAtPoll is enabled).
	var workflowCount, activityCount int
	for _, rec := range recordings {
		latency, ok := rec.Value.(time.Duration)
		s.True(ok, "expected metric value to be time.Duration")
		s.Greater(latency, time.Duration(0), "expected positive dispatch latency")
		if minLatency > 0 {
			s.GreaterOrEqual(latency, minLatency, "expected dispatch latency to include forwarding delay")
		}

		// Validate source tag.
		source := rec.Tags["source"]
		s.True(
			source == "History" || source == "DbBacklog",
			"unexpected source tag value: %s", source,
		)
		if expectedSource != "" {
			s.Equal(expectedSource, source, "unexpected source tag")
		}

		// Validate forwarded tag.
		s.Equal(expectedForwarded, rec.Tags["forwarded"], "unexpected forwarded tag")

		// Validate taskqueue tag (breakdown enabled by default).
		s.Equal(tqName, rec.Tags["taskqueue"], "unexpected taskqueue tag")

		// Validate partition tag: poll is always served at root partition 0.
		s.Equal(expectedPartitionID, rec.Tags["partition"], "unexpected partition tag")

		// Validate worker_version tag matches the deployment options passed to the poll.
		// With BreakdownMetricsByBuildID enabled (default), the tag is "deploymentName:buildId".
		s.Equal("__unversioned__", rec.Tags["worker_version"], "unexpected worker_version tag")

		// Validate task_priority tag is present (empty string for default priority).
		s.Equal("2", rec.Tags["task_priority"], "unexpected task_priority tag")

		switch rec.Tags["task_type"] {
		case "Workflow":
			workflowCount++
		case "Activity":
			activityCount++
		default:
			s.Failf("unexpected task_type", "got %s", rec.Tags["task_type"])
		}
	}

	s.Equal(1, workflowCount, "expected exactly 1 task_dispatch_latency recording for workflow task")
	s.Equal(1, activityCount, "expected exactly 1 task_dispatch_latency recording for activity task")
}

func testNexusTaskDispatchLatencyEmitted(s *testcore.TestEnv, expectedForwarded, _, expectedPartitionID string, minLatency time.Duration) {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a nexus endpoint targeting our task queue.
	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: "nexus-" + uuid.New().String(),
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: tv.TaskQueue().GetName(),
					},
				},
			},
		},
	})
	s.NoError(err)

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	nexusDone := make(chan struct{})

	// Start nexus task poller goroutine.
	go func() {
		_, err := s.TaskPoller().PollNexusTask(&workflowservice.PollNexusTaskQueueRequest{}).HandleTask(tv,
			func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
				close(nexusDone)
				return &workflowservice.RespondNexusTaskCompletedRequest{}, nil
			},
		)
		s.NoError(err)
	}()

	// Wait for nexus poller to arrive at root partition before dispatching.
	s.Eventually(func() bool {
		resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
			ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: s.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     tv.TaskQueue().GetName(),
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
				},
				Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
				ReportPollers: true,
			},
		)
		if err != nil {
			return false
		}
		for _, vi := range resp.GetVersionsInfoInternal() {
			if len(vi.GetPhysicalTaskQueueInfo().GetPollers()) > 0 {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "nexus poller did not arrive at root partition")

	// Build the DispatchNexusTask request. When forwarding is expected, target
	// partition 11 directly via the RPC name so the child partition forwards to root.
	tqName := tv.TaskQueue().GetName()
	dispatchTQName := tqName
	if expectedForwarded == "true" {
		tqFamily, tqErr := tqid.NewTaskQueueFamily(s.NamespaceID().String(), tqName)
		s.NoError(tqErr)
		nexusTQ := tqFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_NEXUS)
		dispatchTQName = nexusTQ.NormalPartition(11).RpcName()
	}

	_, err = s.GetTestCluster().MatchingClient().DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: dispatchTQName,
		},
		Request: &nexuspb.Request{
			Header: map[string]string{
				"key": "value",
			},
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   tv.Any().String(),
					Operation: tv.Any().String(),
				},
			},
		},
	})
	s.NoError(err)

	<-nexusDone
	snap := capture.Snapshot()

	// Filter recordings for our specific task queue.
	var recordings []*metricstest.CapturedRecording
	for _, rec := range snap["task_dispatch_latency"] {
		if rec.Tags["taskqueue"] == tqName {
			recordings = append(recordings, rec)
		}
	}
	s.NotEmpty(recordings, "expected task_dispatch_latency metric for nexus task")

	var nexusCount int
	for _, rec := range recordings {
		latency, ok := rec.Value.(time.Duration)
		s.True(ok, "expected metric value to be time.Duration")
		s.Greater(latency, time.Duration(0), "expected positive dispatch latency")
		if minLatency > 0 {
			s.GreaterOrEqual(latency, minLatency, "expected dispatch latency to include forwarding delay")
		}

		s.Equal("Nexus", rec.Tags["task_type"], "expected Nexus task_type")

		// Nexus tasks are always sync-matched (source is History).
		s.Equal("History", rec.Tags["source"], "unexpected source tag for nexus")

		s.Equal(expectedForwarded, rec.Tags["forwarded"], "unexpected forwarded tag")
		s.Equal(tqName, rec.Tags["taskqueue"], "unexpected taskqueue tag")
		s.Equal(expectedPartitionID, rec.Tags["partition"], "unexpected partition tag")
		s.Equal("__unversioned__", rec.Tags["worker_version"], "unexpected worker_version tag")
		s.Empty(rec.Tags["task_priority"], "expected empty task_priority for nexus (no priority support)")
		nexusCount++
	}

	s.Equal(1, nexusCount, "expected exactly 1 task_dispatch_latency recording for nexus task")
}

func testQueryTaskDispatchLatencyEmitted(s *testcore.TestEnv, expectedForwarded, _, expectedPartitionID string, minLatency time.Duration) {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wftDone := make(chan struct{})

	// Poll and handle the initial workflow task to get the workflow running.
	go func() {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			},
		)
		s.NoError(err)
		close(wftDone)
	}()

	// Wait for poller to arrive at root partition before starting workflow.
	s.Eventually(func() bool {
		resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
			ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: s.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     tv.TaskQueue().GetName(),
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
				Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
				ReportPollers: true,
			},
		)
		if err != nil {
			return false
		}
		for _, vi := range resp.GetVersionsInfoInternal() {
			if len(vi.GetPhysicalTaskQueueInfo().GetPollers()) > 0 {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "pollers did not arrive at root partition")

	// Start workflow and wait for the initial WFT to be handled.
	_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     tv.ClientIdentity(),
		Priority: &commonpb.Priority{
			PriorityKey: 2,
		},
	})
	s.NoError(err)
	<-wftDone

	// Now start metric capture (after WFT so only query metric is captured).
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	queryDone := make(chan struct{})

	// Start query poller goroutine.
	go func() {
		_, err := s.TaskPoller().PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{}).
			HandleLegacyQuery(tv,
				func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
					return &workflowservice.RespondQueryTaskCompletedRequest{
						CompletedType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
						QueryResult:   payloads.EncodeString("query-result"),
					}, nil
				},
			)
		s.NoError(err)
		close(queryDone)
	}()

	// Wait for query poller to arrive before issuing query.
	s.Eventually(func() bool {
		resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
			ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: s.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     tv.TaskQueue().GetName(),
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
				Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
				ReportPollers: true,
			},
		)
		if err != nil {
			return false
		}
		for _, vi := range resp.GetVersionsInfoInternal() {
			if len(vi.GetPhysicalTaskQueueInfo().GetPollers()) > 0 {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "query poller did not arrive at root partition")

	// Issue the query.
	_, err = s.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
		Query:     &querypb.WorkflowQuery{QueryType: "test-query"},
	})
	s.NoError(err)

	<-queryDone
	snap := capture.Snapshot()

	// Filter recordings for our specific task queue.
	tqName := tv.TaskQueue().GetName()
	var recordings []*metricstest.CapturedRecording
	for _, rec := range snap["task_dispatch_latency"] {
		if rec.Tags["taskqueue"] == tqName {
			recordings = append(recordings, rec)
		}
	}
	s.NotEmpty(recordings, "expected task_dispatch_latency metric for query task")

	var queryCount int
	for _, rec := range recordings {
		latency, ok := rec.Value.(time.Duration)
		s.True(ok, "expected metric value to be time.Duration")
		s.Greater(latency, time.Duration(0), "expected positive dispatch latency")
		if minLatency > 0 {
			s.GreaterOrEqual(latency, minLatency, "expected dispatch latency to include forwarding delay")
		}

		// Query tasks are dispatched through the workflow task queue.
		s.Equal("Workflow", rec.Tags["task_type"], "expected Workflow task_type for query")

		// Queries are always sync-matched (source is History).
		s.Equal("History", rec.Tags["source"], "unexpected source tag for query")

		s.Equal(expectedForwarded, rec.Tags["forwarded"], "unexpected forwarded tag")
		s.Equal(tqName, rec.Tags["taskqueue"], "unexpected taskqueue tag")
		s.Equal(expectedPartitionID, rec.Tags["partition"], "unexpected partition tag")
		s.Equal("__unversioned__", rec.Tags["worker_version"], "unexpected worker_version tag")
		s.Equal("2", rec.Tags["task_priority"], "expected empty task_priority for query (default priority)")
		queryCount++
	}

	s.Equal(1, queryCount, "expected exactly 1 task_dispatch_latency recording for query task")
}

func (s *TaskQueueSuite) TestShutdownWorkerCancelsOutstandingPolls() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCancelWorkerPollsOnShutdown, true)

	tv := testvars.New(s.T())
	workerInstanceKey := uuid.NewString()

	// Use a long poll timeout (2 minutes) to ensure we're testing cancellation, not timeout.
	pollTimeout := 2 * time.Minute

	// Start 2 long polls in goroutines to verify bulk cancellation
	var wg sync.WaitGroup
	pollResults := make(chan struct {
		resp *workflowservice.PollWorkflowTaskQueueResponse
		err  error
	}, 2)

	for range 2 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
			defer cancel()
			resp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace:         s.Namespace().String(),
				TaskQueue:         tv.TaskQueue(),
				Identity:          tv.WorkerIdentity(),
				WorkerInstanceKey: workerInstanceKey,
			})
			pollResults <- struct {
				resp *workflowservice.PollWorkflowTaskQueueResponse
				err  error
			}{resp, err}
		})
	}

	// Keep calling ShutdownWorker until all polls are cancelled and complete.
	// Polls register asynchronously, so we retry until all are caught.
	ctx := context.Background()
	s.Eventually(func() bool {
		_, err := s.FrontendClient().ShutdownWorker(ctx, &workflowservice.ShutdownWorkerRequest{
			Namespace:         s.Namespace().String(),
			StickyTaskQueue:   tv.StickyTaskQueue().GetName(),
			Identity:          tv.WorkerIdentity(),
			Reason:            "graceful shutdown test",
			WorkerInstanceKey: workerInstanceKey,
			TaskQueue:         tv.TaskQueue().GetName(),
		})
		s.NoError(err)
		// Check if all polls have completed (short timeout to just check status)
		return common.AwaitWaitGroup(&wg, 50*time.Millisecond)
	}, 30*time.Second, 200*time.Millisecond, "polls did not complete after repeated shutdown attempts")

	close(pollResults)

	// Verify both polls returned empty responses (no task token)
	for result := range pollResults {
		s.NoError(result.err)
		s.NotNil(result.resp)
		s.Empty(result.resp.GetTaskToken(), "poll should return empty response after shutdown")
	}

	// Verify poller is removed from DescribeTaskQueue (eager poller history cleanup)
	descResp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	for _, poller := range descResp.GetPollers() {
		s.NotEqual(tv.WorkerIdentity(), poller.GetIdentity(),
			"poller should be removed from DescribeTaskQueue after shutdown")
	}

	// Verify that subsequent polls from the same worker are rejected immediately
	// (the shutdown worker cache prevents zombie re-polls from stealing tasks).
	// Use a long timeout so we can distinguish "rejected quickly" from "timed out".
	rePollTimeout := 5 * time.Minute

	// Workflow poll should be rejected immediately.
	wfStart := time.Now()
	rePollCtx, rePollCancel := context.WithTimeout(ctx, rePollTimeout)
	defer rePollCancel()
	rePollResp, err := s.FrontendClient().PollWorkflowTaskQueue(rePollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		WorkerInstanceKey: workerInstanceKey,
	})
	s.NoError(err)
	s.NotNil(rePollResp)
	s.Empty(rePollResp.GetTaskToken(), "re-poll from shutdown worker should return empty response")
	// TODO: Replace timing assertion with an explicit poll response field indicating
	// shutdown rejection, so we don't rely on timing to distinguish cache rejection
	// from natural poll timeout. Requires adding a field to PollWorkflowTaskQueueResponse
	// and PollActivityTaskQueueResponse in the public API proto.
	s.Less(time.Since(wfStart), 2*time.Minute, "workflow re-poll should be rejected quickly, not wait for timeout")

	// Activity poll should also be rejected immediately.
	actStart := time.Now()
	actCtx, actCancel := context.WithTimeout(ctx, rePollTimeout)
	defer actCancel()
	actResp, err := s.FrontendClient().PollActivityTaskQueue(actCtx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		WorkerInstanceKey: workerInstanceKey,
	})
	s.NoError(err)
	s.NotNil(actResp)
	s.Empty(actResp.GetTaskToken(), "activity re-poll from shutdown worker should return empty response")
	s.Less(time.Since(actStart), 2*time.Minute, "activity re-poll should be rejected quickly, not wait for timeout")
}

func (s *TaskQueueSuite) TestShutdownWorkerIsIdempotent() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCancelWorkerPollsOnShutdown, true)

	tv := testvars.New(s.T())
	workerInstanceKey := uuid.NewString()
	ctx := context.Background()

	shutdownReq := &workflowservice.ShutdownWorkerRequest{
		Namespace:         s.Namespace().String(),
		StickyTaskQueue:   tv.StickyTaskQueue().GetName(),
		Identity:          tv.WorkerIdentity(),
		Reason:            "idempotency test",
		WorkerInstanceKey: workerInstanceKey,
		TaskQueue:         tv.TaskQueue().GetName(),
	}

	// Call ShutdownWorker three times in a row — all should succeed.
	for range 3 {
		_, err := s.FrontendClient().ShutdownWorker(ctx, shutdownReq)
		s.NoError(err)
	}

	// A poll from this worker should still be rejected after repeated shutdowns.
	pollStart := time.Now()
	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer pollCancel()
	resp, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		WorkerInstanceKey: workerInstanceKey,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Empty(resp.GetTaskToken(), "poll from shutdown worker should return empty response")
	s.Less(time.Since(pollStart), 2*time.Minute, "poll should be rejected quickly by shutdown cache")
}
