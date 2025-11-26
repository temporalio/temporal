package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	expmaps "golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TaskQueueSuite struct {
	testcore.FunctionalTestBase
	newMatcher bool
}

func TestTaskQueueSuite_Classic(t *testing.T) {
	t.Parallel()
	suite.Run(t, &TaskQueueSuite{newMatcher: false})
}

func TestTaskQueueSuite_New(t *testing.T) {
	t.Parallel()
	suite.Run(t, &TaskQueueSuite{newMatcher: true})
}

func (s *TaskQueueSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key(): s.newMatcher,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *TaskQueueSuite) TestRateLimitNotAppliedToInvalidTasks() {
	s.Run("1Partition_2Pollers", func() { s.taskQueueRateLimitTest(30, 1, 2, 12*time.Second) })

	// Testing multiple partitions with insufficient pollers is too flaky, because token recycling
	// depends on a process being available to accept the token, so I'm not testing it
	s.Run("4Partitions_16Pollers", func() { s.taskQueueRateLimitTest(30, 4, 16, 24*time.Second) })
}

func (s *TaskQueueSuite) taskQueueRateLimitTest(nWorkflows, nPartitions, nPollers int, timeToDrain time.Duration) {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, nPartitions)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, nPartitions)

	// exclude the effect of the default forwarding rate limit (10)
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxRatePerSecond, 1000)

	// 30 tasks at 1 task per second is 30 seconds.
	// if invalid tasks are NOT using the rate limit, then this should take well below that long.
	// task forwarding between task queue partitions is rate-limited by default to 10 rps.
	s.OverrideDynamicConfig(dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, 1)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0)

	tv := testvars.New(s.T())

	helloRateLimitTest := func(ctx workflow.Context, name string) (string, error) {
		return "Hello " + name + " !", nil
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// start workflows to create a backlog
	for wfidx := range nWorkflows {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf%d", wfidx),
		}, helloRateLimitTest, "Donna")
		s.NoError(err)
	}

	// wait for backlog to be >= maxBacklog
	s.Eventually(
		func() bool { return s.getBacklogCount(ctx, tv) == nWorkflows },
		5*time.Second,
		200*time.Millisecond,
	)

	// terminate all those workflow executions so that all the tasks in the backlog are invalid
	for wfidx := range nWorkflows {
		_, err := s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("wf%d", wfidx)},
		})
		s.NoError(err)
	}

	// start a worker
	w := sdkworker.New(s.SdkClient(), tv.TaskQueue().GetName(), sdkworker.Options{
		MaxConcurrentWorkflowTaskPollers: nPollers,
	})
	w.RegisterWorkflow(helloRateLimitTest)
	s.NoError(w.Start())
	defer w.Stop()

	// wait for backlog to be 0
	s.Eventually(
		func() bool { return s.getBacklogCount(ctx, tv) == 0 },
		timeToDrain,
		200*time.Millisecond,
	)
	s.T().Log("elapsed", time.Since(start), "out of", timeToDrain)
}

func (s *TaskQueueSuite) getBacklogCount(ctx context.Context, tv *testvars.TestVars) int {
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ApiMode:       enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		ReportStats:   true,
	})
	s.NoError(err)
	// nolint:staticcheck // using deprecated field
	return int(resp.GetVersionsInfo()[""].GetTypesInfo()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetStats().GetApproximateBacklogCount())
}

// configureRateLimitAndLaunchWorkflows sets up the test environment to validate task queue API rate limiting behavior.
//   - Applies an API-level RPS override on the activity task queue by calling the updateTaskQueueConfig api.
//   - Starts an activity worker with a specified worker-side RPS limit.
//   - Starts a workflow worker that dispatches activities to the activity queue.
//   - Launches a given number of workflows that each execute a single activity.
//   - Tracks the time each activity is executed (via runTimes) for test assertions.
func (s *TaskQueueSuite) configureRateLimitAndLaunchWorkflows(
	taskCount int,
	workerRPS float64,
	apiRPS float64,
	drainTimeout time.Duration,
) {
	tv := testvars.New(s.T())
	// Apply API rate limit
	_, err := s.FrontendClient().UpdateTaskQueueConfig(context.Background(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     tv.TaskQueue().Name,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(apiRPS)},
			Reason:    "Test API override",
		},
	})
	s.NoError(err)

	// Track activity run times
	var mu sync.Mutex
	var runTimes []time.Time
	activityFunc := func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		runTimes = append(runTimes, time.Now())
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			TaskQueue:           tv.TaskQueue().Name,
			StartToCloseTimeout: 5 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return workflow.ExecuteActivity(ctx, activityFunc).Get(ctx, nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout+5*time.Second)
	defer cancel()

	// Start the worker
	worker := sdkworker.New(s.SdkClient(), tv.TaskQueue().Name, sdkworker.Options{
		// Setting rate limit at worker level (this will be ignored in favor of the limit set through the api)
		TaskQueueActivitiesPerSecond:     workerRPS,
		MaxConcurrentActivityTaskPollers: 10,
		MaxConcurrentWorkflowTaskPollers: 10,
	})
	worker.RegisterActivity(activityFunc)
	worker.RegisterWorkflow(workflowFn)
	s.NoError(worker.Start())
	defer worker.Stop()

	// Launch workflows
	for range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
		}, workflowFn)
		s.NoError(err)
	}

	if apiRPS > 0 {
		// Wait for all activities to complete
		s.Eventually(func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(runTimes) == taskCount
		}, drainTimeout, 100*time.Millisecond, "timeout waiting for activities to complete")

		mu.Lock()
		defer mu.Unlock()
		s.InEpsilon(apiRPS, getAvgRate(runTimes, apiRPS), 0.3, "rate limit was not close enough to expected")
	} else {
		// Wait for the duration and ensure no tasks executed
		time.Sleep(drainTimeout) // nolint:forbidigo // checking that a thing didn't happen
		mu.Lock()
		defer mu.Unlock()
		s.Empty(runTimes, "Some activities unexpectedly completed despite API RPS = 0")
	}
}

// gets average rate with linear regression
func getAvgRate(ts []time.Time, ignoreInitialBurst float64) float64 {
	ts = ts[int(ignoreInitialBurst):]
	n := float64(len(ts))
	var sumX, sumY, sumXY, sumXX float64
	for x, t := range ts {
		xf := float64(x)
		y := t.Sub(ts[0]).Seconds()
		sumX += xf
		sumY += y
		sumXY += xf * y
		sumXX += xf * xf
	}
	return (n*sumXX - sumX*sumX) / (n*sumXY - sumX*sumY)
}

// TestTaskQueueAPIRateLimitOverridesWorkerLimit tests that the API rate limit overrides the worker rate limit.
// We ignore the initial burst when calculating the average rate.
func (s *TaskQueueSuite) TestTaskQueueAPIRateLimitOverridesWorkerLimit() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.configureRateLimitAndLaunchWorkflows(
		55,
		50.0,
		5.0,
		20*time.Second,
	)
}

// TestTaskQueueAPIRateLimitZero ensures that when the API rate limit is set to 0,
// no activity tasks are dispatched. Also checks if the rate limit is set to 0,
// then the burst is defaulted to 0 to prevent any initial tasks from executing immediately.
func (s *TaskQueueSuite) TestTaskQueueAPIRateLimitZero() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.configureRateLimitAndLaunchWorkflows(
		10,
		50.0,
		0.0, // no tasks should run
		3*time.Second,
	)
}

func (s *TaskQueueSuite) TestTaskQueueRateLimit_UpdateFromWorkerConfigAndAPI() {
	const (
		workerSetRPS      = 2.0 // Worker rate limit for activities
		apiSetRPS         = 4.0 // API rate limit for activities set to half of workerSetRPS to test override behavior
		taskCount         = 36  // Number of tasks to launch
		activityTaskQueue = "RateLimitTest_Update"
		drainTimeout      = 35 * time.Second // 5 second additional buffer to prevent flakiness
	)

	tv := testvars.New(s.T())
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	var mu sync.Mutex
	var runTimes []time.Time

	// Activity: record run time
	activityFunc := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		runTimes = append(runTimes, time.Now())
		return nil
	}

	// Workflow: calls the activity
	workflowFn := func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           tv.TaskQueue().Name,
			StartToCloseTimeout: 5 * time.Second,
		})
		return workflow.ExecuteActivity(ctx, activityFunc).Get(ctx, nil)
	}

	// Start worker
	worker := sdkworker.New(s.SdkClient(), tv.TaskQueue().Name, sdkworker.Options{
		TaskQueueActivitiesPerSecond:     workerSetRPS, // worker set RPS should take effect initially
		MaxConcurrentActivityTaskPollers: 10,
		MaxConcurrentWorkflowTaskPollers: 10,
	})
	worker.RegisterActivity(activityFunc)
	worker.RegisterWorkflow(workflowFn)
	s.NoError(worker.Start())
	defer worker.Stop()

	// Launch workflows under workerSetRPS
	for range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
		}, workflowFn)
		s.NoError(err)
	}

	s.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(runTimes) == taskCount
	}, drainTimeout, 100*time.Millisecond, "timeout waiting for activities to complete")

	// Measure duration with workerSetRPS config
	mu.Lock()
	avgRateInitial := getAvgRate(runTimes, workerSetRPS)

	// Reset for API override phase
	runTimes = nil
	mu.Unlock()

	//  Apply API rate limit override workerSetRPS to set the effective RPS to apiSetRPS
	_, err := s.FrontendClient().UpdateTaskQueueConfig(context.Background(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     s.Namespace().String(),
		Identity:      tv.ClientIdentity(),
		TaskQueue:     tv.TaskQueue().Name,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: float32(apiSetRPS)},
			Reason:    "test api override",
		},
	})
	s.NoError(err)

	s.EventuallyWithT(func(c *assert.CollectT) {
		res, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			ReportConfig:  true,
		})
		require.NoError(c, err)
		require.InEpsilon(c, apiSetRPS, res.GetConfig().GetQueueRateLimit().GetRateLimit().GetRequestsPerSecond(), 0.001)
	}, 3*time.Second, 100*time.Millisecond, "DescribeTaskQueue did not reflect override")

	// Launch workflows under API override
	for range taskCount {
		_, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
		}, workflowFn)
		s.NoError(err)
	}

	s.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(runTimes) == taskCount
	}, drainTimeout, 100*time.Millisecond, "timeout waiting for activities to complete")

	// Measure duration with API override
	mu.Lock()
	avgRateOverride := getAvgRate(runTimes, apiSetRPS)
	mu.Unlock()
	s.T().Log("avg rates", avgRateInitial, avgRateOverride)

	// initial rate should be twice as high as the effective RPS is doubled
	s.InEpsilon(workerSetRPS/apiSetRPS, avgRateInitial/avgRateOverride, 0.2, "ratio should be similar")
}

func (s *TaskQueueSuite) TestWholeQueueLimit_TighterThanPerKeyDefault_IsEnforced() {
	if !s.newMatcher {
		s.T().Skip("only for fairness")
	}
	s.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, true)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	const (
		wholeQueueRPS = 4.0  // tighter
		perKeyRPS     = 50.0 // looser than whole queue, should not bind
		tasksPerKey   = 30
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
	s.InEpsilon(wholeQueueRPS, getAvgRate(allTimes, wholeQueueRPS), 0.3)
}

func (s *TaskQueueSuite) TestPerKeyRateLimit_Default_IsEnforcedAcrossThreeKeys() {
	if !s.newMatcher {
		s.T().Skip("only for fairness")
	}
	s.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, true)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	const (
		perKeyRPS     = 4.0 // tighter
		wholeQueueRPS = 1000.0
		tasksPerKey   = 30
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
		s.InEpsilon(perKeyRPS, getAvgRate(times, perKeyRPS), 0.2, "key %s", key)
	}
}

func (s *TaskQueueSuite) TestPerKeyRateLimit_WeightOverride_IsEnforcedAcrossThreeKeys() {
	if !s.newMatcher {
		s.T().Skip("only for fairness")
	}
	s.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, true)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	const (
		perKeyRPS     = 4.0    // base per-key limit
		wholeQueueRPS = 1000.0 // keep high so only per-key gates
		tasksPerKey   = 30
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

		scaledRPS := perKeyRPS * float64(fairnessWeightOverride)
		s.InEpsilon(scaledRPS, getAvgRate(times, scaledRPS), 0.1, "key %s", key)
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
	fairnessKeys := expmaps.Keys(fairnessKeysWithWeight)
	total := len(fairnessKeys) * activitiesPerKey

	// Start workflows with fairness keys (each will schedule one activity)
	for i := range total {
		key := fairnessKeys[i%len(fairnessKeys)]
		weight := fairnessKeysWithWeight[key]
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			WorkflowId:   uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Priority:     &commonpb.Priority{FairnessKey: key, FairnessWeight: weight},
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

				cmd := &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             uuid.NewString(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
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
	perKeyTimes := make(map[string][]time.Time)
	allTimes := make([]time.Time, 0, total)

	actsHandled := 0
	for actsHandled < total {
		if err := ctx.Err(); err != nil {
			s.T().Fatalf("context deadline while draining activity tasks: handled=%d/%d: %v", actsHandled, total, err)
		}
		_, err := s.TaskPoller().PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				key := task.Priority.FairnessKey
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
