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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	// taskQueueStatsSuite encapsulates the test environment and parameters for task queue stats tests.
	taskQueueStatsSuite struct {
		testcore.Env
		t               *testing.T
		usePriMatcher   bool
		minPriority     int
		maxPriority     int
		defaultPriority int
		partitionCount  int
	}

	TaskQueueExpectations struct {
		BacklogCount  int
		MaxExtraTasks int
		CachedEnabled bool
	}

	// TaskQueueExpectationsByType maps task queue types to their expectations
	TaskQueueExpectationsByType map[enumspb.TaskQueueType]TaskQueueExpectations
)

func newTaskQueueStatsSuite(t *testing.T, env testcore.Env, usePriMatcher bool) *taskQueueStatsSuite {
	return &taskQueueStatsSuite{
		Env:             env,
		t:               t,
		usePriMatcher:   usePriMatcher,
		minPriority:     1,
		maxPriority:     5,
		defaultPriority: 3,
		partitionCount:  2, // kept low to reduce test time on CI
	}
}

// TODO(pri): remove once the classic matcher is removed
func TestTaskQueueStats_Classic_Suite(t *testing.T) {
	testcore.MustRunSequential(t, "tests flake when run in parallel")
	t.Parallel()
	runTaskQueueStatsTests(t, false) // usePriMatcher = false
}

func TestTaskQueueStats_Pri_Suite(t *testing.T) {
	testcore.MustRunSequential(t, "tests flake when run in parallel")
	t.Parallel()
	runTaskQueueStatsTests(t, true) // usePriMatcher = true
}

func runTaskQueueStatsTests(t *testing.T, usePriMatcher bool) {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableDeploymentVersions, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingUseNewMatcher, usePriMatcher),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPriorityLevels, 5), // maxPriority
	}

	// Tests WITHOUT RunTestWithMatchingBehavior
	t.Run("TestDescribeTaskQueue_NonRoot", func(t *testing.T) {
		env := testcore.NewEnv(t, baseOpts...)
		s := newTaskQueueStatsSuite(t, env, usePriMatcher)
		s.testDescribeTaskQueueNonRoot()
	})

	t.Run("TestNoTasks_ValidateStats", func(t *testing.T) {
		opts := append(baseOpts,
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 2),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 2),
			testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second),
			testcore.WithDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond),
		)
		env := testcore.NewEnv(t, opts...)
		s := newTaskQueueStatsSuite(t, env, usePriMatcher)
		s.publishConsumeWorkflowTasksValidateStats(0, false)
	})

	t.Run("TestAddMultipleTasks_ValidateStats_Cached", func(t *testing.T) {
		opts := append(baseOpts,
			testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second),
			testcore.WithDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Hour),
		)
		env := testcore.NewEnv(t, opts...)
		s := newTaskQueueStatsSuite(t, env, usePriMatcher)
		s.testAddMultipleTasksValidateStatsCached()
	})

	// Tests WITH RunTestWithMatchingBehavior
	// Note: runWithMatchingBehavior already configures partition count based on forwarding behavior.
	// Do NOT override MatchingNumTaskqueueReadPartitions/WritePartitions inside the subtest.
	t.Run("TestMultipleTasks_WithMatchingBehavior_ValidateStats", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
			s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)
			s.publishConsumeWorkflowTasksValidateStats(4, false)
		})
	})

	t.Run("TestCurrentVersionAbsorbsUnversionedBacklog_NoRamping", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.currentVersionAbsorbsUnversionedBacklogNoRamping()
		})
	})

	t.Run("TestRampingAndCurrentAbsorbUnversionedBacklog", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.rampingAndCurrentAbsorbsUnversionedBacklog()
		})
	})

	t.Run("TestCurrentAbsorbsUnversionedBacklog_WhenRampingToUnversioned", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.currentAbsorbsUnversionedBacklogWhenRampingToUnversioned()
		})
	})

	t.Run("TestRampingAbsorbsUnversionedBacklog_WhenCurrentIsUnversioned", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.rampingAbsorbsUnversionedBacklogWhenCurrentIsUnversioned()
		})
	})

	t.Run("TestInactiveVersionDoesNotAbsorbUnversionedBacklog", func(t *testing.T) {
		runWithMatchingBehavior(t, baseOpts, usePriMatcher, func(s *taskQueueStatsSuite) {
			s.inactiveVersionDoesNotAbsorbUnversionedBacklog()
		})
	})
}

// runWithMatchingBehavior runs a test with all combinations of matching behaviors.
func runWithMatchingBehavior(
	t *testing.T,
	baseOpts []testcore.TestOption,
	usePriMatcher bool,
	subtest func(s *taskQueueStatsSuite),
) {
	for _, behavior := range testcore.AllMatchingBehaviors() {
		t.Run(behavior.Name(), func(t *testing.T) {
			opts := append([]testcore.TestOption{}, baseOpts...)
			opts = append(opts, behavior.Options()...)

			env := testcore.NewEnv(t, opts...)
			behavior.InjectHooks(env)

			s := newTaskQueueStatsSuite(t, env, usePriMatcher)
			subtest(s)
		})
	}
}

func (s *taskQueueStatsSuite) testDescribeTaskQueueNonRoot() {
	resp, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, resp)

	_, err = s.FrontendClient().DescribeTaskQueue(context.Background(),
		&workflowservice.DescribeTaskQueueRequest{
			Namespace:   s.Namespace().String(),
			TaskQueue:   &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ReportStats: true,
		})
	require.ErrorContains(s.t, err, "DescribeTaskQueue stats are only supported for the root partition")
}

func (s *taskQueueStatsSuite) testAddMultipleTasksValidateStatsCached() {
	tqName := "tq-" + common.GenerateRandomString(5)
	s.createDeploymentInTaskQueue(tqName)

	// Enqueue all workflows.
	total := s.enqueueWorkflows(2, tqName)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	// Expect at least *one* of the workflow/activity tasks to be in the stats.
	expectations := TaskQueueExpectations{
		BacklogCount:  1,     // ie at least one task in the backlog
		MaxExtraTasks: total, // ie at most all tasks can be in the backlog
		CachedEnabled: true,
	}

	// Enqueue 1 activity set, to make sure the workflow backlog has some tasks.
	s.enqueueActivitiesForEachWorkflow(1, tqName)

	// Expect the workflow backlog to be non-empty now.
	// This query will cache the stats for the remainder of the test.
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, expectations, false)

	// Enqueue remaining activities.
	s.enqueueActivitiesForEachWorkflow(1, tqName)

	// Poll 2 activities, ie 1 per version, to make sure the activity backlog has some tasks.
	s.pollActivities(2, tqName)

	// Verify activity dispatch rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, false, true)

	// Expect the activity backlog to be non-empty now.
	// This query will cache the stats for the remainder of the test.
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)

	// Poll remaining activities.
	s.pollActivities(total-2, tqName)

	// Despite having polled all the workflows/activies; the stats won't have changed at all since they were cached.
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, expectations, false)
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)
}

func (s *taskQueueStatsSuite) currentVersionAbsorbsUnversionedBacklogNoRamping() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	deploymentName := testcore.RandomizeStr("deployment")
	tqName := "tq-" + common.GenerateRandomString(5)
	currentBuildID := "v1"

	// Register this version in the task queue
	pollerCtx, cancelPoller := context.WithCancel(testcore.NewContext())
	s.createVersionsInTaskQueue(pollerCtx, tqName, deploymentName, currentBuildID)

	// Set current version only (no ramping)
	s.setCurrentVersion(deploymentName, currentBuildID)
	// Stopping the pollers so that we verify the backlog expectations
	cancelPoller()

	// Enqueue unversioned backlog
	unversionedWorkflowCount := 10 * s.partitionCount
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	currentStatsExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should also show the full backlog for this task queue.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			currentBuildID,
			currentStatsExpectation,
		)

		// DescribeTaskQueue Legacy Mode: Since the task queue is part of the current version, the legacy mode should report the total backlog count.
		s.requireLegacyTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeTaskQueue[legacy]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			currentStatsExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)

	// The backlog count for the activity task queue should be equal to the number of activities scheduled since the activity task queue is part of the current version.
	activitesToSchedule := 10 * s.partitionCount
	s.completeWorkflowTasksAndScheduleActivities(tqName, deploymentName, currentBuildID, activitesToSchedule)

	// Verify activity add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, true, false)

	activityStatsExpectation := TaskQueueExpectations{
		BacklogCount:  activitesToSchedule,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// Since the activity task queue is part of the current version,
		// the DescribeWorkerDeploymentVersion should report the backlog count for the activity task queue.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[activity][after-scheduling-activities]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY, // Querying the activity task queue to validate the backlogged activities
			deploymentName,
			currentBuildID,
			activityStatsExpectation,
		)

		// DescribeTaskQueue Legacy Mode: Since the activity task queue is part of the current version, the legacy mode should report the total backlog count.
		s.requireLegacyTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeTaskQueue[legacy][activity]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			activityStatsExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *taskQueueStatsSuite) rampingAndCurrentAbsorbsUnversionedBacklog() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tqName := "tq-" + common.GenerateRandomString(5)
	deploymentName := testcore.RandomizeStr("deployment")
	currentBuildID := "v1"
	rampingBuildID := "v2"

	pollCtx, cancelPoll := context.WithCancel(ctx)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, currentBuildID)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, rampingBuildID)
	cancelPoll() // cancel the pollers so that we can verify the backlog expectations

	// Set ramping version to 30%
	rampPercentage := 30
	s.setRampingVersion(deploymentName, rampingBuildID, rampPercentage)

	// Set current version
	s.setCurrentVersion(deploymentName, currentBuildID)

	// Enqueue unversioned backlog.
	unversionedWorkflowCount := 10 * s.partitionCount
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks: 0,
	}
	rampingExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks: 0,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount,
		MaxExtraTasks: 0,
	}

	// Currently only testing the following API's:
	// - DescribeWorkerDeploymentVersion for the current and ramping versions.
	// - DescribeTaskQueue Legacy Mode for the current and ramping versions.
	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should also show only 70% of the unversioned backlog for this task queue
		// as a ramping version, with ramp set to 30%, exists and absorbs 30% of the unversioned backlog.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[current][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			currentBuildID,
			currentExpectation,
		)

		// DescribeWorkerDeploymentVersion: ramping version should show the remaining 30% of the unversioned backlog for this task queue
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[ramping][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			rampingBuildID,
			rampingExpectation,
		)
		// Since the task queue is part of both the current and ramping versions, the legacy mode should report the total backlog count.
		s.requireLegacyTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeTaskQueue[legacy][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			legacyExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)

	// Here, since the activity task queue is present both in the current and in the ramping version, the backlog count would differ depending on the version described.
	// Poll with BOTH buildIDs in parallel to drain all workflow tasks (hash distribution splits them between current and ramping)
	s.pollWorkflowTasksAndScheduleActivitiesParallel(
		workflowTasksAndActivitiesPollerParams{
			tqName:             tqName,
			deploymentName:     deploymentName,
			buildID:            currentBuildID,
			identity:           "current-version-worker",
			logPrefix:          "current",
			activityIDPrefix:   "activity-current",
			maxToSchedule:      unversionedWorkflowCount,
			maxConsecEmptyPoll: 2,
			versioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
		workflowTasksAndActivitiesPollerParams{
			tqName:             tqName,
			deploymentName:     deploymentName,
			buildID:            rampingBuildID,
			identity:           "ramping-version-worker",
			logPrefix:          "ramping",
			activityIDPrefix:   "activity-ramping",
			maxToSchedule:      unversionedWorkflowCount,
			maxConsecEmptyPoll: 3,
			versioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	)

	// Verify activity add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, true, false)

	// It is important to note that the expected values here are theoretical values based on the ramp percentage. In other words, 70% of the unversioned backlog
	// may not be scheduled on the current version by matching since it makes it's decision based on the workflowID of the workflow. However, when the number of workflows
	// to schedule is high, the expected value of workflows scheduled on the current version will be close to the theoretical value. Here, we shall just be verifying if
	// the theoretical statistics that are being reported are correct.
	activitiesOnCurrentVersionExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks: 0,
	}

	activitiesOnRampingVersionExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// Validate current version activity stats
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[activity][after-scheduling-activities][current-version]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			deploymentName,
			currentBuildID,
			activitiesOnCurrentVersionExpectation,
		)

		// Validate ramping version activity stats
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[activity][after-scheduling-activities][ramping-version]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			deploymentName,
			rampingBuildID,
			activitiesOnRampingVersionExpectation,
		)

	}, 10*time.Second, 200*time.Millisecond)
}

func (s *taskQueueStatsSuite) currentAbsorbsUnversionedBacklogWhenRampingToUnversioned() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	deploymentName := testcore.RandomizeStr("deployment")
	tqName := "tq-" + common.GenerateRandomString(5)
	currentBuildID := "v1"

	pollCtx, cancelPoll := context.WithCancel(ctx)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, currentBuildID)
	cancelPoll() // cancel the pollers so that we can verify the backlog expectations

	// Set current version.
	s.setCurrentVersion(deploymentName, currentBuildID)

	rampPercentage := 20
	s.setRampingVersion(deploymentName, "", rampPercentage)

	// Enqueue unversioned backlog.
	unversionedWorkflowCount := 10 * s.partitionCount
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks: 0,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// There is no way right now for a user to query stats of the "unversioned" version. All we can do in this case
		// is to query the current version's stats and see that it is attributed 80% of the unversioned backlog.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[current][workflow][ramping-to-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			currentBuildID,
			currentExpectation,
		)

		// Since the task queue is part of both the current and ramping versions, the legacy mode should report the total backlog count.
		s.requireLegacyTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeTaskQueue[legacy][workflow][ramping-to-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			legacyExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *taskQueueStatsSuite) rampingAbsorbsUnversionedBacklogWhenCurrentIsUnversioned() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	deploymentName := testcore.RandomizeStr("deployment")
	tqName := "tq-" + common.GenerateRandomString(5)
	rampingBuildID := "v2"

	pollCtx, cancelPoll := context.WithCancel(ctx)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, rampingBuildID)
	cancelPoll() // cancel the pollers so that we can verify the backlog expectations

	// Set current to unversioned (nil current version).
	s.setCurrentVersion(deploymentName, "")

	// Set ramping to a versioned deployment.
	rampPercentage := 30
	s.setRampingVersion(deploymentName, rampingBuildID, rampPercentage)

	// Enqueue unversioned backlog.
	unversionedWorkflowCount := 10 * s.partitionCount
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	rampingExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks: 0,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflowCount,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// We can't query "unversioned" as a WorkerDeploymentVersion, but we can validate that the ramping version
		// is attributed its ramp share of the unversioned backlog.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[ramping][workflow][current-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			rampingBuildID,
			rampingExpectation,
		)

		// Legacy mode should continue to report the total backlog for the task queue.
		s.requireLegacyTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeTaskQueue[legacy][workflow][current-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			legacyExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *taskQueueStatsSuite) inactiveVersionDoesNotAbsorbUnversionedBacklog() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	tqName := "tq-" + common.GenerateRandomString(5)
	deploymentName := testcore.RandomizeStr("deployment")
	currentBuildID := "v1"
	inactiveBuildID := "v2"

	pollCtx, cancelPoll := context.WithCancel(testcore.NewContext())

	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, currentBuildID)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, inactiveBuildID)

	// Set current version
	s.setCurrentVersion(deploymentName, currentBuildID)

	// Stopping the pollers so that we verify the backlog expectations
	cancelPoll()

	// Enqueue unversioned backlog.
	unversionedWorkflows := 10 * s.partitionCount
	s.startUnversionedWorkflows(unversionedWorkflows, tqName)

	// Enqueue pinned workflows.
	pinnedWorkflows := 10 * s.partitionCount
	s.startPinnedWorkflows(pinnedWorkflows, tqName, deploymentName, inactiveBuildID)

	// Verify workflow add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflows,
		MaxExtraTasks: 0,
	}
	inactiveExpectation := TaskQueueExpectations{
		BacklogCount:  pinnedWorkflows,
		MaxExtraTasks: 0,
	}

	// Currently only testing the following API's:
	// - DescribeWorkerDeploymentVersion
	// - DescribeTaskQueue Legacy Mode
	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should should show 100% of the unversioned backlog for this task queue
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[current][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			currentBuildID,
			currentExpectation,
		)

		// DescribeWorkerDeploymentVersion: inactive version should only show the pinned workflows that are scheduled on it.
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[inactive][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			inactiveBuildID,
			inactiveExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)

	// Polling the workflow tasks and scheduling activities
	s.pollWorkflowTasksAndScheduleActivitiesParallel(
		workflowTasksAndActivitiesPollerParams{
			tqName:             tqName,
			deploymentName:     deploymentName,
			buildID:            currentBuildID,
			identity:           "current-version-worker",
			logPrefix:          "current",
			activityIDPrefix:   "activity-current",
			maxToSchedule:      unversionedWorkflows,
			maxConsecEmptyPoll: 2,
			versioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
		workflowTasksAndActivitiesPollerParams{
			tqName:             tqName,
			deploymentName:     deploymentName,
			buildID:            inactiveBuildID,
			identity:           "inactive-version-worker",
			logPrefix:          "inactive",
			activityIDPrefix:   "activity-inactive",
			maxToSchedule:      pinnedWorkflows,
			maxConsecEmptyPoll: 2,
			versioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
		},
	)

	// Verify workflow dispatch rate and activity add rate
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, true)
	s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, true, false)

	// Validate activity backlogs
	currentActivityExpectation := TaskQueueExpectations{
		BacklogCount:  unversionedWorkflows,
		MaxExtraTasks: 0,
	}
	inactiveActivityExpectation := TaskQueueExpectations{
		BacklogCount:  pinnedWorkflows,
		MaxExtraTasks: 0,
	}
	workflowTaskQueueEmptyExpectation := TaskQueueExpectations{
		BacklogCount:  0,
		MaxExtraTasks: 0,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		// The activity task queue of the current version should have the backlog count for the activities that were scheduled
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[current][activity]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			deploymentName,
			currentBuildID,
			currentActivityExpectation,
		)

		// The workflow task queue of the current version should be empty since activities were scheduled
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[current][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			currentBuildID,
			workflowTaskQueueEmptyExpectation,
		)

		// The workflow task queue of the inactive version should be empty since activities were scheduled
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[inactive][workflow]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			deploymentName,
			inactiveBuildID,
			workflowTaskQueueEmptyExpectation,
		)

		// The activity task queue of the inactive version should have the backlog count for the activities that were scheduled
		s.requireWDVTaskQueueStatsRelaxed(
			ctx,
			a,
			"DescribeWorkerDeploymentVersion[inactive][activity]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			deploymentName,
			inactiveBuildID,
			inactiveActivityExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

// requireWDVTaskQueueStatsRelaxed asserts task queue statistics by allowing for over-counting in multi-partition ramping scenarios.
// The production code intentionally uses math.Ceil for both ramping and current percentage
// calculations across partitions, which can result in slight over-counting.
func (s *taskQueueStatsSuite) requireWDVTaskQueueStatsRelaxed(
	ctx context.Context,
	a *require.Assertions,
	label string,
	tqName string,
	tqType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
	expectation TaskQueueExpectations,
) {
	stats, found, err := s.describeWDVTaskQueueStats(ctx, tqName, tqType, deploymentName, buildID)
	a.NoError(err)
	a.True(found, "expected %s task queue %s in DescribeWorkerDeploymentVersion response", tqType, tqName)
	a.NotNil(stats, "expected %s task queue %s to have stats in DescribeWorkerDeploymentVersion response", tqType, tqName)

	// Use the existing validateTaskQueueStats with MaxExtraTasks set to numPartitions
	// to account for ceiling operations across partitions
	expectation.MaxExtraTasks = s.partitionCount
	validateTaskQueueStats(label, a, stats, expectation)
}

// requireLegacyTaskQueueStatsRelaxed asserts task queue statistics by allowing for over-counting in multi-partition scenarios.
// The production code intentionally uses math.Ceil for both ramping and current percentage calculations across partitions,
// which can result in slight over-counting.
func (s *taskQueueStatsSuite) requireLegacyTaskQueueStatsRelaxed(
	ctx context.Context,
	a *require.Assertions,
	label string,
	tqName string,
	tqType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
) {
	stats, found, err := s.describeLegacyTaskQueueStats(ctx, tqName, tqType)
	a.NoError(err)
	a.True(found, "expected %s task queue %s in DescribeTaskQueue response", tqType, tqName)
	a.NotNil(stats, "expected %s task queue %s to have stats in DescribeTaskQueue response", tqType, tqName)

	// Use the existing validateTaskQueueStats with MaxExtraTasks set to numPartitions
	// to account for ceiling operations across partitions
	expectation.MaxExtraTasks = s.partitionCount
	validateTaskQueueStats(label, a, stats, expectation)
}

// Publishes versioned and unversioned entities; with one entity per priority (plus default priority). Multiplied by `sets`.
func (s *taskQueueStatsSuite) publishConsumeWorkflowTasksValidateStats(sets int, singlePartition bool) {
	tqName := "tq-" + common.GenerateRandomString(5)
	s.createDeploymentInTaskQueue(tqName)

	// verify both workflow and activity backlogs are empty
	expectations := TaskQueueExpectationsByType{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW: {
			BacklogCount:  0,
			MaxExtraTasks: 0,
		},
		enumspb.TASK_QUEUE_TYPE_ACTIVITY: {
			BacklogCount:  0,
			MaxExtraTasks: 0,
		},
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)

	// Actual counter can be greater than the expected due to History->Matching retries.
	// We make sure the counter is in range [expected, expected+maxExtraTasksAllowed].
	maxExtraTasksAllowed := 3
	if sets == 0 {
		maxExtraTasksAllowed = 0
	}

	// enqueue workflows
	total := s.enqueueWorkflows(sets, tqName)

	// verify workflow add rate
	if sets > 0 {
		s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false)
	}

	// verify workflow backlog is not empty, activity backlog is empty
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:  total,
		MaxExtraTasks: maxExtraTasksAllowed,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)

	// poll all workflow tasks and enqueue one activity task for each workflow
	totalAct := s.enqueueActivitiesForEachWorkflow(sets, tqName)
	require.Equal(s.t, total, totalAct, "should have enqueued the same number of activities as workflows")

	// verify workflow dispatch rate and activity add rate
	if sets > 0 {
		s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, false, true)
		s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, true, false)
	}

	// verify workflow backlog is empty, activity backlog is not
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:  0,
		MaxExtraTasks: maxExtraTasksAllowed,
	}
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:  total,
		MaxExtraTasks: maxExtraTasksAllowed,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)

	// poll all activity tasks
	s.pollActivities(total, tqName)

	// verify activity dispatch rate
	if sets > 0 {
		s.validateRates(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, false, true)
	}

	// verify both workflow and activity backlogs are empty
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:  0,
		MaxExtraTasks: maxExtraTasksAllowed,
	}
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:  0,
		MaxExtraTasks: maxExtraTasksAllowed,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)
}

func (s *taskQueueStatsSuite) startUnversionedWorkflows(count int, tqName string) {
	wt := "functional-workflow-current-absorbs-unversioned"
	workflowType := &commonpb.WorkflowType{Name: wt}
	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:             s.Namespace().String(),
		WorkflowType:          workflowType,
		TaskQueue:             &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:    durationpb.New(10 * time.Minute),
		WorkflowTaskTimeout:   durationpb.New(10 * time.Minute),
		RequestId:             uuid.NewString(),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	for range count {
		request.WorkflowId = uuid.NewString() // starting "count" different Unversioned workflows.
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		require.NoError(s.t, err)
	}
}

func (s *taskQueueStatsSuite) startPinnedWorkflows(count int, tqName string, deploymentName string, buildID string) {
	wt := "functional-workflow-pinned"
	workflowType := &commonpb.WorkflowType{Name: wt}

	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:             s.Namespace().String(),
		WorkflowType:          workflowType,
		TaskQueue:             &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:    durationpb.New(10 * time.Minute),
		WorkflowTaskTimeout:   durationpb.New(10 * time.Minute),
		RequestId:             uuid.NewString(),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		VersioningOverride: &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version: &deploymentpb.WorkerDeploymentVersion{
						BuildId:        buildID,
						DeploymentName: deploymentName,
					},
				},
			},
		},
	}
	for range count {
		request.WorkflowId = uuid.NewString() // starting "n" different Pinned workflows.
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		require.NoError(s.t, err)
	}
}

type workflowTasksAndActivitiesPollerParams struct {
	tqName             string
	deploymentName     string
	buildID            string
	identity           string
	logPrefix          string
	activityIDPrefix   string
	maxToSchedule      int
	maxConsecEmptyPoll int
	versioningBehavior enumspb.VersioningBehavior
}

// pollWorkflowTasksAndScheduleActivitiesParallel polls workflow tasks and schedules activities in parallel for workers of two different buildID's.
func (s *taskQueueStatsSuite) pollWorkflowTasksAndScheduleActivitiesParallel(params ...workflowTasksAndActivitiesPollerParams) {
	var wg sync.WaitGroup
	errCh := make(chan error, len(params))

	for _, p := range params {
		wg.Go(func() {
			_, err := s.pollWorkflowTasksAndScheduleActivities(p)
			errCh <- err
		})
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(s.t, err)
	}
}

func (s *taskQueueStatsSuite) pollWorkflowTasksAndScheduleActivities(params workflowTasksAndActivitiesPollerParams) (int, error) {
	deploymentOpts := createDeploymentOptions(params.deploymentName, params.buildID)

	scheduled := 0
	emptyPollCount := 0
	for i := 0; i < params.maxToSchedule; {
		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: params.tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          params.identity,
			DeploymentOptions: deploymentOpts,
		})
		if err != nil {
			return scheduled, err
		}
		if resp == nil || resp.GetAttempt() < 1 {
			emptyPollCount++
			fmt.Printf("[%s] Empty poll %d/%d. Scheduled %d activities so far\n", params.logPrefix, emptyPollCount, params.maxConsecEmptyPoll, scheduled)
			if emptyPollCount >= params.maxConsecEmptyPoll {
				fmt.Printf("[%s] Stopping after %d empty polls. Scheduled %d activities\n", params.logPrefix, params.maxConsecEmptyPoll, scheduled)
				return scheduled, nil
			}
			continue
		}

		emptyPollCount = 0

		respondReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: resp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:            fmt.Sprintf("%s-%d", params.activityIDPrefix, i),
							ActivityType:          &commonpb.ActivityType{Name: "activity_type1"},
							TaskQueue:             &taskqueuepb.TaskQueue{Name: params.tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
							StartToCloseTimeout:   durationpb.New(time.Minute),
							RequestEagerExecution: false,
						},
					},
				},
			},
			VersioningBehavior: params.versioningBehavior,
			DeploymentOptions:  deploymentOpts,
		}
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), respondReq)
		if err != nil {
			return scheduled, err
		}
		scheduled++
		fmt.Printf("[%s] Scheduled activity %d\n", params.logPrefix, scheduled)
		i++
	}

	return scheduled, nil
}

func (s *taskQueueStatsSuite) completeWorkflowTasksAndScheduleActivities(
	tqName string,
	deploymentName string,
	buildID string,
	activityCount int,
) {
	deploymentOpts := createDeploymentOptions(deploymentName, buildID)

	for i := 0; i < activityCount; {
		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "current-version-worker",
			DeploymentOptions: deploymentOpts,
		})
		require.NoError(s.t, err)
		if resp == nil || resp.GetAttempt() < 1 {
			fmt.Println("Empty poll! Continuing...")
			continue
		}

		// Note: Scheduling activities with no VersioningBehaviour only for the purpose of this test so that we can validate
		// if unversioned activity tasks are considered part of the backlog for the activity task queue in a current version.
		respondReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: resp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:            fmt.Sprintf("activity-%d", i),
							ActivityType:          &commonpb.ActivityType{Name: "activity_type1"},
							TaskQueue:             &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
							StartToCloseTimeout:   durationpb.New(time.Minute),
							RequestEagerExecution: false,
						},
					},
				},
			},
			VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			DeploymentOptions:  deploymentOpts,
		}

		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), respondReq)
		require.NoError(s.t, err)
		i++
	}
}

// TODO (Shivam): We may have to wait for the propagation status to show completed if we are using async workflows here.
func (s *taskQueueStatsSuite) setCurrentVersion(deploymentName, buildID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: deploymentName,
		BuildId:        buildID,
	})
	require.NoError(s.t, err)
}

// TODO (Shivam): We may have to wait for the propagation status to show completed if we are using async workflows here.
func (s *taskQueueStatsSuite) setRampingVersion(deploymentName, buildID string, rampPercentage int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: deploymentName,
		BuildId:        buildID,
		Percentage:     float32(rampPercentage),
	})
	require.NoError(s.t, err)
}

func (s *taskQueueStatsSuite) describeWDVTaskQueueStats(
	ctx context.Context,
	tqName string,
	tqType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
) (stats *taskqueuepb.TaskQueueStats, found bool, err error) {
	resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        buildID,
		},
		ReportTaskQueueStats: true,
	})
	if err != nil {
		return nil, false, err
	}
	for _, tq := range resp.GetVersionTaskQueues() {
		if tq.GetName() == tqName && tq.GetType() == tqType {
			return tq.GetStats(), true, nil
		}
	}
	return nil, false, nil
}

// DescribeTaskQueue Legacy Mode shall report the stats for this task queue from all the different versions
// that the task queue is part of.
func (s *taskQueueStatsSuite) describeLegacyTaskQueueStats(
	ctx context.Context,
	tqName string,
	tqType enumspb.TaskQueueType,
) (stats *taskqueuepb.TaskQueueStats, found bool, err error) {
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: tqType,
		ReportStats:   true,
	})
	if err != nil {
		return nil, false, err
	}
	return resp.GetStats(), true, nil
}

func (s *taskQueueStatsSuite) enqueueWorkflows(sets int, tqName string) int {
	deploymentOpts := s.deploymentOptions(tqName)

	var total int
	for version := range 2 { // 0=unversioned, 1=versioned
		for priority := 0; priority <= s.maxPriority; priority++ {
			for range sets {
				wt := "functional-workflow-multiple-tasks"
				workflowType := &commonpb.WorkflowType{Name: wt}

				request := &workflowservice.StartWorkflowExecutionRequest{
					Namespace:             s.Namespace().String(),
					WorkflowId:            uuid.NewString(),
					WorkflowType:          workflowType,
					TaskQueue:             &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                 nil,
					WorkflowRunTimeout:    durationpb.New(10 * time.Minute),
					WorkflowTaskTimeout:   durationpb.New(10 * time.Minute),
					RequestId:             uuid.NewString(),
					WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
					Priority:              &commonpb.Priority{PriorityKey: int32(priority)},
				}

				if version == 1 {
					request.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_Pinned{
							Pinned: &workflowpb.VersioningOverride_PinnedOverride{
								Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
								Version: &deploymentpb.WorkerDeploymentVersion{
									BuildId:        deploymentOpts.BuildId,
									DeploymentName: deploymentOpts.DeploymentName,
								},
							},
						},
					}
				}

				_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
				require.NoError(s.t, err)

				total++
			}
		}
	}

	s.t.Logf("Enqueued %d workflows", total)
	return total
}

func (s *taskQueueStatsSuite) createVersionsInTaskQueue(ctx context.Context, tqName string, deploymentName string, buildID string) {
	go func() {
		_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: createDeploymentOptions(deploymentName, buildID),
		})
	}()

	go func() {
		_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: createDeploymentOptions(deploymentName, buildID),
		})
	}()

	// Wait for the version to be created.
	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        buildID,
			},
		})
		a.NoError(err)
		a.NotNil(resp)
	}, 10*time.Second, 200*time.Millisecond)
}

// TODO (Shivam): Remove this guy.
func (s *taskQueueStatsSuite) createDeploymentInTaskQueue(tqName string) {
	// Using old DeploymentData format
	var wg sync.WaitGroup

	wg.Go(func() {
		_, _ = s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.deploymentOptions(tqName),
		})
	})

	wg.Go(func() {
		_, _ = s.FrontendClient().PollActivityTaskQueue(testcore.NewContext(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.deploymentOptions(tqName),
		})
	})

	wg.Wait()
}

func (s *taskQueueStatsSuite) enqueueActivitiesForEachWorkflow(sets int, tqName string) int {
	deploymentOpts := s.deploymentOptions(tqName)

	var total int
	for version := range 2 { // 0=unversioned, 1=versioned
		for priority := 0; priority <= s.maxPriority; priority++ {
			for i := 0; i < sets; { // not counting up here to allow for retries
				pollReq := &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: s.Namespace().String(),
					TaskQueue: &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				}
				if version == 1 {
					pollReq.DeploymentOptions = deploymentOpts
				}

				resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), pollReq)
				require.NoError(s.t, err)
				if resp == nil || resp.GetAttempt() < 1 {
					continue
				}

				respondReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: resp.TaskToken,
					Commands: []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
							Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
								ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
									ActivityId:            "activity1",
									ActivityType:          &commonpb.ActivityType{Name: "activity_type1"},
									TaskQueue:             &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
									StartToCloseTimeout:   durationpb.New(time.Minute),
									RequestEagerExecution: false,
									// Priority is inherted from the workflow
								},
							},
						},
					},
				}

				if version == 1 {
					respondReq.DeploymentOptions = deploymentOpts
					respondReq.VersioningBehavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				}

				_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), respondReq)
				require.NoError(s.t, err)

				i++
				total++
			}
		}
	}
	s.t.Logf("Enqueued %d activities", total)
	return total
}

func (s *taskQueueStatsSuite) pollActivities(count int, tqName string) {
	for i := 0; i < count; {
		pollReq := &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tqName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		}
		if i%2 == 0 {
			pollReq.DeploymentOptions = s.deploymentOptions(tqName)
		}

		resp, err := s.FrontendClient().PollActivityTaskQueue(
			testcore.NewContext(), pollReq,
		)
		require.NoError(s.t, err)
		if resp == nil || resp.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
	}
	s.t.Logf("Polled %d activities", count)
}

func (s *taskQueueStatsSuite) validateAllTaskQueueStats(
	tqName string,
	expectations TaskQueueExpectationsByType,
	singlePartition bool,
) {
	for tqType, expectation := range expectations {
		s.validateTaskQueueStatsByType(tqName, tqType, expectation, singlePartition)
	}
}

// validateRates verifies TasksAddRate and/or TasksDispatchRate in a dedicated EventuallyWithT block.
// This should be called immediately after the relevant operation (enqueue for add rate, poll for dispatch rate)
// to ensure the rate is checked while still fresh (before the 30-second sliding window decays).
func (s *taskQueueStatsSuite) validateRates(
	tqName string,
	tqType enumspb.TaskQueueType,
	expectAddRate bool,
	expectDispatchRate bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: tqType,
		ReportStats:   true,
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)
		label := "validateRates[" + tqType.String() + "]"

		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, req)
		a.NoError(err)
		a.NotNil(resp)
		a.NotNil(resp.Stats)

		if expectAddRate {
			a.Greater(resp.Stats.TasksAddRate, float32(0),
				"%s: TasksAddRate should be > 0, got %f", label, resp.Stats.TasksAddRate)
		}
		if expectDispatchRate {
			a.Greater(resp.Stats.TasksDispatchRate, float32(0),
				"%s: TasksDispatchRate should be > 0, got %f", label, resp.Stats.TasksDispatchRate)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *taskQueueStatsSuite) validateTaskQueueStatsByType(
	tqName string,
	tqType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
	singlePartition bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.validateDescribeTaskQueueWithDefaultMode(ctx, tqName, tqType, expectation, singlePartition)

	// The other two methods report *either* versioned or unversioned stats, so we need to
	// half the expectations to account for that.
	halfExpectation := expectation
	halfExpectation.BacklogCount /= 2
	halfExpectation.MaxExtraTasks /= 2

	s.validateDescribeTaskQueueWithEnhancedMode(ctx, tqName, tqType, halfExpectation)
	s.validateDescribeWorkerDeploymentVersion(ctx, tqName, tqType, halfExpectation)
}

func (s *taskQueueStatsSuite) validateDescribeTaskQueueWithDefaultMode(
	ctx context.Context,
	tqName string,
	tqType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
	singlePartition bool,
) {
	req := &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: tqType,
	}

	// test stats are not reported by default (and therefore also not cached)
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, req)
	require.NoError(s.t, err)
	require.NotNil(s.t, resp)
	require.Nil(s.t, resp.Stats, "stats should not be reported by default")
	//nolint:staticcheck // SA1019 deprecated
	require.Nil(s.t, resp.TaskQueueStatus, "status should not be reported by default")

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)
		label := "DescribeTaskQueue_DefaultMode[" + tqType.String() + "]"

		req.ReportStats = true
		//nolint:staticcheck // SA1019 deprecated
		req.IncludeTaskQueueStatus = true
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, req)
		a.NoError(err)
		a.NotNil(resp)

		if singlePartition {
			expected := expectation.BacklogCount / 2 // only reports unversioned
			//nolint:staticcheck // SA1019 deprecated field
			actual := resp.TaskQueueStatus.GetBacklogCountHint()
			a.EqualValuesf(expected, actual, "%s: backlog hint should be %d, got %d", label, expected, actual)
		}

		validateTaskQueueStats(label, a, resp.Stats, expectation)
		if s.usePriMatcher && expectation.BacklogCount > 0 {
			// Per priority stats are only available with the priority matcher and when they've been actively used.
			s.validateTaskQueueStatsByPriority(label, a, resp.StatsByPriorityKey, expectation)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *taskQueueStatsSuite) validateDescribeTaskQueueWithEnhancedMode(
	ctx context.Context,
	tqName string,
	tqType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
) {
	deploymentOpts := s.deploymentOptions(tqName)
	req := &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:   enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions: &taskqueuepb.TaskQueueVersionSelection{
			BuildIds: []string{worker_versioning.WorkerDeploymentVersionToStringV32(
				&deploymentspb.WorkerDeploymentVersion{
					BuildId:        deploymentOpts.BuildId,
					DeploymentName: deploymentOpts.DeploymentName,
				})},
			Unversioned: true,
		},
		TaskQueueTypes: []enumspb.TaskQueueType{tqType},
		ReportPollers:  true,
	}

	if !expectation.CachedEnabled { // skip if testing caching; as this would pin the result to the cache
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, req)
		require.NoError(s.t, err)
		require.NotNil(s.t, resp)
		require.Nil(s.t, resp.Stats, "stats should not be reported by default")
		//nolint:staticcheck // SA1019 deprecated
		require.Nil(s.t, resp.TaskQueueStatus, "status should not be reported")
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		req.ReportStats = true
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, req)
		a.NoError(err)
		a.NotNil(resp)

		//nolint:staticcheck // SA1019 deprecated
		a.Equal(2, len(resp.GetVersionsInfo()), "should be 2: 1 default/unversioned + 1 versioned")
		//nolint:staticcheck // SA1019 deprecated
		for _, v := range resp.GetVersionsInfo() {
			a.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, v.GetTaskReachability())

			info := v.GetTypesInfo()[int32(tqType)]
			a.NotNil(info, "should have info for task queue type %s", tqType)
			if info == nil {
				return
			}
			a.NotNil(info.Stats, "should have stats for task queue type %s", tqType)
			if info.Stats == nil {
				return
			}

			validateTaskQueueStats("DescribeTaskQueue_EnhancedMode["+tqType.String()+"]", a, info.Stats, expectation)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *taskQueueStatsSuite) validateDescribeWorkerDeploymentVersion(
	ctx context.Context,
	tqName string,
	tqType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
) {
	deploymentOpts := s.deploymentOptions(tqName)
	req := &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			BuildId:        deploymentOpts.BuildId,
			DeploymentName: deploymentOpts.DeploymentName,
		},
	}

	// test stats are not reported by default (and therefore also not cached)
	resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, req)
	require.NoError(s.t, err)
	require.NotNil(s.t, resp)
	for _, info := range resp.VersionTaskQueues {
		require.Nil(s.t, info.Stats, "stats should not be reported by default")
	}

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		a := require.New(c)

		req.ReportTaskQueueStats = true
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, req)
		require.NoError(s.t, err)
		a.Len(resp.VersionTaskQueues, 2, "should be 1 task queue for Workflows and 1 for Activities")

		for _, info := range resp.VersionTaskQueues {
			if info.Name == tqName || info.Type == tqType {
				label := "DescribeWorkerDeploymentVersion[" + tqType.String() + "]"
				validateTaskQueueStats(label, a, info.Stats, expectation)
				if s.usePriMatcher && expectation.BacklogCount > 0 {
					// Per priority stats are only available with the priority matcher and when they've been actively used.
					s.validateTaskQueueStatsByPriority(label, a, info.StatsByPriorityKey, expectation)
				}
				return
			}
		}
		a.Failf("Task queue %s of type %s not found in response", tqName, tqType)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *taskQueueStatsSuite) validateTaskQueueStatsByPriority(
	label string,
	a *require.Assertions,
	stats map[int32]*taskqueuepb.TaskQueueStats,
	taskQueueExpectation TaskQueueExpectations,
) {
	a.Len(stats, s.maxPriority, "%s: stats should contain %d priorities", label, s.maxPriority)

	// use an abgridged version when caching since the exact stats are difficult to predict
	if taskQueueExpectation.CachedEnabled {
		for i := int32(s.minPriority); i <= int32(s.maxPriority); i++ {
			if stats[i].ApproximateBacklogCount != 0 && stats[i].TasksDispatchRate > 0 || stats[i].TasksAddRate > 0 {
				return
			}
		}
		a.Failf("%s: should have found at least one non-zero backlog count with any non-zero rate across priorities", label)
	}

	var accBacklogCount int
	for i := int32(s.minPriority); i <= int32(s.maxPriority); i++ {
		priExpectation := taskQueueExpectation
		priExpectation.BacklogCount = taskQueueExpectation.BacklogCount / (s.maxPriority + 1)
		if i == int32(s.defaultPriority) {
			priExpectation.BacklogCount *= 2 // zero priority translates to default priority 3
		}

		a.Containsf(stats, i, "%s: stats should contain priority %d", label, i)
		validateTaskQueueStats(fmt.Sprintf("%s_Pri[%d]", label, i), a, stats[i], priExpectation)
		accBacklogCount += int(stats[i].ApproximateBacklogCount)
	}
	a.GreaterOrEqualf(taskQueueExpectation.BacklogCount, accBacklogCount,
		"%s: accumulated backlog count from all priorities should be at least %d, got %d",
		label, taskQueueExpectation.BacklogCount, accBacklogCount)
}

func validateTaskQueueStatsStrict(
	label string,
	a *require.Assertions,
	stats *taskqueuepb.TaskQueueStats,
	expectation TaskQueueExpectations,
) {
	a.Equal(int64(expectation.BacklogCount), stats.ApproximateBacklogCount,
		"%s: ApproximateBacklogCount should be %d, got %d",
		label, expectation.BacklogCount, stats.ApproximateBacklogCount)

	a.Equal(stats.ApproximateBacklogCount == 0, stats.ApproximateBacklogAge.AsDuration() == time.Duration(0),
		"%s: ApproximateBacklogAge should be 0 when ApproximateBacklogCount is 0, got %s",
		label, stats.ApproximateBacklogAge.AsDuration())
}

func validateTaskQueueStats(
	label string,
	a *require.Assertions,
	stats *taskqueuepb.TaskQueueStats,
	expectation TaskQueueExpectations,
) {
	// Actual counter can be greater than the expected due to history retries. We make sure the counter is in
	// range [expected, expected+maxBacklogExtraTasks]
	a.GreaterOrEqual(stats.ApproximateBacklogCount, int64(expectation.BacklogCount),
		"%s: ApproximateBacklogCount should be at least %d, got %d",
		label, expectation.BacklogCount, stats.ApproximateBacklogCount)

	maxApproximateBacklogCount := int64(expectation.BacklogCount + expectation.MaxExtraTasks)
	a.LessOrEqual(stats.ApproximateBacklogCount, maxApproximateBacklogCount,
		"%s: ApproximateBacklogCount should be at most %d, got %d",
		label, maxApproximateBacklogCount, stats.ApproximateBacklogCount)

	a.Equal(stats.ApproximateBacklogCount == 0, stats.ApproximateBacklogAge.AsDuration() == time.Duration(0),
		"%s: ApproximateBacklogAge should be 0 when ApproximateBacklogCount is 0, got %s",
		label, stats.ApproximateBacklogAge.AsDuration())
}

// TODO: Remove this once older stats tests are refactored to use the createDeploymentOptions function.
func (s *taskQueueStatsSuite) deploymentOptions(tqName string) *deploymentpb.WorkerDeploymentOptions {
	return &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       tqName + "-deployment",
		BuildId:              "build-id",
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
}

func createDeploymentOptions(deploymentName string, buildID string) *deploymentpb.WorkerDeploymentOptions {
	return &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       deploymentName,
		BuildId:              buildID,
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
}
