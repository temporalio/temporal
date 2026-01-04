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
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	minPriority     = 1
	maxPriority     = 5
	defaultPriority = 3
)

type (
	// TaskQueueStatsSuite tests are querying task queue stats.
	//
	// There are currently three ways to do that:
	// 1. DescribeTaskQueue with ReportStats=true
	// 2. DescribeTaskQueue with ApiMode=ENHANCED and ReportStats=true [deprecated]
	// 3. DescribeWorkerDeploymentVersion with ReportTaskQueueStats=true
	//
	// Unless a test calls out a specific methods, all three methods are tested in each test case.
	TaskQueueStatsSuite struct {
		testcore.FunctionalTestBase
		usePriMatcher        bool
		useNewDeploymentData bool
	}

	TaskQueueExpectations struct {
		BacklogCount     int
		MaxExtraTasks    int
		ExpectedAddRate  bool
		ExpectedDispatch bool
		CachedEnabled    bool
	}

	// TaskQueueExpectationsByType maps task queue types to their expectations
	TaskQueueExpectationsByType map[enumspb.TaskQueueType]TaskQueueExpectations
)

// TODO(pri): remove once the classic matcher is removed
func TestTaskQueueStats_Classic_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &TaskQueueStatsSuite{usePriMatcher: false})
}

func TestTaskQueueStats_Pri_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &TaskQueueStatsSuite{usePriMatcher: true})
}

func (s *TaskQueueStatsSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.EnableDeploymentVersions, true)
	s.OverrideDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true)
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, s.usePriMatcher)
	s.OverrideDynamicConfig(dynamicconfig.MatchingPriorityLevels, maxPriority)
}

func (s *TaskQueueStatsSuite) TestDescribeTaskQueue_NonRoot() {
	resp, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(resp)

	_, err = s.FrontendClient().DescribeTaskQueue(context.Background(),
		&workflowservice.DescribeTaskQueueRequest{
			Namespace:   s.Namespace().String(),
			TaskQueue:   &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ReportStats: true,
		})
	s.ErrorContains(err, "DescribeTaskQueue stats are only supported for the root partition")
}

func (s *TaskQueueStatsSuite) TestNoTasks_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	s.publishConsumeWorkflowTasksValidateStats(0, false)
}

func (s *TaskQueueStatsSuite) TestSingleTask_SinglePartition_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingUpdateAckInterval, 5*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	s.publishConsumeWorkflowTasksValidateStats(1, true)
}

func (s *TaskQueueStatsSuite) TestMultipleTasks_MultiplePartitions_WithMatchingBehavior_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	s.RunTestWithMatchingBehavior(func() {
		s.publishConsumeWorkflowTasksValidateStats(4, false)
	})
}

// NOTE: Cache _eviction_ is already covered by the other tests.
func (s *TaskQueueStatsSuite) TestAddMultipleTasks_MultiplePartitions_ValidateStats_Cached() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Hour) // using a long TTL to verify caching

	tqName := testcore.RandomizeStr("backlog-counter-task-queue")
	s.createDeploymentInTaskQueue(tqName)

	// Enqueue all workflows.
	total := s.enqueueWorkflows(2, tqName)

	// Expect at least *one* of the workflow/activity tasks to be in the stats.
	expectations := TaskQueueExpectations{
		BacklogCount:     1,     // ie at least one task in the backlog
		MaxExtraTasks:    total, // ie at most all tasks can be in the backlog
		ExpectedAddRate:  true,
		ExpectedDispatch: true,
		CachedEnabled:    true,
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

	// Expect the activity backlog to be non-empty now.
	// This query will cache the stats for the remainder of the test.
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)

	// Poll remaining activities.
	s.pollActivities(total-2, tqName)

	// Despite having polled all the workflows/activies; the stats won't have changed at all since they were cached.
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, expectations, false)
	s.validateTaskQueueStatsByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)
}

func (s *TaskQueueStatsSuite) TestCurrentVersionAbsorbsUnversionedBacklog_NoRamping_SinglePartition() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.currentVersionAbsorbsUnversionedBacklogNoRamping(1)
}

func (s *TaskQueueStatsSuite) TestCurrentVersionAbsorbsUnversionedBacklog_NoRamping_MultiplePartitions() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

	s.RunTestWithMatchingBehavior(func() {
		s.currentVersionAbsorbsUnversionedBacklogNoRamping(4)
	})
}

func (s *TaskQueueStatsSuite) currentVersionAbsorbsUnversionedBacklogNoRamping(numPartitions int) {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tqName := testcore.RandomizeStr("tq-current-absorbs-unversioned")
	deploymentName := tqName + "-deployment"
	currentBuildID := "current-build-id"

	// Register this version in the task queue
	pollerCtx, cancelPoller := context.WithCancel(testcore.NewContext())
	s.createVersionsInTaskQueue(pollerCtx, tqName, deploymentName, currentBuildID)

	// Set current version only (no ramping)
	s.setCurrentVersion(deploymentName, currentBuildID)
	// Stopping the pollers so that we verify the backlog expectations
	cancelPoller()

	// Enqueue unversioned backlog
	unversionedWorkflowCount := 10 * numPartitions
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	currentStatsExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should also show the full backlog for this task queue.
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireLegacyTaskQueueStatsStrict(
			ctx,
			a,
			"DescribeTaskQueue[legacy]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			currentStatsExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)

	// The backlog count for the activity task queue should be equal to the number of activities scheduled since the activity task queue is part of the current version.
	activitesToSchedule := 10 * numPartitions
	s.completeWorkflowTasksAndScheduleActivities(tqName, deploymentName, currentBuildID, activitesToSchedule)

	activityStatsExpectation := TaskQueueExpectations{
		BacklogCount:     activitesToSchedule,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// Since the activity task queue is part of the current version,
		// the DescribeWorkerDeploymentVersion should report the backlog count for the activity task queue.
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireLegacyTaskQueueStatsStrict(
			ctx,
			a,
			"DescribeTaskQueue[legacy][activity]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			activityStatsExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *TaskQueueStatsSuite) TestRampingAndCurrentAbsorbUnversionedBacklog_SinglePartition() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.rampingAndCurrentAbsorbsUnversionedBacklog(1)
}

func (s *TaskQueueStatsSuite) TestRampingAndCurrentAbsorbUnversionedBacklog_MultiplePartitions() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

	s.RunTestWithMatchingBehavior(func() {
		s.rampingAndCurrentAbsorbsUnversionedBacklog(4)
	})
}

func (s *TaskQueueStatsSuite) TestCurrentAbsorbsUnversionedBacklog_WhenRampingToUnversioned_SinglePartition() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.currentAbsorbsUnversionedBacklogWhenRampingToUnversioned(1)
}

func (s *TaskQueueStatsSuite) TestCurrentAbsorbsUnversionedBacklog_WhenRampingToUnversioned_MultiplePartitions() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

	s.RunTestWithMatchingBehavior(func() {
		s.currentAbsorbsUnversionedBacklogWhenRampingToUnversioned(4)
	})
}

func (s *TaskQueueStatsSuite) currentAbsorbsUnversionedBacklogWhenRampingToUnversioned(numPartitions int) {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tqName := testcore.RandomizeStr("ramping-unversioned")
	deploymentName := tqName + "-deployment"
	currentBuildID := "current-build-id"

	pollCtx, cancelPoll := context.WithCancel(ctx)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, currentBuildID)
	cancelPoll() // cancel the pollers so that we can verify the backlog expectations

	// Set current version.
	s.setCurrentVersion(deploymentName, currentBuildID)

	rampPercentage := 20
	s.setRampingVersion(deploymentName, "", rampPercentage)

	// Enqueue unversioned backlog.
	unversionedWorkflowCount := 10 * numPartitions
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// There is no way right now for a user to query stats of the "unversioned" version. All we can do in this case
		// is to query the current version's stats and see that it is attributed 80% of the unversioned backlog.
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireLegacyTaskQueueStatsStrict(
			ctx,
			a,
			"DescribeTaskQueue[legacy][workflow][ramping-to-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			legacyExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *TaskQueueStatsSuite) TestRampingAbsorbsUnversionedBacklog_WhenCurrentIsUnversioned_SinglePartition() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.rampingAbsorbsUnversionedBacklogWhenCurrentIsUnversioned(1)
}

func (s *TaskQueueStatsSuite) TestRampingAbsorbsUnversionedBacklog_WhenCurrentIsUnversioned_MultiplePartitions() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

	s.RunTestWithMatchingBehavior(func() {
		s.rampingAbsorbsUnversionedBacklogWhenCurrentIsUnversioned(4)
	})
}

func (s *TaskQueueStatsSuite) rampingAbsorbsUnversionedBacklogWhenCurrentIsUnversioned(numPartitions int) {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tqName := testcore.RandomizeStr("tq-ramping-from-unversioned")
	deploymentName := tqName + "-deployment"
	rampingBuildID := "ramping-build-id"

	pollCtx, cancelPoll := context.WithCancel(ctx)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, rampingBuildID)
	cancelPoll() // cancel the pollers so that we can verify the backlog expectations

	// Set current to unversioned (nil current version).
	s.setCurrentVersion(deploymentName, "")

	// Set ramping to a versioned deployment.
	rampPercentage := 30
	s.setRampingVersion(deploymentName, rampingBuildID, rampPercentage)

	// Enqueue unversioned backlog.
	unversionedWorkflowCount := 10 * numPartitions
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	rampingExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// We can't query "unversioned" as a WorkerDeploymentVersion, but we can validate that the ramping version
		// is attributed its ramp share of the unversioned backlog.
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireLegacyTaskQueueStatsStrict(
			ctx,
			a,
			"DescribeTaskQueue[legacy][workflow][current-unversioned]",
			tqName,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			legacyExpectation,
		)
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *TaskQueueStatsSuite) rampingAndCurrentAbsorbsUnversionedBacklog(numPartitions int) {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tqName := testcore.RandomizeStr("tq-ramping-and-current")
	deploymentName := tqName + "-deployment"
	currentBuildID := "current-build-id"
	rampingBuildID := "ramping-build-id"

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
	unversionedWorkflowCount := 10 * numPartitions
	s.startUnversionedWorkflows(unversionedWorkflowCount, tqName)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	rampingExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	legacyExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	// Currently only testing the following API's:
	// - DescribeWorkerDeploymentVersion for the current and ramping versions.
	// - DescribeTaskQueue Legacy Mode for the current and ramping versions.
	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should also show only 70% of the unversioned backlog for this task queue
		// as a ramping version, with ramp set to 30%, exists and absorbs 30% of the unversioned backlog.
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireLegacyTaskQueueStatsStrict(
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

	// It is important to note that the expected values here are theoretical values based on the ramp percentage. In other words, 70% of the unversioned backlog
	// may not be scheduled on the current version by matching since it makes it's decision based on the workflowID of the workflow. However, when the number of workflows
	// to schedule is high, the expected value of workflows scheduled on the current version will be close to the theoretical value. Here, we shall just be verifying if
	// the theoretical statistics that are being reported are correct.
	activitiesOnCurrentVersionExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * (100 - rampPercentage) / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	activitiesOnRampingVersionExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflowCount * rampPercentage / 100,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// Validate current version activity stats
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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

func (s *TaskQueueStatsSuite) TestInactiveVersionDoesNotAbsorbUnversionedBacklog_MultiplePartitions() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

	s.RunTestWithMatchingBehavior(func() {
		s.inactiveVersionDoesNotAbsorbUnversionedBacklog(4)
	})
}

func (s *TaskQueueStatsSuite) TestInactiveVersionDoesNotAbsorbUnversionedBacklog_SinglePartition() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.inactiveVersionDoesNotAbsorbUnversionedBacklog(1)
}

func (s *TaskQueueStatsSuite) inactiveVersionDoesNotAbsorbUnversionedBacklog(numPartitions int) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond) // zero means no TTL

	tqName := testcore.RandomizeStr("inactive-version")
	deploymentName := tqName + "-deployment"
	currentBuildID := "current-build-id"
	inactiveBuildID := "inactive-build-id"

	pollCtx, cancelPoll := context.WithCancel(testcore.NewContext())

	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, currentBuildID)
	s.createVersionsInTaskQueue(pollCtx, tqName, deploymentName, inactiveBuildID)

	// Set current version
	s.setCurrentVersion(deploymentName, currentBuildID)

	// Stopping the pollers so that we verify the backlog expectations
	cancelPoll()

	// Enqueue unversioned backlog.
	unversionedWorkflows := 10 * numPartitions
	s.startUnversionedWorkflows(unversionedWorkflows, tqName)

	// Enqueue pinned workflows.
	pinnedWorkflows := 10 * numPartitions
	s.startPinnedWorkflows(pinnedWorkflows, tqName, deploymentName, inactiveBuildID)

	currentExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflows,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	inactiveExpectation := TaskQueueExpectations{
		BacklogCount:     pinnedWorkflows,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}

	// Currently only testing the following API's:
	// - DescribeWorkerDeploymentVersion
	// - DescribeTaskQueue Legacy Mode
	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// DescribeWorkerDeploymentVersion: current version should should show 100% of the unversioned backlog for this task queue
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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

	// Validate activity backlogs
	currentActivityExpectation := TaskQueueExpectations{
		BacklogCount:     unversionedWorkflows,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	inactiveActivityExpectation := TaskQueueExpectations{
		BacklogCount:     pinnedWorkflows,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: false,
	}
	workflowTaskQueueEmptyExpectation := TaskQueueExpectations{
		BacklogCount:     0,
		MaxExtraTasks:    0,
		ExpectedAddRate:  true,
		ExpectedDispatch: true, // Since workflow tasks were dispatched
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// The activity task queue of the current version should have the backlog count for the activities that were scheduled
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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
		s.requireWDVTaskQueueStatsStrict(
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

func (s *TaskQueueStatsSuite) requireWDVTaskQueueStatsStrict(
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
	validateTaskQueueStatsStrict(label, a, stats, expectation)
}

func (s *TaskQueueStatsSuite) requireLegacyTaskQueueStatsStrict(
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
	validateTaskQueueStatsStrict(label, a, stats, expectation)
}

// Publishes versioned and unversioned entities; with one entity per priority (plus default priority). Multiplied by `sets`.
func (s *TaskQueueStatsSuite) publishConsumeWorkflowTasksValidateStats(sets int, singlePartition bool) {
	tqName := testcore.RandomizeStr("backlog-counter-task-queue")
	s.createDeploymentInTaskQueue(tqName)

	// verify both workflow and activity backlogs are empty
	expectations := TaskQueueExpectationsByType{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW: {
			BacklogCount:     0,
			MaxExtraTasks:    0,
			ExpectedAddRate:  false,
			ExpectedDispatch: false,
		},
		enumspb.TASK_QUEUE_TYPE_ACTIVITY: {
			BacklogCount:     0,
			MaxExtraTasks:    0,
			ExpectedAddRate:  false,
			ExpectedDispatch: false,
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

	// verify workflow backlog is not empty, activity backlog is empty
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:     total,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  sets > 0,
		ExpectedDispatch: false,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)

	// poll all workflow tasks and enqueue one activity task for each workflow
	totalAct := s.enqueueActivitiesForEachWorkflow(sets, tqName)
	s.EqualValues(total, totalAct, "should have enqueued the same number of activities as workflows")

	// verify workflow backlog is empty, activity backlog is not
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:     0,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  sets > 0,
		ExpectedDispatch: sets > 0,
	}
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:     total,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  sets > 0,
		ExpectedDispatch: false,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)

	// poll all activity tasks
	s.pollActivities(total, tqName)

	// verify both workflow and activity backlogs are empty
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:     0,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  sets > 0,
		ExpectedDispatch: sets > 0,
	}

	s.validateAllTaskQueueStats(tqName, expectations, singlePartition)
}

func (s *TaskQueueStatsSuite) startUnversionedWorkflows(count int, tqName string) {
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

	for i := 0; i < count; i++ {
		request.WorkflowId = uuid.NewString() // starting "count" different Unversioned workflows.
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	}
}

func (s *TaskQueueStatsSuite) startPinnedWorkflows(count int, tqName string, deploymentName string, buildID string) {
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
	for i := 0; i < count; i++ {
		request.WorkflowId = uuid.NewString() // starting "n" different Pinned workflows.
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
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
func (s *TaskQueueStatsSuite) pollWorkflowTasksAndScheduleActivitiesParallel(params ...workflowTasksAndActivitiesPollerParams) {
	var wg sync.WaitGroup
	errCh := make(chan error, len(params))

	for _, p := range params {
		p := p
		wg.Go(func() {
			_, err := s.pollWorkflowTasksAndScheduleActivities(p)
			errCh <- err
		})
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		s.NoError(err)
	}
}

func (s *TaskQueueStatsSuite) pollWorkflowTasksAndScheduleActivities(params workflowTasksAndActivitiesPollerParams) (int, error) {
	deploymentOpts := s.createDeploymentOptions(params.deploymentName, params.buildID)

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

func (s *TaskQueueStatsSuite) completeWorkflowTasksAndScheduleActivities(
	tqName string,
	deploymentName string,
	buildID string,
	activityCount int,
) {
	deploymentOpts := s.createDeploymentOptions(deploymentName, buildID)

	for i := 0; i < activityCount; {
		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "current-version-worker",
			DeploymentOptions: deploymentOpts,
		})
		s.NoError(err)
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
		s.NoError(err)
		i++
	}
}

// TODO (Shivam): We may have to wait for the propagation status to show completed if we are using async workflows here.
func (s *TaskQueueStatsSuite) setCurrentVersion(deploymentName, buildID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: deploymentName,
		BuildId:        buildID,
	})
	s.NoError(err)
}

// TODO (Shivam): We may have to wait for the propagation status to show completed if we are using async workflows here.
func (s *TaskQueueStatsSuite) setRampingVersion(deploymentName, buildID string, rampPercentage int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: deploymentName,
		BuildId:        buildID,
		Percentage:     float32(rampPercentage),
	})
	s.NoError(err)
}

func (s *TaskQueueStatsSuite) describeWDVTaskQueueStats(
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
func (s *TaskQueueStatsSuite) describeLegacyTaskQueueStats(
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
func (s *TaskQueueStatsSuite) enqueueWorkflows(sets int, tqName string) int {
	deploymentOpts := s.deploymentOptions(tqName)

	var total int
	for version := 0; version < 2; version++ { // 0=unversioned, 1=versioned
		for priority := 0; priority <= maxPriority; priority++ {
			for i := 0; i < sets; i++ {
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
				s.NoError(err)

				total++
			}
		}
	}

	s.T().Logf("Enqueued %d workflows", total)
	return total
}

func (s *TaskQueueStatsSuite) createVersionsInTaskQueue(ctx context.Context, tqName string, deploymentName string, buildID string) {
	go func() {
		_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.createDeploymentOptions(deploymentName, buildID),
		})
	}()

	go func() {
		_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.createDeploymentOptions(deploymentName, buildID),
		})
	}()

	// Wait for the version to be created.
	s.EventuallyWithT(func(c *assert.CollectT) {
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
func (s *TaskQueueStatsSuite) createDeploymentInTaskQueue(tqName string) {
	// Using old DeploymentData format
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.deploymentOptions(tqName),
		})
	}()

	go func() {
		defer wg.Done()
		_, _ = s.FrontendClient().PollActivityTaskQueue(testcore.NewContext(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace:         s.Namespace().String(),
			TaskQueue:         &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:          "random",
			DeploymentOptions: s.deploymentOptions(tqName),
		})
	}()

	wg.Wait()
}

func (s *TaskQueueStatsSuite) enqueueActivitiesForEachWorkflow(sets int, tqName string) int {
	deploymentOpts := s.deploymentOptions(tqName)

	var total int
	for version := 0; version < 2; version++ { // 0=unversioned, 1=versioned
		for priority := 0; priority <= maxPriority; priority++ {
			for i := 0; i < sets; { // not counting up here to allow for retries
				pollReq := &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: s.Namespace().String(),
					TaskQueue: &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				}
				if version == 1 {
					pollReq.DeploymentOptions = deploymentOpts
				}

				resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), pollReq)
				s.NoError(err)
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
				s.NoError(err)

				i++
				total++
			}
		}
	}
	s.T().Logf("Enqueued %d activities", total)
	return total
}

func (s *TaskQueueStatsSuite) pollActivities(count int, tqName string) {
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
		s.NoError(err)
		if resp == nil || resp.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
	}
	s.T().Logf("Polled %d activities", count)
}

func (s *TaskQueueStatsSuite) validateAllTaskQueueStats(
	tqName string,
	expectations TaskQueueExpectationsByType,
	singlePartition bool,
) {
	for tqType, expectation := range expectations {
		s.validateTaskQueueStatsByType(tqName, tqType, expectation, singlePartition)
	}
}

func (s *TaskQueueStatsSuite) validateTaskQueueStatsByType(
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

func (s *TaskQueueStatsSuite) validateDescribeTaskQueueWithDefaultMode(
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
	s.NoError(err)
	s.NotNil(resp)
	s.Nil(resp.Stats, "stats should not be reported by default")
	//nolint:staticcheck // SA1019 deprecated
	s.Nil(resp.TaskQueueStatus, "status should not be reported by default")

	s.EventuallyWithT(func(c *assert.CollectT) {
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
			validateTaskQueueStatsByPriority(label, a, resp.StatsByPriorityKey, expectation)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *TaskQueueStatsSuite) validateDescribeTaskQueueWithEnhancedMode(
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
		s.NoError(err)
		s.NotNil(resp)
		s.Nil(resp.Stats, "stats should not be reported by default")
		//nolint:staticcheck // SA1019 deprecated
		s.Nil(resp.TaskQueueStatus, "status should not be reported")
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
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

			validateTaskQueueStats("DescribeTaskQueue_EnhancedMode["+tqType.String()+"]", a, info.Stats, expectation)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *TaskQueueStatsSuite) validateDescribeWorkerDeploymentVersion(
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
	s.NoError(err)
	s.NotNil(resp)
	for _, info := range resp.VersionTaskQueues {
		s.Nil(info.Stats, "stats should not be reported by default")
	}

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		req.ReportTaskQueueStats = true
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, req)
		s.NoError(err)
		a.Len(resp.VersionTaskQueues, 2, "should be 1 task queue for Workflows and 1 for Activities")

		for _, info := range resp.VersionTaskQueues {
			if info.Name == tqName || info.Type == tqType {
				label := "DescribeWorkerDeploymentVersion[" + tqType.String() + "]"
				validateTaskQueueStats(label, a, info.Stats, expectation)
				if s.usePriMatcher && expectation.BacklogCount > 0 {
					// Per priority stats are only available with the priority matcher and when they've been actively used.
					validateTaskQueueStatsByPriority(label, a, info.StatsByPriorityKey, expectation)
				}
				return
			}
		}
		a.Failf("Task queue %s of type %s not found in response", tqName, tqType)
	}, 5*time.Second, 100*time.Millisecond)
}

func validateTaskQueueStatsByPriority(
	label string,
	a *require.Assertions,
	stats map[int32]*taskqueuepb.TaskQueueStats,
	taskQueueExpectation TaskQueueExpectations,
) {
	a.Len(stats, maxPriority, "%s: stats should contain %d priorities", label, maxPriority)

	// use an abgridged version when caching since the exact stats are difficult to predict
	if taskQueueExpectation.CachedEnabled {
		for i := int32(minPriority); i <= maxPriority; i++ {
			if stats[i].ApproximateBacklogCount != 0 && stats[i].TasksDispatchRate > 0 || stats[i].TasksAddRate > 0 {
				return
			}
		}
		a.Failf("%s: should have found at least one non-zero backlog count with any non-zero rate across priorities", label)
	}

	var accBacklogCount int
	for i := int32(minPriority); i <= maxPriority; i++ {
		priExpectation := taskQueueExpectation
		priExpectation.BacklogCount = taskQueueExpectation.BacklogCount / (maxPriority + 1)
		if i == defaultPriority {
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

	a.Equal(expectation.ExpectedAddRate, stats.TasksAddRate > 0,
		"%s: TasksAddRate should be greater than 0 when ExpectedAddRate is true, got %f",
		label, stats.TasksAddRate)

	a.Equal(expectation.ExpectedDispatch, stats.TasksDispatchRate > 0,
		"%s: TasksDispatchRate should be greater than 0 when ExpectedDispatch is true, got %f",
		label, stats.TasksDispatchRate)
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

	a.Equal(expectation.ExpectedAddRate, stats.TasksAddRate > 0,
		"%s: TasksAddRate should be greater than 0 when ExpectedAddRate is true, got %f",
		label, stats.TasksAddRate)

	a.Equal(expectation.ExpectedDispatch, stats.TasksDispatchRate > 0,
		"%s: TasksDispatchRate should be greater than 0 when ExpectedDispatch is true, got %f",
		label, stats.TasksDispatchRate)
}

// TODO: Remove this once older stats tests are refactored to use the createDeploymentOptions function.
func (s *TaskQueueStatsSuite) deploymentOptions(tqName string) *deploymentpb.WorkerDeploymentOptions {
	return &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       tqName + "-deployment",
		BuildId:              "build-id",
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
}

func (s *TaskQueueStatsSuite) createDeploymentOptions(deploymentName string, buildID string) *deploymentpb.WorkerDeploymentOptions {
	return &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       deploymentName,
		BuildId:              buildID,
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
}
