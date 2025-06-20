package tests

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	DescribeTaskQueueSuite struct {
		testcore.FunctionalTestBase
	}

	TaskQueueExpectations struct {
		BacklogCount     int
		MaxExtraTasks    int
		ExpectedAddRate  bool
		ExpectedDispatch bool
	}

	// TaskQueueExpectationsByType maps task queue types to their expectations
	TaskQueueExpectationsByType map[enumspb.TaskQueueType]TaskQueueExpectations
)

func TestDescribeTaskQueueSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DescribeTaskQueueSuite))
}

func (s *DescribeTaskQueueSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.EnableDeploymentVersions, true)
	s.OverrideDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true)
}

func (s *DescribeTaskQueueSuite) TestNonRoot() {
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

func (s *DescribeTaskQueueSuite) TestAddNoTasks_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	s.publishConsumeWorkflowTasksValidateStats(0, false)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_Single_Partition_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingUpdateAckInterval, 5*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	s.publishConsumeWorkflowTasksValidateStats(2, true) // 1 unversioned, 1 versioned
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasks_MultiplePartitions_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Millisecond)

	s.RunTestWithMatchingBehavior(func() {
		s.publishConsumeWorkflowTasksValidateStats(50, false) // 25 unversioned, 25 versioned
	})
}

// NOTE: Cache _eviction_ is already covered by the other tests.
func (s *DescribeTaskQueueSuite) TestAddMultipleTasks_MultiplePartitions_ValidateStats_Cached() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 1*time.Hour) // using a long TTL to verify caching

	tqName := testcore.RandomizeStr("backlog-counter-task-queue")
	workflows := 50 // 25 unversioned, 25 versioned

	// Expect at least *one* of the workflow/activity tasks to be in the stats.
	expectations := TaskQueueExpectations{
		BacklogCount:     1,         // ie at least one task in the backlog
		MaxExtraTasks:    workflows, // ie at most all tasks can be in the backlog
		ExpectedAddRate:  true,
		ExpectedDispatch: true,
	}

	// Enqueue all workflows, 50/50 split between unversioned and versioned.
	s.enqueueWorkflows(workflows, tqName)

	// Enqueue 2 activities, ie 1 per version, to make sure the workflow backlog has some tasks.
	s.enqueueActivitiesForEachWorkflow(2, tqName)

	// Expect the workflow backlog to be non-empty now.
	// This query will cache the stats for the remainder of the test.
	s.validateDescribeTaskQueueByType(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, expectations, false)

	// Enqueue remaining activities.
	s.enqueueActivitiesForEachWorkflow(workflows-2, tqName)

	// Poll 2 activities, ie 1 per version, to make sure the activity backlog has some tasks.
	s.pollActivities(2, tqName)

	// Expect the activity backlog to be non-empty now.
	// This query will cache the stats for the remainder of the test.
	s.validateDescribeTaskQueueByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)

	// Poll remaining activities.
	s.pollActivities(workflows-2, tqName)

	// Despite having polled all the workflows/activies; the stats won't have changed at all since they were cached.
	s.validateDescribeTaskQueueByType(tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, expectations, false)
	s.validateDescribeTaskQueueByType(tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY, expectations, false)
}

// publish 50% to default/unversioned task queue and 50% to versioned task queue
func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStats(workflows int, singlePartition bool) {
	if workflows%2 != 0 {
		s.T().Fatal("workflows must be an even number to ensure half of them are versioned and half are unversioned")
	}

	tqName := testcore.RandomizeStr("backlog-counter-task-queue")

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

	s.validateDescribeTaskQueue(tqName, expectations, singlePartition)

	// Actual counter can be greater than the expected due to History->Matching retries.
	// We make sure the counter is in range [expected, expected+maxExtraTasksAllowed].
	maxExtraTasksAllowed := 3
	if workflows <= 0 {
		maxExtraTasksAllowed = 0
	}

	// enqueue workflows
	s.enqueueWorkflows(workflows, tqName)

	// verify workflow backlog is not empty, activity backlog is empty
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:     int(workflows),
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  workflows > 0,
		ExpectedDispatch: false,
	}

	s.validateDescribeTaskQueue(tqName, expectations, singlePartition)

	// poll all workflow tasks and enqueue one activity task for each workflow
	s.enqueueActivitiesForEachWorkflow(workflows, tqName)

	// verify workflow backlog is empty, activity backlog is not
	expectations[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = TaskQueueExpectations{
		BacklogCount:     0,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  workflows > 0,
		ExpectedDispatch: workflows > 0,
	}
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:     int(workflows),
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  workflows > 0,
		ExpectedDispatch: false,
	}

	s.validateDescribeTaskQueue(tqName, expectations, singlePartition)

	// poll all activity tasks
	s.pollActivities(workflows, tqName)

	// verify both workflow and activity backlogs are empty
	expectations[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = TaskQueueExpectations{
		BacklogCount:     0,
		MaxExtraTasks:    maxExtraTasksAllowed,
		ExpectedAddRate:  workflows > 0,
		ExpectedDispatch: workflows > 0,
	}

	s.validateDescribeTaskQueue(tqName, expectations, singlePartition)
}

func (s *DescribeTaskQueueSuite) enqueueWorkflows(count int, tqName string) {
	deploymentOpts := s.deploymentOptions()

	// tie new deployment version to the task queue before adding tasks to it
	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		testcore.NewContext(), &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId: s.NamespaceID().String(),
			TaskQueue:   tqName,
			TaskQueueTypes: []enumspb.TaskQueueType{
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			},
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version: &deploymentspb.WorkerDeploymentVersion{
						BuildId:        deploymentOpts.BuildId,
						DeploymentName: deploymentOpts.DeploymentName,
					},
				},
			},
		},
	)
	s.NoError(err)

	tq := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	for i := 0; i < count; i++ {
		wt := "functional-workflow-multiple-tasks"
		workflowType := &commonpb.WorkflowType{Name: wt}

		request := &workflowservice.StartWorkflowExecutionRequest{
			Namespace:           s.Namespace().String(),
			WorkflowId:          uuid.New(),
			WorkflowType:        workflowType,
			TaskQueue:           tq,
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(10 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Minute),
		}

		// half of them are versioned
		if i%2 == 0 {
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

		_, err = s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	}
}

func (s *DescribeTaskQueueSuite) enqueueActivitiesForEachWorkflow(count int, tqName string) {
	deploymentOpts := s.deploymentOptions()

	for i := 0; i < count; {
		pollReq := &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		}
		if i%2 == 0 {
			pollReq.DeploymentOptions = deploymentOpts
		}

		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), pollReq)
		s.NoError(err)
		if resp == nil || resp.GetAttempt() < 1 {
			continue // poll again on empty responses
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
						},
					},
				},
			},
		}
		if i%2 == 0 {
			respondReq.DeploymentOptions = deploymentOpts
			respondReq.VersioningBehavior = enumspb.VERSIONING_BEHAVIOR_PINNED
		}
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), respondReq)
		s.NoError(err)

		i++
	}
}

func (s *DescribeTaskQueueSuite) pollActivities(activities int, tqName string) {
	for i := 0; i < activities; {
		pollReq := &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tqName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		}
		if i%2 == 0 {
			pollReq.DeploymentOptions = s.deploymentOptions()
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
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueue(
	tq string,
	expectations TaskQueueExpectationsByType,
	singlePartition bool,
) {
	for taskQueueType, expectation := range expectations {
		s.validateDescribeTaskQueueByType(
			tq,
			taskQueueType,
			expectation,
			singlePartition,
		)
	}
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueueByType(
	tq string,
	taskQueueType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
	singlePartition bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.validateDescribeTaskQueueWithEnhancedModeByType(ctx, tq, taskQueueType, expectation)
	s.validateDescribeTaskQueueWithDefaultModeByType(ctx, tq, taskQueueType, expectation, singlePartition)
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueueWithDefaultModeByType(
	ctx context.Context,
	tq string,
	taskQueueType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
	singlePartition bool,
) {
	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.Namespace().String(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType:          taskQueueType,
			IncludeTaskQueueStatus: true,
			ReportStats:            true,
		})
		a.NoError(err)
		a.NotNil(resp)

		if singlePartition {
			//nolint:staticcheck // SA1019 deprecated field
			a.EqualValues(expectation.BacklogCount/2, // only reports default queue
				resp.TaskQueueStatus.GetBacklogCountHint())
		}

		validateDescribeTaskQueueStats(a, resp.Stats, expectation)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueueWithEnhancedModeByType(
	ctx context.Context,
	tq string,
	taskQueueType enumspb.TaskQueueType,
	expectation TaskQueueExpectations,
) {
	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		deploymentOpts := s.deploymentOptions()
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:   enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions: &taskqueuepb.TaskQueueVersionSelection{
				BuildIds: []string{worker_versioning.WorkerDeploymentVersionToStringV32(
					&deploymentspb.WorkerDeploymentVersion{
						BuildId:        deploymentOpts.BuildId,
						DeploymentName: deploymentOpts.DeploymentName,
					})},
				Unversioned: true,
			},
			TaskQueueTypes:         []enumspb.TaskQueueType{taskQueueType},
			ReportPollers:          true,
			ReportTaskReachability: false,
			ReportStats:            true,
		})
		a.NoError(err)
		a.NotNil(resp)

		//nolint:staticcheck // SA1019 deprecated
		a.Equal(2, len(resp.GetVersionsInfo()), "should be 2: 1 default/unversioned + 1 versioned")
		//nolint:staticcheck // SA1019 deprecated
		for _, v := range resp.GetVersionsInfo() {
			a.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, v.GetTaskReachability())

			info := v.GetTypesInfo()[int32(taskQueueType)]
			a.NotNil(info, "should have info for task queue type %s", taskQueueType)
			stats := info.Stats

			// only expecting half of the backlog count for each version
			validateDescribeTaskQueueStats(a, stats, TaskQueueExpectations{
				BacklogCount:     expectation.BacklogCount / 2,
				MaxExtraTasks:    expectation.MaxExtraTasks / 2,
				ExpectedAddRate:  expectation.ExpectedAddRate,
				ExpectedDispatch: expectation.ExpectedDispatch,
			})
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func validateDescribeTaskQueueStats(
	a *require.Assertions,
	stats *taskqueuepb.TaskQueueStats,
	expectation TaskQueueExpectations,
) {
	// Actual counter can be greater than the expected due to history retries. We make sure the counter is in
	// range [expected, expected+maxBacklogExtraTasks]
	a.GreaterOrEqual(stats.ApproximateBacklogCount, int64(expectation.BacklogCount))
	a.LessOrEqual(stats.ApproximateBacklogCount, int64(expectation.BacklogCount+expectation.MaxExtraTasks))
	a.Equal(stats.ApproximateBacklogCount == 0, stats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
	a.Equal(expectation.ExpectedAddRate, stats.TasksAddRate > 0)
	a.Equal(expectation.ExpectedDispatch, stats.TasksDispatchRate > 0)
}

func (s *DescribeTaskQueueSuite) deploymentOptions() *deploymentpb.WorkerDeploymentOptions {
	return &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       "describe-task-queue-test",
		BuildId:              "build-id",
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
}
