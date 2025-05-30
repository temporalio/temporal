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
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	DescribeTaskQueueSuite struct {
		testcore.FunctionalTestBase
	}
)

// TODO(stephanos): add test for versioned task queues
func TestDescribeTaskQueueSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DescribeTaskQueueSuite))
}

func (s *DescribeTaskQueueSuite) TestNonRoot() {
	resp, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(resp)

	resp, err = s.FrontendClient().DescribeTaskQueue(context.Background(),
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
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0*time.Millisecond)

	s.publishConsumeWorkflowTasksValidateStats(0, false)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTaskPerVersion_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingUpdateAckInterval, 5*time.Second)

	s.RunTestWithMatchingBehavior(func() {
		s.publishConsumeWorkflowTasksValidateStats(1, false)
	})
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasks_MultiplePartitions_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0*time.Millisecond)

	s.publishConsumeWorkflowTasksValidateStats(2, false)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTaskPerVersion_SinglePartition_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(1, true)
}

// TODO(stephanos): re-enable this test
//func (s *DescribeTaskQueueSuite) TestAddSingleTaskPerVersion_ValidateCachedStats_NoMatchingBehaviour() {
//	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
//	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
//	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 500*time.Millisecond)
//
//	s.publishConsumeWorkflowTasksValidateStatsCached(1, true)
//}

// publish 50% to default/unversioned task queue and 50% to versioned task queue
func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStats(workflows int, singlePartition bool) {
	s.OverrideDynamicConfig(dynamicconfig.EnableDeploymentVersions, true)
	s.OverrideDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true)

	expectedBacklogCount := make(map[enumspb.TaskQueueType]int64)
	maxBacklogExtraTasks := make(map[enumspb.TaskQueueType]int64)
	expectedAddRate := make(map[enumspb.TaskQueueType]bool)
	expectedDispatchRate := make(map[enumspb.TaskQueueType]bool)

	// Actual counter can be greater than the expected due to History->Matching retries.
	// We make sure the counter is in range [expected, expected+maxExtraTasksAllowed].
	maxExtraTasksAllowed := int64(3)
	if workflows <= 0 {
		maxExtraTasksAllowed = int64(0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false

	identity := "worker-multiple-tasks"
	tqName := testcore.RandomizeStr("backlog-counter-task-queue")

	// enqueue workflows
	tq := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	for i := 0; i < workflows; i++ {
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
			Identity:            identity,
		}

		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, singlePartition)

	// poll workflow tasks
	for i := 0; i < workflows; {
		pollReq := &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: tq,
			Identity:  identity,
		}

		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), pollReq)
		s.NoError(err)
		if resp == nil || resp.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++

		respondReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			Identity:  identity,
			TaskToken: resp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:            "activity1",
							ActivityType:          &commonpb.ActivityType{Name: "activity_type1"},
							TaskQueue:             tq,
							StartToCloseTimeout:   durationpb.New(time.Minute),
							RequestEagerExecution: false,
						},
					},
				},
			},
		}
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), respondReq)
		s.NoError(err)
	}

	// call describeTaskQueue to verify if the WTF backlog decreased and activity backlog increased
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(0)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, singlePartition)

	// poll activity tasks
	for i := 0; i < workflows; {
		pollReq := &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: tq,
			Identity:  identity,
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

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = int64(0)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, singlePartition)
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueue(
	tq string,
	expectedBacklogCount map[enumspb.TaskQueueType]int64,
	maxBacklogExtraTasks map[enumspb.TaskQueueType]int64,
	expectedAddRate map[enumspb.TaskQueueType]bool,
	expectedDispatchRate map[enumspb.TaskQueueType]bool,
	singlePartition bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ==== Enhanced API Mode

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:   enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions: &taskqueuepb.TaskQueueVersionSelection{
				Unversioned: true,
			},
			TaskQueueTypes:         nil, // both defaultVersionInfoByType
			ReportPollers:          true,
			ReportTaskReachability: false,
			ReportStats:            true,
		})
		a.NoError(err)
		a.NotNil(resp)

		//nolint:staticcheck // SA1019 deprecated
		a.Equal(1, len(resp.GetVersionsInfo()))
		//nolint:staticcheck // SA1019 deprecated
		for _, v := range resp.GetVersionsInfo() {
			a.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, v.GetTaskReachability())

			versionInfoByType := v.GetTypesInfo()
			a.Equal(len(versionInfoByType), len(expectedBacklogCount))

			validateDescribeTaskQueueStats(
				a,
				v.GetTypesInfo()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Stats,
				expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
				maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
				expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
				expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW])

			validateDescribeTaskQueueStats(
				a,
				v.GetTypesInfo()[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].Stats,
				expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
				maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
				expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
				expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
		}
	}, 6*time.Second, 100*time.Millisecond)

	// ==== Default API Mode

	s.EventuallyWithT(func(c *assert.CollectT) {
		a := require.New(c)

		// workflows task queue type

		workflowResp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.Namespace().String(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType:          enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			IncludeTaskQueueStatus: true,
			ReportStats:            true,
		})
		a.NoError(err)
		a.NotNil(workflowResp)

		if singlePartition {
			//nolint:staticcheck // SA1019 deprecated field
			a.Equal(expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW], workflowResp.TaskQueueStatus.GetBacklogCountHint())
		}

		validateDescribeTaskQueueStats(
			a,
			workflowResp.Stats,
			expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
			maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
			expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW],
			expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW])

		// activity task queue type

		activityResp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.Namespace().String(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType:          enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			IncludeTaskQueueStatus: true,
			ReportStats:            true,
		})
		a.NoError(err)
		a.NotNil(activityResp)

		if singlePartition {
			//nolint:staticcheck // SA1019 deprecated field
			a.Equal(expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY], activityResp.TaskQueueStatus.GetBacklogCountHint())
		}

		validateDescribeTaskQueueStats(
			a,
			activityResp.Stats,
			expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
			maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
			expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY],
			expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
	}, 6*time.Second, 100*time.Millisecond)
}

func validateDescribeTaskQueueStats(
	a *require.Assertions,
	stats *taskqueuepb.TaskQueueStats,
	expectedBacklogCount int64,
	maxBacklogExtraTasks int64,
	expectedAddRate bool,
	expectedDispatchRate bool,
) {
	// Actual counter can be greater than the expected due to history retries. We make sure the counter is in
	// range [expected, expected+maxBacklogExtraTasks]
	a.GreaterOrEqual(stats.ApproximateBacklogCount, expectedBacklogCount)
	a.LessOrEqual(stats.ApproximateBacklogCount, expectedBacklogCount+maxBacklogExtraTasks)
	a.Equal(stats.ApproximateBacklogCount == 0, stats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
	a.Equal(expectedAddRate, stats.TasksAddRate > 0)
	a.Equal(expectedDispatchRate, stats.TasksDispatchRate > 0)
}

// validateDescribeTaskQueuePartition calls DescribeTaskQueuePartition to fetch the stats into the partition; used for testing the
// DescribeTaskQueue caching behaviour
func (s *DescribeTaskQueueSuite) validateDescribeTaskQueuePartition(
	tqName string,
	expectedBacklogCount map[enumspb.TaskQueueType]int64,
	expectedAddRate map[enumspb.TaskQueueType]bool,
	expectedDispatchRate map[enumspb.TaskQueueType]bool,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
			context.Background(),
			&matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: s.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     tqName,
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, // since we have only workflow tasks
				},
				Versions: &taskqueuepb.TaskQueueVersionSelection{
					Unversioned: true,
				},
				ReportStats:                   true,
				ReportPollers:                 false,
				ReportInternalTaskQueueStatus: false,
			})
		a := require.New(t)
		a.NoError(err)

		// parsing out the response
		a.Equal(1, len(resp.GetVersionsInfoInternal()), "should be 1 because only default/unversioned queue")
		a.NotNil(resp.GetVersionsInfoInternal()[""])
		a.NotNil(resp.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo())

		// validating stats
		wfStats := resp.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		a.NotNil(wfStats)

		a.GreaterOrEqual(wfStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW])
		a.Equal(expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] == 0, wfStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
		a.Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksAddRate > 0)
		a.Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksDispatchRate > 0)
	}, 1*time.Minute, 50*time.Millisecond)
}

func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStatsCached(workflows int, singlePartition bool) {
	expectedBacklogCount := make(map[enumspb.TaskQueueType]int64)
	maxBacklogExtraTasks := make(map[enumspb.TaskQueueType]int64)
	expectedAddRate := make(map[enumspb.TaskQueueType]bool)
	expectedDispatchRate := make(map[enumspb.TaskQueueType]bool)

	// Actual counter can be greater than the expected due to History->Matching retries. We make sure the counter is in
	// range [expected, expected+maxExtraTasksAllowed]
	maxExtraTasksAllowed := int64(3)
	if workflows <= 0 {
		maxExtraTasksAllowed = int64(0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false

	tqName := testcore.RandomizeStr("backlog-counter-task-queue")
	tq := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	identity := "worker-multiple-tasks"
	for i := 0; i < workflows; i++ {
		id := uuid.New()
		wt := "functional-workflow-multiple-tasks"
		workflowType := &commonpb.WorkflowType{Name: wt}

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.New(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          id,
			WorkflowType:        workflowType,
			TaskQueue:           tq,
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(10 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Minute),
			Identity:            identity,
		}

		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false

	// DescribeTaskQueuePartition loads the latest stats into the partition; this ensures
	// we don't wait when we make the following DescribeTaskQueue call
	s.validateDescribeTaskQueuePartition(tqName, expectedBacklogCount, expectedAddRate, expectedDispatchRate)

	// cache gets populated for the first time
	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, singlePartition)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err)
		if resp == nil || resp.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
	}

	// Do a describe Tq partition calls in an eventually with the matching client
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(0)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = true
	s.validateDescribeTaskQueuePartition(tqName, expectedBacklogCount, expectedAddRate, expectedDispatchRate)

	// verify cached stats, injected in the initial call, are being fetched
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false
	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, singlePartition)
}
