// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		*require.Assertions
		testcore.FunctionalTestBase
	}
)

func TestDescribeTaskQueueSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DescribeTaskQueueSuite))
}

func (s *DescribeTaskQueueSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite("testdata/es_cluster.yaml")
}

func (s *DescribeTaskQueueSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *DescribeTaskQueueSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *DescribeTaskQueueSuite) TestNonRootLegacy() {
	resp, err := s.FrontendClient().DescribeTaskQueue(context.Background(), &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "/_sys/foo/1", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:   enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED,
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *DescribeTaskQueueSuite) TestAddNoTasks_ValidateStats() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0*time.Millisecond)

	s.publishConsumeWorkflowTasksValidateStats(0, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateStats() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingUpdateAckInterval, 5*time.Second)
	s.RunTestWithMatchingBehavior(func() { s.publishConsumeWorkflowTasksValidateStats(1, true) })
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasksMultiplePartitions_ValidateStats() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(100, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateStatsLegacyAPIMode() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(1, false)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateCachedStatsNoMatchingBehaviour() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 500*time.Millisecond)
	s.publishConsumeWorkflowTasksValidateStatsCached(1)
}

func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStats(workflows int, isEnhancedMode bool) {
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
			Namespace:           s.Namespace(),
			WorkflowId:          id,
			WorkflowType:        workflowType,
			TaskQueue:           tq,
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(10 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Minute),
			Identity:            identity,
		}

		_, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, isEnhancedMode, false)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace(),
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err1)
		if resp1 == nil || resp1.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
		_, err := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace(),
			Identity:  identity,
			TaskToken: resp1.TaskToken,
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
		})
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

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, isEnhancedMode, false)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.FrontendClient().PollActivityTaskQueue(
			testcore.NewContext(), &workflowservice.PollActivityTaskQueueRequest{
				Namespace: s.Namespace(),
				TaskQueue: tq,
				Identity:  identity,
			},
		)
		s.NoError(err1)
		if resp1 == nil || resp1.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
	}

	// fetch the latest stats
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = int64(0)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0

	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, isEnhancedMode, false)
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueue(
	tq string,
	expectedBacklogCount map[enumspb.TaskQueueType]int64,
	maxBacklogExtraTasks map[enumspb.TaskQueueType]int64,
	expectedAddRate map[enumspb.TaskQueueType]bool,
	expectedDispatchRate map[enumspb.TaskQueueType]bool,
	isEnhancedMode bool,
	isCached bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	if isEnhancedMode {
		if isCached {
			resp, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.Namespace(),
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:               nil, // default version, in this case unversioned queue
				TaskQueueTypes:         nil, // both types
				ReportPollers:          true,
				ReportTaskReachability: false,
				ReportStats:            true,
			})
			s.NoError(err)
			s.NotNil(resp)
			s.Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
			versionInfo := resp.GetVersionsInfo()[""]
			s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, versionInfo.GetTaskReachability())
			types := versionInfo.GetTypesInfo()
			s.Equal(len(types), len(expectedBacklogCount))

			wfStats := types[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Stats
			actStats := types[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].Stats

			// Actual counter can be greater than the expected due to history retries. We make sure the counter is in
			// range [expected, expected+maxBacklogExtraTasks]
			s.GreaterOrEqual(wfStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW])
			s.LessOrEqual(wfStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW]+maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW])
			s.GreaterOrEqual(actStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
			s.LessOrEqual(actStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY]+maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
			s.Equal(wfStats.ApproximateBacklogCount == 0, wfStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
			s.Equal(actStats.ApproximateBacklogCount == 0, actStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
			s.Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksAddRate > 0)
			s.Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksAddRate > 0)
			s.Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksDispatchRate > 0)
			s.Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksDispatchRate > 0)
		} else {
			s.EventuallyWithT(func(t *assert.CollectT) {
				resp, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
					Namespace:              s.Namespace(),
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
					Versions:               nil, // default version, in this case unversioned queue
					TaskQueueTypes:         nil, // both types
					ReportPollers:          true,
					ReportTaskReachability: false,
					ReportStats:            true,
				})
				s.NoError(err)
				s.NotNil(resp)
				s.Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
				versionInfo := resp.GetVersionsInfo()[""]
				s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, versionInfo.GetTaskReachability())
				types := versionInfo.GetTypesInfo()
				s.Equal(len(types), len(expectedBacklogCount))

				wfStats := types[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Stats
				actStats := types[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].Stats

				a := assert.New(t)

				// Actual counter can be greater than the expected due to history retries. We make sure the counter is in
				// range [expected, expected+maxBacklogExtraTasks]
				a.GreaterOrEqual(wfStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW])
				a.LessOrEqual(wfStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW]+maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW])
				a.GreaterOrEqual(actStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
				a.LessOrEqual(actStats.ApproximateBacklogCount, expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY]+maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_ACTIVITY])
				a.Equal(wfStats.ApproximateBacklogCount == 0, wfStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
				a.Equal(actStats.ApproximateBacklogCount == 0, actStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
				a.Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksAddRate > 0)
				a.Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksAddRate > 0)
				a.Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksDispatchRate > 0)
				a.Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksDispatchRate > 0)
			}, 6*time.Second, 100*time.Millisecond)
		}
	} else {
		// Querying the Legacy API
		s.Eventually(func() bool {
			resp, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.Namespace(),
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED,
				IncludeTaskQueueStatus: true,
			})
			s.NoError(err)
			s.NotNil(resp)
			return resp.TaskQueueStatus.GetBacklogCountHint() == expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW]
		}, 6*time.Second, 100*time.Millisecond)
	}
}

// validateDescribeTaskQueuePartition calls DescribeTaskQueuePartition to fetch the stats into the partition; used for testing the
// DescribeTaskQueue caching behaviour
func (s *DescribeTaskQueueSuite) validateDescribeTaskQueuePartition(tqName string, expectedBacklogCount map[enumspb.TaskQueueType]int64,
	expectedAddRate map[enumspb.TaskQueueType]bool, expectedDispatchRate map[enumspb.TaskQueueType]bool) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(
			context.Background(),
			&matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: s.GetNamespaceID(s.Namespace()),
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
		a := assert.New(t)
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
	}, 200*time.Millisecond, 50*time.Millisecond)
}

func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStatsCached(workflows int) {
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
			Namespace:           s.Namespace(),
			WorkflowId:          id,
			WorkflowType:        workflowType,
			TaskQueue:           tq,
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(10 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Minute),
			Identity:            identity,
		}

		_, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	maxBacklogExtraTasks[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = maxExtraTasksAllowed
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false

	// DescribeTaskQueuePartition loads the latest stats into the partition; this ensures
	// we don't wait when we make the following DescribeTaskQueue call
	s.validateDescribeTaskQueuePartition(tqName, expectedBacklogCount, expectedAddRate, expectedDispatchRate)

	// cache gets populated for the first time
	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, true, true)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace(),
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err1)
		if resp1 == nil || resp1.GetAttempt() < 1 {
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
	s.validateDescribeTaskQueue(tqName, expectedBacklogCount, maxBacklogExtraTasks, expectedAddRate, expectedDispatchRate, true, true)

}
