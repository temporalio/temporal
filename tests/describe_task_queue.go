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
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/dynamicconfig"
)

type (
	DescribeTaskQueueSuite struct {
		*require.Assertions
		FunctionalTestBase
	}
)

func (s *DescribeTaskQueueSuite) SetupSuite() {
	s.setupSuite("testdata/es_cluster.yaml")
}

func (s *DescribeTaskQueueSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *DescribeTaskQueueSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *DescribeTaskQueueSuite) TestAddNoTasks_ValidateStats() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(0, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateStats() {
	s.testWithMatchingBehavior(func() { s.publishConsumeWorkflowTasksValidateStats(1, true) })
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasksMultiplePartitions_ValidateStats() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(100, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateStatsLegacyAPIMode() {
	// Override the ReadPartitions and WritePartitions
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	s.publishConsumeWorkflowTasksValidateStats(1, false)
}

func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateStats(workflows int, isEnhancedMode bool) {
	expectedBacklogCount := make(map[enumspb.TaskQueueType]int64)
	expectedAddRate := make(map[enumspb.TaskQueueType]bool)
	expectedDispatchRate := make(map[enumspb.TaskQueueType]bool)

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false

	tl := s.randomizeStr("backlog-counter-task-queue")
	tq := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	identity := "worker-multiple-tasks"
	for i := 0; i < workflows; i++ {
		id := uuid.New()
		wt := "functional-workflow-multiple-tasks"
		workflowType := &commonpb.WorkflowType{Name: wt}

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.New(),
			Namespace:           s.namespace,
			WorkflowId:          id,
			WorkflowType:        workflowType,
			TaskQueue:           tq,
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(10 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Minute),
			Identity:            identity,
		}

		_, err0 := s.client.StartWorkflowExecution(NewContext(), request)
		s.NoError(err0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = false

	s.validateDescribeTaskQueue(tl, expectedBacklogCount, expectedAddRate, expectedDispatchRate, isEnhancedMode)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.client.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err1)
		if resp1 == nil || resp1.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
		_, err := s.client.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.namespace,
			Identity:  identity,
			TaskToken: resp1.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:            "activity1",
							ActivityType:          &commonpb.ActivityType{Name: "activity_type1"},
							TaskQueue:             &taskqueuepb.TaskQueue{Name: tl},
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
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = false

	s.validateDescribeTaskQueue(tl, expectedBacklogCount, expectedAddRate, expectedDispatchRate, isEnhancedMode)

	// Poll the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.client.PollActivityTaskQueue(
			NewContext(), &workflowservice.PollActivityTaskQueueRequest{
				Namespace: s.namespace,
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

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = int64(0)
	expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0
	expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = workflows > 0

	s.validateDescribeTaskQueue(tl, expectedBacklogCount, expectedAddRate, expectedDispatchRate, isEnhancedMode)
}

func (s *DescribeTaskQueueSuite) isBacklogHeadCreateTimeCorrect(actualBacklogAge time.Duration, expectEmptyBacklog bool) bool {
	return expectEmptyBacklog == (actualBacklogAge == time.Duration(0))
}

func (s *DescribeTaskQueueSuite) isAddDispatchTasksRateCorrect(
	actualAddTasksRate float32, actualDispatchTasksRate float32, expectAddRate, expectDispatchRate bool) bool {
	return expectDispatchRate == (actualDispatchTasksRate != 0) && (expectAddRate == (actualAddTasksRate != 0))
}

func (s *DescribeTaskQueueSuite) isBacklogCountCorrect(actualBacklogCounter int64, expectedBacklogCount int64) bool {
	return actualBacklogCounter == expectedBacklogCount
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueue(
	tq string,
	expectedBacklogCount map[enumspb.TaskQueueType]int64,
	expectedAddRate map[enumspb.TaskQueueType]bool,
	expectedDispatchRate map[enumspb.TaskQueueType]bool,
	isEnhancedMode bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	if isEnhancedMode {
		s.Eventually(func() bool {
			resp, err = s.client.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.namespace,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:               nil, // default version, in this case unversioned queue
				TaskQueueTypes:         nil, // both types
				ReportPollers:          true,
				ReportTaskReachability: true,
				ReportStats:            true,
			})
			s.NoError(err)
			s.NotNil(resp)
			s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
			versionInfo := resp.GetVersionsInfo()[""]
			s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
			types := versionInfo.GetTypesInfo()
			s.Assert().Equal(len(types), len(expectedBacklogCount))

			wfStats := types[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Stats
			actStats := types[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].Stats

			// Waiting until the counts are as expected, then all other metrics must be consistent
			if wfStats.ApproximateBacklogCount != expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] ||
				actStats.ApproximateBacklogCount != expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] {
				return false
			}

			s.Assert().Equal(expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] == 0, wfStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
			s.Assert().Equal(expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] == 0, actStats.ApproximateBacklogAge.AsDuration() == time.Duration(0))
			s.Assert().Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksAddRate > 0)
			s.Assert().Equal(expectedAddRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksAddRate > 0)
			s.Assert().Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_WORKFLOW], wfStats.TasksDispatchRate > 0)
			s.Assert().Equal(expectedDispatchRate[enumspb.TASK_QUEUE_TYPE_ACTIVITY], actStats.TasksDispatchRate > 0)

			return true
		}, 3*time.Second, 50*time.Millisecond)

	} else {
		// Querying the Legacy API
		s.Eventually(func() bool {
			resp, err = s.client.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.namespace,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED,
				IncludeTaskQueueStatus: true,
			})
			s.NoError(err)
			s.NotNil(resp)
			return resp.TaskQueueStatus.GetBacklogCountHint() == expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW]
		}, 3*time.Second, 50*time.Millisecond)
	}
}
