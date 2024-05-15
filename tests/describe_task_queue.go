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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	DescribeTaskQueueSuite struct {
		*require.Assertions
		FunctionalTestBase
	}
)

func (s *DescribeTaskQueueSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// this is overridden for tests using testWithMatchingBehavior
		dynamicconfig.MatchingNumTaskqueueReadPartitions:  4,
		dynamicconfig.MatchingNumTaskqueueWritePartitions: 4,
	}
	s.setupSuite("testdata/es_cluster.yaml")
}

func (s *DescribeTaskQueueSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *DescribeTaskQueueSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *DescribeTaskQueueSuite) TestAddNoTasks_ValidateBacklogInfo() {
	s.publishConsumeWorkflowTasksValidateBacklogInfo(4, 0, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateBacklogInfo() {
	s.publishConsumeWorkflowTasksValidateBacklogInfo(1, 1, true)
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasksMultiplePartitions_ValidateBacklogInfo() {
	s.publishConsumeWorkflowTasksValidateBacklogInfo(4, 100, true)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateBacklogInfoLegacyAPIMode() {
	s.publishConsumeWorkflowTasksValidateBacklogInfo(1, 1, false)
}

func (s *DescribeTaskQueueSuite) publishConsumeWorkflowTasksValidateBacklogInfo(partitions int, workflows int, isEnhancedMode bool) {
	// overriding the ReadPartitions and WritePartitions
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, partitions)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, partitions)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingLongPollExpirationInterval, 10*time.Second)

	expectedBacklogCount := make(map[enumspb.TaskQueueType]int64)
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_ACTIVITY] = 0

	tl := "backlog-counter-task-queue"
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

		_, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err0)
	}

	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(workflows)
	s.validateDescribeTaskQueue(tl, expectedBacklogCount, isEnhancedMode)

	// Polling the tasks
	for i := 0; i < workflows; {
		resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err1)
		if resp1 == nil || resp1.GetAttempt() < 1 {
			continue // poll again on empty responses
		}
		i++
	}

	// call describeTaskQueue to verify if the backlog decreased
	expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = int64(0)
	s.validateDescribeTaskQueue(tl, expectedBacklogCount, isEnhancedMode)
}

func (s *DescribeTaskQueueSuite) validateDescribeTaskQueue(tl string, expectedBacklogCount map[enumspb.TaskQueueType]int64,
	isEnhancedMode bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	if isEnhancedMode {
		s.Eventually(func() bool {
			resp, err = s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.namespace,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:               nil, // default version, in this case unversioned queue
				TaskQueueTypes:         nil, // both types
				ReportPollers:          true,
				ReportTaskReachability: true,
				ReportBacklogInfo:      true,
			})
			s.NoError(err)
			s.NotNil(resp)
			s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
			versionInfo := resp.GetVersionsInfo()[""]
			s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
			types := versionInfo.GetTypesInfo()
			s.Assert().Equal(len(types), len(expectedBacklogCount))

			validator := true
			for qT, t := range types {
				queueType := enumspb.TaskQueueType(qT)
				if t.BacklogInfo.ApproximateBacklogCount != expectedBacklogCount[queueType] {
					validator = false
				}
			}
			return validator == true
		}, 3*time.Second, 50*time.Millisecond)

	} else {
		// Querying the Legacy API
		s.Eventually(func() bool {
			resp, err = s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:              s.namespace,
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED,
				IncludeTaskQueueStatus: true,
			})
			s.NoError(err)
			s.NotNil(resp)
			return resp.TaskQueueStatus.GetBacklogCountHint() == expectedBacklogCount[enumspb.TASK_QUEUE_TYPE_WORKFLOW]
		}, 3*time.Second, 50*time.Millisecond)
	}
}
