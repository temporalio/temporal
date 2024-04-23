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
	"flag"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/updateutils"
	"google.golang.org/protobuf/types/known/durationpb"
)

// making a new test suite here

type (
	DescribeTaskQueueSuite struct {
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		updateutils.UpdateUtils
		FunctionalTestBase
	}
)

func (s *DescribeTaskQueueSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs:     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs: true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs:     true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode:        partitionTreeDegree,
		dynamicconfig.TaskQueuesPerBuildIdLimit:                  3,

		dynamicconfig.AssignmentRuleLimitPerQueue:              10,
		dynamicconfig.RedirectRuleLimitPerQueue:                10,
		dynamicconfig.RedirectRuleChainLimitPerQueue:           10,
		dynamicconfig.MatchingDeletedRuleRetentionTime:         24 * time.Hour,
		dynamicconfig.ReachabilityBuildIdVisibilityGracePeriod: 3 * time.Minute,
		dynamicconfig.ReachabilityQueryBuildIdLimit:            4,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS:                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS:                               1000,

		// The dispatch tests below rely on being able to see the effects of changing
		// versioning data relatively quickly. In general, we only promise to act on new
		// versioning data "soon", i.e. after a long poll interval. We can reduce the long poll
		// interval so that we don't have to wait so long.
		// TODO: update this comment. it may be outdated and/or misleading.
		dynamicconfig.MatchingLongPollExpirationInterval: longPollTime,

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
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.UpdateUtils = updateutils.New(s.T())
}

func (s *DescribeTaskQueueSuite) TestAddNoTasks_ValidateBacklogInfo() {
	s.PublishConsumeWorkflowTasksValidateBacklogInfo(4, 0)
}

func (s *DescribeTaskQueueSuite) TestAddSingleTask_ValidateBacklogInfo() {
	s.PublishConsumeWorkflowTasksValidateBacklogInfo(1, 1)
}

func (s *DescribeTaskQueueSuite) TestAddMultipleTasksMultiplePartitions_ValidateBacklogInfo() {
	s.PublishConsumeWorkflowTasksValidateBacklogInfo(4, 4)
}

func (s *DescribeTaskQueueSuite) PublishConsumeWorkflowTasksValidateBacklogInfo(partitions int, workflows int) {
	// overriding the ReadPartitions and WritePartitions
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, partitions)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, partitions)

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
			WorkflowRunTimeout:  durationpb.New(20 * time.Second),
			WorkflowTaskTimeout: durationpb.New(3 * time.Second),
			Identity:            identity,
		}

		resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err0)

		we := &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resp0.RunId,
		}

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))
	}

	response := s.ValidateDescribeTaskQueue(tl)
	versionInfo := response.GetVersionsInfo()[""]

	// verifying backlog counter increased as tasks were added
	addedTasks := int64(workflows)
	for qT, t := range versionInfo.GetTypesInfo() {
		queueType := enumspb.TaskQueueType(qT)
		if queueType == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			s.Assert().Equal(t.BacklogInfo.ApproximateBacklogCount, addedTasks)
		} else if queueType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			s.Assert().Equal(t.BacklogInfo.ApproximateBacklogCount, int64(0))
		}
	}

	// starting and completing the workflow tasks by fetching their workflowExecutions
	for i := 0; i < workflows; i++ {
		resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int32(1), resp1.GetAttempt())

		// responding with completion
		_, err2 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:             s.namespace,
			TaskToken:             resp1.GetTaskToken(),
			ReturnNewWorkflowTask: true,
		})
		s.NoError(err2)
	}

	// call describeTaskQueue to verify if the backlog decreased (should decrease since we only had one task)
	response = s.ValidateDescribeTaskQueue(tl)
	versionInfo = response.GetVersionsInfo()[""]

	for _, t := range versionInfo.GetTypesInfo() {
		s.Assert().Equal(t.BacklogInfo.ApproximateBacklogCount, int64(0))
	}
}

func (s *DescribeTaskQueueSuite) ValidateDescribeTaskQueue(tl string) *workflowservice.DescribeTaskQueueResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

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
		return len(versionInfo.GetTypesInfo()) == 2

	}, 3*time.Second, 50*time.Millisecond)
	return resp
}

func TestDescribeTaskQueueSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(DescribeTaskQueueSuite))
}
