// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type TaskQueueSuite struct {
	testcore.FunctionalTestSuite
	sdkClient sdkclient.Client
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
	s.FunctionalTestSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *TaskQueueSuite) SetupTest() {
	s.FunctionalTestSuite.SetupTest()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
}

func (s *TaskQueueSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
	s.FunctionalTestBase.TearDownTest()
}

// Not using RunTestWithMatchingBehavior because I want to pass different expected drain times for different configurations
func (s *TaskQueueSuite) TestTaskQueueRateLimit() {
	s.RunTaskQueueRateLimitTest(1, 1, 2*time.Second, true)  // ~0.75s avg
	s.RunTaskQueueRateLimitTest(1, 1, 2*time.Second, false) // ~1.1s avg

	// Testing multiple partitions with insufficient pollers is too flaky, because token recycling
	// depends on a process being available to accept the token, so I'm not testing it
	s.RunTaskQueueRateLimitTest(4, 8, 4*time.Second, true)   // ~1.6s avg
	s.RunTaskQueueRateLimitTest(4, 8, 12*time.Second, false) // ~6s avg
}

func (s *TaskQueueSuite) RunTaskQueueRateLimitTest(nPartitions, nWorkers int, timeToDrain time.Duration, useNewMatching bool) {
	s.Run(s.testTaskQueueRateLimitName(nPartitions, nWorkers, useNewMatching), func() { s.taskQueueRateLimitTest(nPartitions, nWorkers, timeToDrain, useNewMatching) })
}

func (s *TaskQueueSuite) taskQueueRateLimitTest(nPartitions, nWorkers int, timeToDrain time.Duration, useNewMatching bool) {
	if useNewMatching {
		s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)
	}
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, nPartitions)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, nPartitions)

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
	for wfidx := 0; wfidx < maxBacklog; wfidx++ {
		_, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv.TaskQueue().GetName(),
			ID:        fmt.Sprintf("wf%d", wfidx),
		}, helloRateLimitTest, "Donna")
		s.NoError(err)
	}

	// wait for backlog to be >= maxBacklog
	wfBacklogCount := int64(0)
	s.Eventually(
		func() bool {
			resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:   s.Namespace().String(),
				TaskQueue:   tv.TaskQueue(),
				ApiMode:     enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				ReportStats: true,
			})
			s.NoError(err)
			wfBacklogCount = resp.GetVersionsInfo()[""].GetTypesInfo()[sdkclient.TaskQueueTypeWorkflow].GetStats().GetApproximateBacklogCount()
			return wfBacklogCount >= maxBacklog
		},
		1*time.Second,
		200*time.Millisecond,
	)

	// terminate all those workflow executions so that all the tasks in the backlog are invalid
	listResp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Namespace().String(),
		Query:     fmt.Sprintf("TaskQueue = '%s'", tv.TaskQueue().GetName()),
	})
	s.NoError(err)
	for _, exec := range listResp.GetExecutions() {
		_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: exec.GetExecution().GetWorkflowId(), RunId: exec.GetExecution().GetRunId()},
			Reason:            "test",
			Identity:          tv.ClientIdentity(),
		})
		s.NoError(err)
	}

	// start some workers
	workers := make([]worker.Worker, nWorkers)
	for i := 0; i < nWorkers; i++ {
		workers[i] = worker.New(s.sdkClient, tv.TaskQueue().GetName(), worker.Options{})
		workers[i].RegisterWorkflow(helloRateLimitTest)
		err := workers[i].Start()
		s.NoError(err)
	}

	// wait for backlog to be 0
	s.Eventually(
		func() bool {
			resp, err := s.sdkClient.DescribeTaskQueueEnhanced(ctx, sdkclient.DescribeTaskQueueEnhancedOptions{
				TaskQueue:   tv.TaskQueue().GetName(),
				ReportStats: true,
			})
			s.NoError(err)
			wfBacklogCount = resp.VersionsInfo[""].TypesInfo[sdkclient.TaskQueueTypeWorkflow].Stats.ApproximateBacklogCount
			return wfBacklogCount == 0
		},
		timeToDrain,
		200*time.Millisecond,
	)

}

func (s *TaskQueueSuite) testTaskQueueRateLimitName(nPartitions, nWorkers int, useNewMatching bool) string {
	ret := fmt.Sprintf("%vPartitions_%vWorkers", nPartitions, nWorkers)
	if useNewMatching {
		return "NewMatching_" + ret
	}
	return "OldMatching_" + ret
}
