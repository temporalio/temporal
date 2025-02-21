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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type PriorityFairnessSuite struct {
	testcore.FunctionalTestSdkSuite
}

func TestPriorityFairnessSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PriorityFairnessSuite))
}

func (s *PriorityFairnessSuite) SetupSuite() {
	// minimize workflow task backlog to make it easier to debug tests
	s.WorkerOptions.MaxConcurrentWorkflowTaskPollers = 20

	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():     true,
		dynamicconfig.MatchingGetTasksBatchSize.Key(): 20,
	}
	s.FunctionalTestSdkSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PriorityFairnessSuite) TestPriority_Activity_Basic() {
	const N = 100
	const Levels = 5
	actq := testcore.RandomizeStr("actq")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var runs []int

	act1 := func(ctx context.Context, wfidx, pri int) error {
		runs = append(runs, pri)
		// activity.GetLogger(ctx).Info("activity", "pri", pri, "wfidx", wfidx)
		return nil
	}

	wf1 := func(ctx workflow.Context, wfidx int) error {
		var futures []workflow.Future
		for _, pri := range rand.Perm(Levels) {
			actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:              actq,
				ScheduleToCloseTimeout: 60 * time.Second,
				Priority:               temporal.Priority{PriorityKey: pri + 1},
			})
			f := workflow.ExecuteActivity(actCtx, act1, wfidx, pri+1)
			futures = append(futures, f)
		}
		for _, f := range futures {
			s.NoError(f.Get(ctx, nil))
		}
		return nil
	}
	s.Worker().RegisterWorkflow(wf1)

	var execs []client.WorkflowRun
	wfopts := client.StartWorkflowOptions{
		TaskQueue: s.TaskQueue(),
	}
	for wfidx := range N {
		exec, err := s.SdkClient().ExecuteWorkflow(ctx, wfopts, wf1, wfidx)
		s.NoError(err)
		execs = append(execs, exec)
	}

	// wait for activity queue to build up backlog
	s.T().Log("waiting for backlog")
	s.waitForBacklog(ctx, actq, enumspb.TASK_QUEUE_TYPE_ACTIVITY, N*Levels)

	// now let activities run
	s.T().Log("starting worker")
	actw := worker.New(s.SdkClient(), actq, worker.Options{
		MaxConcurrentActivityExecutionSize: 1, // serialize activities
	})
	actw.RegisterActivity(act1)
	actw.Start()
	defer actw.Stop()

	// wait on all workflows to clean up
	for _, exec := range execs {
		s.NoError(exec.Get(ctx, nil))
	}

	w := wrongorderness(runs)
	s.T().Log("wrongorderness:", w)
	s.Less(w, 0.15)
}

func (s *PriorityFairnessSuite) waitForBacklog(ctx context.Context, tq string, tp enumspb.TaskQueueType, n int) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:      s.Namespace().String(),
			TaskQueue:      &taskqueuepb.TaskQueue{Name: tq},
			ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			TaskQueueTypes: []enumspb.TaskQueueType{tp},
			ReportStats:    true,
		})
		assert.NoError(t, err)
		stats := resp.GetVersionsInfo()[""].TypesInfo[int32(tp)].Stats
		assert.GreaterOrEqual(t, stats.ApproximateBacklogCount, int64(n))
	}, 10*time.Second, 200*time.Millisecond)
}

func wrongorderness(vs []int) float64 {
	l := len(vs)
	wrong := 0
	for i, v := range vs[:l-1] {
		for _, w := range vs[i+1:] {
			if v > w {
				wrong++
			}
		}
	}
	return float64(wrong) / float64(l*(l-1)/2)
}
