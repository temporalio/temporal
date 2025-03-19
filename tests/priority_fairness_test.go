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
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PriorityFairnessSuite struct {
	testcore.FunctionalTestSuite
}

func TestPriorityFairnessSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PriorityFairnessSuite))
}

func (s *PriorityFairnessSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():     true,
		dynamicconfig.MatchingGetTasksBatchSize.Key(): 20,
	}
	s.FunctionalTestSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PriorityFairnessSuite) TestPriority_Activity_Basic() {
	const N = 100
	const Levels = 5

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for wfidx := range N {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
			TaskQueue:    tv.TaskQueue(),
			RequestId:    uuid.NewString(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range N {
		_, err := s.TaskPoller.PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Equal(3, len(task.History.Events))

				var wfidx int
				_, err := fmt.Sscanf(task.WorkflowExecution.WorkflowId, "wf%d", &wfidx)
				s.NoError(err)

				var commands []*commandpb.Command

				for i, pri := range rand.Perm(Levels) {
					input, err := payloads.Encode(wfidx, pri+1)
					s.NoError(err)
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             fmt.Sprintf("act%d", i),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								Priority: &commonpb.Priority{
									PriorityKey: int32(pri + 1),
								},
								Input: input,
							},
						},
					})
				}

				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
			},
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	// process activity tasks
	var runs []int
	for range N * Levels {
		_, err := s.TaskPoller.PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				var wfidx, pri int
				s.NoError(payloads.Decode(task.Input, &wfidx, &pri))
				// s.T().Log("activity", "pri", pri, "wfidx", wfidx)
				runs = append(runs, pri)
				nothing, err := payloads.Encode()
				s.NoError(err)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	w := wrongorderness(runs)
	s.T().Log("wrongorderness:", w)
	s.Less(w, 0.15)
}

// func (s *PriorityFairnessSuite) TestSubqueue_Migration() {
// 	actq := testcore.RandomizeStr("actq")

// 	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
// 	defer cancel()

// 	// start with old matcher
// 	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

// 	var activitiesCompleted atomic.Int64
// 	unblockActivities := make(chan struct{})

// 	act1 := func(ctx context.Context, wfidx int) error {
// 		if activitiesCompleted.Load() == 100 || activitiesCompleted.Load() == 200 {
// 			<-unblockActivities
// 		}
// 		activitiesCompleted.Add(1)
// 		return nil
// 	}

// 	wf1 := func(ctx workflow.Context, wfidx int) error {
// 		var futures []workflow.Future
// 		for range 3 {
// 			actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
// 				TaskQueue:              actq,
// 				ScheduleToCloseTimeout: 60 * time.Second,
// 			})
// 			f := workflow.ExecuteActivity(actCtx, act1, wfidx)
// 			futures = append(futures, f)
// 		}
// 		for _, f := range futures {
// 			s.NoError(f.Get(ctx, nil))
// 		}
// 		return nil
// 	}
// 	s.Worker().RegisterWorkflow(wf1)

// 	wfopts := client.StartWorkflowOptions{TaskQueue: s.TaskQueue()}
// 	for wfidx := range 100 {
// 		_, err := s.SdkClient().ExecuteWorkflow(ctx, wfopts, wf1, wfidx)
// 		s.NoError(err)
// 	}

// 	s.T().Log("waiting for backlog")
// 	s.waitForBacklog(ctx, actq, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 300)

// 	s.T().Log("starting worker")
// 	actw := worker.New(s.SdkClient(), actq, worker.Options{
// 		MaxConcurrentActivityExecutionSize: 1, // serialize activities
// 	})
// 	actw.RegisterActivity(act1)
// 	actw.Start()
// 	defer actw.Stop()

// 	s.T().Log("waiting for first 100 activities")
// 	s.Eventually(func() bool { return activitiesCompleted.Load() == 100 }, 10*time.Second, 10*time.Millisecond)

// 	s.T().Log("switching to new matcher")
// 	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)

// 	s.T().Log("unblocking activities")
// 	unblockActivities <- struct{}{}

// 	s.T().Log("waiting for next 100 activities")
// 	s.Eventually(func() bool { return activitiesCompleted.Load() == 200 }, 10*time.Second, 10*time.Millisecond)

// 	s.T().Log("switching back to old matcher")
// 	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

// 	s.T().Log("unblocking activities")
// 	unblockActivities <- struct{}{}

// 	s.T().Log("waiting for last 100 activites")
// 	s.Eventually(func() bool { return activitiesCompleted.Load() == 300 }, 10*time.Second, 10*time.Millisecond)
// }

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
