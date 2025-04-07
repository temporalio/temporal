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
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TaskQueueSuite struct {
	testcore.FunctionalTestSuite
}

func TestTaskQueueSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TaskQueueSuite))
}

func (s *TaskQueueSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():                                  true,
		dynamicconfig.MatchingForwarderMaxRatePerSecond.Key():                      1000,
		dynamicconfig.MatchingUseNewMatcher.Key():                                  true,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key():                    4,
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():                     4,
		dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate.Key(): 1,
	}
	s.FunctionalTestSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// func (s *FunctionalTestBase) RunTestWithMatchingBehavior(subtest func()) {
//	for _, forcePollForward := range []bool{false, true} {
//		for _, forceTaskForward := range []bool{false, true} {
//			for _, forceAsync := range []bool{false, true} {
//				name := "NoTaskForward"
//				if forceTaskForward {
//					// force two levels of forwarding
//					name = "ForceTaskForward"
//				}
//				if forcePollForward {
//					name += "ForcePollForward"
//				} else {
//					name += "NoPollForward"
//				}
//				if forceAsync {
//					name += "ForceAsync"
//				} else {
//					name += "AllowSync"
//				}
//
//				s.Run(
//					name, func() {
//						if forceTaskForward {
//							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
//							s.InjectHook(testhooks.MatchingLBForceWritePartition, 11)
//						} else {
//							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
//						}
//						if forcePollForward {
//							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
//							s.InjectHook(testhooks.MatchingLBForceReadPartition, 5)
//						} else {
//							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
//						}
//						if forceAsync {
//							s.InjectHook(testhooks.MatchingDisableSyncMatch, true)
//						} else {
//							s.InjectHook(testhooks.MatchingDisableSyncMatch, false)
//						}
//
//						subtest()
//					},
//				)
//			}
//		}
//	}
//}

//	for _, nPartitions := range []int{1, 4, 8} {
//		for _, nWorkers := range []int{1, 4, 8, 16} {
//
//		}
//	}

func (s *TaskQueueSuite) TestTaskQueueRateLimit_1Partition_1Worker() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	const nWorkflows = 1000

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for wfidx := range nWorkflows {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

}

func (s *TaskQueueSuite) testTaskQueueRateLimitName(nPartitions, nWorkers int) string {
	return fmt.Sprintf("TestTaskQueueRateLimit_%vPartitions_%vWorkers", nPartitions, nWorkers)
}

func (s *TaskQueueSuite) testTaskQueueRateLimit(nPartitions, nWorkers, nBackloggedTasks int, timeToDrain time.Duration) {
	s.Run()

	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1)

	const N = 100
	const Levels = 5

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for wfidx := range N {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
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

func (s *MatchingSuite) TestSubqueue_Migration() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// start with old matcher
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	// start 100 workflows
	for range 100 {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   uuid.NewString(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks and create 300 activities
	for range 100 {
		_, err := s.TaskPoller.PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Equal(3, len(task.History.Events))

				var commands []*commandpb.Command

				for i := range 3 {
					input, err := payloads.Encode(i)
					s.NoError(err)
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             fmt.Sprintf("act%d", i),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								Input:                  input,
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

	processActivity := func() {
		_, err := s.TaskPoller.PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				nothing, err := payloads.Encode()
				s.NoError(err)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	s.T().Log("process first 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching to new matcher")
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)

	s.T().Log("processing next 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching back to old matcher")
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	s.T().Log("processing last 100 activities")
	for range 100 {
		processActivity()
	}
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
