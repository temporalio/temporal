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
	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type PrioritySuite struct {
	testcore.FunctionalTestBase
}

func TestPrioritySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PrioritySuite))
}

func (s *PrioritySuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():     true,
		dynamicconfig.MatchingGetTasksBatchSize.Key(): 20,
		dynamicconfig.MatchingGetTasksReloadAt.Key():  5,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PrioritySuite) TestPriority_Activity_Basic() {
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
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
		_, err := s.TaskPoller().PollAndHandleActivityTask(
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

func (s *PrioritySuite) TestSubqueue_Migration() {
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
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := s.TaskPoller().PollAndHandleActivityTask(
				tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					nothing, err := payloads.Encode()
					s.NoError(err)
					return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
				},
				taskpoller.WithContext(ctx),
			)
			assert.NoError(c, err)
		}, 5*time.Second, time.Millisecond)
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

type FairnessSuite struct {
	testcore.FunctionalTestBase
}

func TestFairnessSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(FairnessSuite))
}

func (s *FairnessSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingEnableFairness.Key():         true,
		dynamicconfig.MatchingGetTasksBatchSize.Key():      20,
		dynamicconfig.MatchingGetTasksReloadAt.Key():       5,
		dynamicconfig.NumPendingActivitiesLimitError.Key(): 1000,
		// TODO: disable this later?
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  1,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 1,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *FairnessSuite) TestFairness_Activity_Basic() {
	const Workflows = 15
	const Tasks = 15
	const Keys = 10

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zipf := rand.NewZipf(rand.New(rand.NewSource(12345)), 2, 2, Keys-1)

	for wfidx := range Workflows {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range Workflows {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Equal(3, len(task.History.Events))

				var wfidx int
				_, err := fmt.Sscanf(task.WorkflowExecution.WorkflowId, "wf%d", &wfidx)
				s.NoError(err)

				var commands []*commandpb.Command

				for i := range Tasks {
					fkey := int(zipf.Uint64())
					input, err := payloads.Encode(wfidx, fkey)
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
									FairnessKey: fmt.Sprintf("key%d", fkey),
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
	for range Workflows * Tasks {
		_, err := s.TaskPoller().PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				var wfidx, fkey int
				s.NoError(payloads.Decode(task.Input, &wfidx, &fkey))
				s.T().Log("activity", "fkey", fkey, "wfidx", wfidx)
				runs = append(runs, fkey)
				nothing, err := payloads.Encode()
				s.NoError(err)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	u := unfairness(runs)
	s.T().Log("unfairness:", u)
	s.Less(u, 1.0)
}

func unfairness(vs []int) float64 {
	firsts := make(map[int]int)
	for i, v := range vs {
		if _, ok := firsts[v]; !ok {
			firsts[v] = i
		}
	}
	var totalDelay int
	for _, first := range firsts {
		totalDelay += first
	}
	return float64(totalDelay) / float64(len(firsts)*len(firsts))
}

func (s *FairnessSuite) TestFairness_UpdateWorkflowExecutionOptions_InvalidatesPendingTask() {
	tv := testvars.New(s.T())

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
	defer cancel()

	originalPriority := &commonpb.Priority{FairnessKey: "KEY"}
	updatedPriority := &commonpb.Priority{FairnessKey: "NEW_KEY"}

	// Queue up new workflow.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Priority:     originalPriority,
	})
	s.NoError(err)

	// Wait for workflow task to be backlogged.
	s.Eventually(func() bool {
		resp, err := s.AdminClient().GetTaskQueueTasks(ctx, &adminservice.GetTaskQueueTasksRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			BatchSize:     10,
			MinPass:       1,
		})
		return err == nil && len(resp.GetTasks()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Update workflow options to set a new priority.
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.GetRunId(),
		},
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			Priority: updatedPriority,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	s.NoError(err)
	s.NotNil(updateResp.GetWorkflowExecutionOptions())
	s.ProtoEqual(updatedPriority, updateResp.GetWorkflowExecutionOptions().GetPriority())

	// Query workflow to verify workflow has the updated priority.
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.NotNil(descResp.GetWorkflowExecutionInfo())
	s.NotNil(descResp.GetWorkflowExecutionInfo().GetPriority())
	s.ProtoEqual(updatedPriority, descResp.GetWorkflowExecutionInfo().GetPriority())

	// Poll for workflow task and schedule an activity.
	_, err = s.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.ContainsHistoryEvents(`
				3 WorkflowExecutionOptionsUpdated { "Priority": { "FairnessKey": "NEW_KEY" } }
				4 WorkflowTaskStarted
			`, task.History.Events)

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								Priority:               originalPriority,
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
							},
						},
					},
				},
			}, nil
		},
		taskpoller.WithContext(ctx),
	)
	s.NoError(err)

	// Verify that 2 workflow tasks were sent to matching; and 1 was marked obsolete.
	addWorkflowTaskCount := 0
	obsoleteWorkflowTaskCount := 0
	snap := capture.Snapshot()
	for _, rec := range snap[metrics.ClientRequests.Name()] {
		for key, val := range rec.Tags {
			if key == metrics.OperationTagName && val == metrics.MatchingClientAddWorkflowTaskScope {
				addWorkflowTaskCount++
			}
		}
	}
	s.Equal(2, addWorkflowTaskCount, "Expected 2 workflow tasks to be dispatched to matching")
	for _, rec := range snap[metrics.ClientFailures.Name()] {
		for key, val := range rec.Tags {
			if key == metrics.ErrorTypeTagName && val == fmt.Sprintf("%T", serviceerror.ObsoleteMatchingTask{}) {
				obsoleteWorkflowTaskCount++
			}
		}
	}
	s.Equal(1, obsoleteWorkflowTaskCount, "Expected 1 worklow task to be obsolete")

	// Wait for activity task to be backlogged
	s.Eventually(func() bool {
		resp, err := s.AdminClient().GetTaskQueueTasks(ctx, &adminservice.GetTaskQueueTasksRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			BatchSize:     10,
			MinPass:       1,
		})
		return err == nil && len(resp.GetTasks()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Update activity options to set a new priority
	_, err = s.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.GetRunId(),
		},
		Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: tv.ActivityID()},
		ActivityOptions: &activitypb.ActivityOptions{
			Priority: updatedPriority,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	s.NoError(err)

	// Query workflow to verify activity has the updated priority.
	descResp, err = s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.NotNil(descResp.GetPendingActivities())
	s.Len(descResp.GetPendingActivities(), 1)
	s.ProtoEqual(updatedPriority, descResp.GetPendingActivities()[0].GetPriority())
	s.ProtoEqual(updatedPriority, descResp.GetPendingActivities()[0].GetActivityOptions().GetPriority())

	// Poll for activity task and verify it has the updated priority.
	_, err = s.TaskPoller().PollAndHandleActivityTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Equal(tv.ActivityID(), task.ActivityId)
			s.ProtoEqual(updatedPriority, task.Priority)
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		},
		taskpoller.WithContext(ctx),
	)
	s.NoError(err)

	// Verify that 2 activity tasks were sent to matching; and 1 was marked obsolete
	addActivityTaskCount := 0
	obsoleteActivityTaskCount := 0
	snap = capture.Snapshot()
	for _, rec := range snap[metrics.ClientRequests.Name()] {
		for key, val := range rec.Tags {
			if key == metrics.OperationTagName && val == metrics.MatchingClientAddActivityTaskScope {
				addActivityTaskCount++
			}
		}
	}
	s.Equal(2, addActivityTaskCount, "Expected 2 activity tasks to be dispatched to matching")
	for _, rec := range snap[metrics.ClientFailures.Name()] {
		for key, val := range rec.Tags {
			if key == metrics.ErrorTypeTagName && val == fmt.Sprintf("%T", serviceerror.ObsoleteMatchingTask{}) {
				obsoleteActivityTaskCount++
			}
		}
	}
	s.Equal(1+obsoleteWorkflowTaskCount, obsoleteActivityTaskCount, "Expected 1 activity task to be obsolete")
}
