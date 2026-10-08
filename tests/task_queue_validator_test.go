package tests

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *TaskQueueSuite) TestTaskValidatorBacklog() {
	for _, tc := range []struct {
		name              string
		tasks             int
		invalid           []int
		pollBeforeCleanup int
	}{
		{name: "all_valid_partial_batch", tasks: 2},
		{name: "all_valid_full_batch", tasks: 3},
		{name: "all_invalid_multiple_batches", tasks: 7, invalid: []int{0, 1, 2, 3, 4, 5, 6}},
		{name: "valid_head_mixed", tasks: 5, invalid: []int{2, 3, 4}},
		{name: "invalid_head_mixed", tasks: 4, invalid: []int{0, 1}},
		{name: "interleaved_partial_batch", tasks: 4, invalid: []int{1, 3}},
		{name: "full_valid_prefix_then_invalid", tasks: 5, invalid: []int{3, 4}, pollBeforeCleanup: 3},
	} {
		s.Run(tc.name, func(s *TaskQueueSuite) {
			s.testTaskValidatorBacklog(enumspb.TASK_QUEUE_TYPE_WORKFLOW, tc.tasks, tc.invalid, tc.pollBeforeCleanup)
			s.testTaskValidatorBacklog(enumspb.TASK_QUEUE_TYPE_ACTIVITY, tc.tasks, tc.invalid, tc.pollBeforeCleanup)
		})
	}
}

func (s *TaskQueueSuite) testTaskValidatorBacklog(taskType enumspb.TaskQueueType, taskCount int, invalidPositions []int, pollBeforeCleanup int) {
	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnableFairness, false),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingValidatorBatchSize, 3),
		testcore.WithDynamicConfig(dynamicconfig.MatchingValidatorValidationThreshold, 0),
	)
	tv := env.Tv()
	workflowTV := tv
	validationOperation := "IsWorkflowTaskValid"
	if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		workflowTV = tv.WithTaskQueueNumber(1)
		validationOperation = "IsActivityTaskValid"
	}
	capture := env.StartNamespaceMetricCapture()
	queueCounts := func() (loaded, approximate int64, ok bool) {
		resp, err := env.AdminClient().DescribeTaskQueuePartition(s.Context(), &adminservice.DescribeTaskQueuePartitionRequest{
			Namespace: env.Namespace().String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tv.TaskQueue().GetName(),
				TaskQueueType: taskType,
				PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: 0},
			},
			BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
		})
		if err != nil {
			return 0, 0, false
		}
		statuses := resp.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo().GetInternalTaskQueueStatus()
		if len(statuses) == 0 {
			return 0, 0, false
		}
		for _, status := range statuses {
			loaded += status.GetLoadedTasks()
			approximate += status.GetApproximateBacklogCount()
		}
		return loaded, approximate, true
	}

	executions := make([]*commonpb.WorkflowExecution, taskCount)
	validWorkflows := make(map[string]struct{}, taskCount)
	for i := range taskCount {
		wfID := tv.WithWorkflowIDNumber(i).WorkflowID()
		resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:           env.Namespace().String(),
			WorkflowId:          wfID,
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           workflowTV.TaskQueue(),
			WorkflowRunTimeout:  durationpb.New(5 * time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            tv.ClientIdentity(),
		})
		s.NoError(err)
		executions[i] = &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: resp.GetRunId()}
		validWorkflows[wfID] = struct{}{}
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			_, err := env.TaskPoller().PollAndHandleWorkflowTask(workflowTV,
				func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					s.Equal(wfID, task.GetWorkflowExecution().GetWorkflowId())
					return &workflowservice.RespondWorkflowTaskCompletedRequest{
						Commands: []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
							Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
								ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
									ActivityId:             tv.ActivityID(),
									ActivityType:           tv.ActivityType(),
									TaskQueue:              tv.TaskQueue(),
									ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
									StartToCloseTimeout:    durationpb.New(10 * time.Second),
								},
							},
						}},
					}, nil
				},
				taskpoller.WithContext(s.Context()),
			)
			s.NoError(err)
		}
		// Preserve queue order despite workflows using different history shards.
		s.AwaitTrue(func() bool {
			loaded, _, ok := queueCounts()
			return ok && loaded == int64(i+1)
		}, 10*time.Second, 25*time.Millisecond)
	}
	for _, i := range invalidPositions {
		_, err := env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: executions[i],
			Reason:            "invalidate the persisted workflow task",
			Identity:          tv.ClientIdentity(),
		})
		s.NoError(err)
		delete(validWorkflows, executions[i].GetWorkflowId())
	}

	// With one root partition and no polls on this queue, only the validator makes these calls.
	s.AwaitTrue(func() bool {
		return len(capture.CollectMetric(metrics.ServiceRequests.Name(), func(rec *metricstest.CapturedRecording) bool {
			return rec.Tags["operation"] == validationOperation
		})) > 0
	}, 15*time.Second, 25*time.Millisecond)
	for _, execution := range executions {
		if _, valid := validWorkflows[execution.GetWorkflowId()]; !valid {
			continue
		}
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
		s.NoError(err)
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			s.Len(resp.GetHistory().GetEvents(), 5)
			s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, resp.GetHistory().GetEvents()[4].GetEventType())
		} else {
			s.Len(resp.GetHistory().GetEvents(), 2)
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, resp.GetHistory().GetEvents()[1].GetEventType())
		}
	}

	dispatched := make(map[string]struct{}, len(validWorkflows))
	pollValidTask := func() {
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			_, err := env.TaskPoller().PollAndHandleActivityTask(tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					wfID := task.GetWorkflowExecution().GetWorkflowId()
					s.Contains(validWorkflows, wfID)
					s.NotContains(dispatched, wfID)
					dispatched[wfID] = struct{}{}
					return taskpoller.CompleteActivityTask(tv)(task)
				},
				taskpoller.WithContext(s.Context()),
			)
			s.NoError(err)
			return
		}
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				wfID := task.GetWorkflowExecution().GetWorkflowId()
				s.Contains(validWorkflows, wfID)
				s.NotContains(dispatched, wfID)
				dispatched[wfID] = struct{}{}
				return taskpoller.CompleteWorkflowHandler(task)
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}
	for range pollBeforeCleanup {
		pollValidTask()
	}

	// Approximate backlog can retain dropped tasks until a valid head advances the ack level.
	s.AwaitTrue(func() bool {
		dropped := capture.CollectMetric(metrics.DroppedTasksCounter.Name(), func(rec *metricstest.CapturedRecording) bool {
			return rec.Tags["taskqueue"] == tv.TaskQueue().GetName() && rec.Tags["reason"] == metrics.DroppedTaskReasonInvalid
		})
		loaded, _, ok := queueCounts()
		return len(dropped) == len(invalidPositions) && ok && loaded == int64(len(validWorkflows)-len(dispatched))
	}, 30*time.Second, 25*time.Millisecond)

	for len(dispatched) < len(validWorkflows) {
		pollValidTask()
	}
	s.Len(dispatched, len(validWorkflows))
	if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		for range len(validWorkflows) {
			_, err := env.TaskPoller().PollAndHandleWorkflowTask(workflowTV, taskpoller.CompleteWorkflowHandler, taskpoller.WithContext(s.Context()))
			s.NoError(err)
		}
	}
	s.AwaitTrue(func() bool {
		loaded, approximate, ok := queueCounts()
		return ok && loaded == 0 && approximate == 0
	}, 10*time.Second, 25*time.Millisecond)
	for _, execution := range executions {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
		s.NoError(err)
		wantStatus := enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		if _, valid := validWorkflows[execution.GetWorkflowId()]; valid {
			wantStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		}
		s.Equal(wantStatus, resp.GetWorkflowExecutionInfo().GetStatus())
	}
}
