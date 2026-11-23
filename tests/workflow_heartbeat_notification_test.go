package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowHeartbeatNotificationSuite struct {
	parallelsuite.Suite[*WorkflowHeartbeatNotificationSuite]
}

func TestWorkflowHeartbeatNotificationSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowHeartbeatNotificationSuite{})
}

func (s *WorkflowHeartbeatNotificationSuite) newEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowHeartbeatNotificationRolloutPercent, 100),
		// Dispatch is a sync match, so give the control queue poll below time to arrive.
		testcore.WithDynamicConfig(dynamicconfig.WorkflowHeartbeatNotificationDispatchTimeout, 10*time.Second),
	)
}

func (s *WorkflowHeartbeatNotificationSuite) startWorkflow(env *testcore.TestEnv, tv *testvars.TestVars, eager bool) *workflowservice.StartWorkflowExecutionResponse {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.RequestID(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Identity:                 tv.WorkerIdentity(),
		WorkflowExecutionTimeout: durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(30 * time.Second),
	}
	if eager {
		request.RequestEagerExecution = true
		request.EagerWorkerControlTaskQueue = tv.ControlQueueName(env.Namespace().String())
		request.EagerWorkerCapabilities = &workflowservice.StartWorkflowExecutionRequest_Capabilities{
			SupportsWorkflowHeartbeatNotification: true,
		}
	}
	resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	return resp
}

func (s *WorkflowHeartbeatNotificationSuite) pollWorkflowTask(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	taskQueue *taskqueuepb.TaskQueue,
	background <-chan error,
) *workflowservice.PollWorkflowTaskQueueResponse {
	task := callWatchingBackground(s, background, func(ctx context.Context) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
		return env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace:              env.Namespace().String(),
			TaskQueue:              taskQueue,
			Identity:               tv.WorkerIdentity(),
			WorkerInstanceKey:      tv.WorkerInstanceKey(),
			WorkerControlTaskQueue: tv.ControlQueueName(env.Namespace().String()),
			Capabilities: &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{
				SupportsWorkflowHeartbeatNotification: true,
			},
		})
	})
	s.NotEmpty(task.GetTaskToken())
	return task
}

func (s *WorkflowHeartbeatNotificationSuite) completeWorkflowTask(
	env *testcore.TestEnv,
	taskToken []byte,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) *workflowservice.RespondWorkflowTaskCompletedResponse {
	request.Namespace = env.Namespace().String()
	request.TaskToken = taskToken
	resp, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), request)
	s.NoError(err)
	return resp
}

func (s *WorkflowHeartbeatNotificationSuite) signal(env *testcore.TestEnv, tv *testvars.TestVars, runID string) {
	_, err := env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: tv.WithRunID(runID).WorkflowExecution(),
		SignalName:        tv.SignalName(),
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)
}

// sendUpdate sends an Update that stays unanswered. The returned channel gets the RPC error if the call ends.
func (s *WorkflowHeartbeatNotificationSuite) sendUpdate(env *testcore.TestEnv, tv *testvars.TestVars, runID string) <-chan error {
	return s.callInBackground(func(ctx context.Context) error {
		_, err := env.FrontendClient().UpdateWorkflowExecution(ctx, s.updateRequest(env, tv, runID))
		return err
	})
}

func (s *WorkflowHeartbeatNotificationSuite) callInBackground(call func(context.Context) error) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- call(s.Context()) }()
	return errCh
}

// callWatchingBackground runs call and fails early if the background RPC fails first, so the test reports that
// error instead of a poll timeout. A nil background channel is never ready.
func callWatchingBackground[T any](
	s *WorkflowHeartbeatNotificationSuite,
	background <-chan error,
	call func(context.Context) (T, error),
) T {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := call(ctx)
		done <- result{value: value, err: err}
	}()
	for {
		select {
		case r := <-done:
			s.NoError(r.err)
			return r.value
		case err := <-background:
			s.NoError(err, "background RPC failed")
			background = nil
		}
	}
}

func (s *WorkflowHeartbeatNotificationSuite) updateRequest(env *testcore.TestEnv, tv *testvars.TestVars, runID string) *workflowservice.UpdateWorkflowExecutionRequest {
	return &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: tv.WithRunID(runID).WorkflowExecution(),
		WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
		Request: &updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: tv.UpdateID()},
			Input: &updatepb.Input{Name: tv.HandlerName()},
		},
	}
}

// receiveNotification polls the worker control queue and acknowledges the notification like an SDK worker.
func (s *WorkflowHeartbeatNotificationSuite) receiveNotification(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	background <-chan error,
) *workerservicepb.ExecuteCommandsRequest {
	resp, request := s.pollNotification(env, tv, background)
	s.respondNotification(env, tv, resp.GetTaskToken(), &workerpb.WorkflowHeartbeatNotificationResult{
		Outcome: workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_ACCEPTED,
	})
	return request
}

func (s *WorkflowHeartbeatNotificationSuite) pollNotification(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	background <-chan error,
) (*workflowservice.PollNexusTaskQueueResponse, *workerservicepb.ExecuteCommandsRequest) {
	resp := callWatchingBackground(s, background, func(ctx context.Context) (*workflowservice.PollNexusTaskQueueResponse, error) {
		return env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tv.ControlQueueName(env.Namespace().String()), Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
	})
	s.NotEmpty(resp.GetTaskToken())
	var request workerservicepb.ExecuteCommandsRequest
	s.NoError(payload.Decode(resp.GetRequest().GetStartOperation().GetPayload(), &request))
	return resp, &request
}

func (s *WorkflowHeartbeatNotificationSuite) respondNotification(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	taskToken []byte,
	result *workerpb.WorkflowHeartbeatNotificationResult,
) {
	capture := env.StartNamespaceMetricCapture()
	data, err := proto.Marshal(&workerservicepb.ExecuteCommandsResponse{Results: []*workerpb.WorkerCommandResult{{
		Type: &workerpb.WorkerCommandResult_WorkflowHeartbeatNotification{WorkflowHeartbeatNotification: result},
	}}})
	s.NoError(err)
	_, err = env.FrontendClient().RespondNexusTaskCompleted(s.Context(), &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  tv.WorkerIdentity(),
		TaskToken: taskToken,
		Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: &commonpb.Payload{
					Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
					Data:     data,
				},
			}},
		}}},
	})
	s.NoError(err)
	s.Await(func(s *WorkflowHeartbeatNotificationSuite) {
		results := capture.Metric(metrics.WorkflowHeartbeatNotificationResults.Name())
		s.Len(results, 1)
		s.Equal(workerpb.WorkflowHeartbeatNotificationResult_Outcome_name[int32(result.GetOutcome())], results[0].Tags["result"])
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *WorkflowHeartbeatNotificationSuite) requireNotificationToken(
	request *workerservicepb.ExecuteCommandsRequest,
	tv *testvars.TestVars,
	runID string,
	heldToken []byte,
) {
	s.Len(request.GetCommands(), 1)
	notification := request.GetCommands()[0].GetWorkflowHeartbeatNotification()
	s.NotNil(notification)
	s.Equal(heldToken, notification.GetTaskToken())
	s.Equal(tv.WorkflowID(), notification.GetExecution().GetWorkflowId())
	s.Equal(runID, notification.GetExecution().GetRunId())
}

// TestNotificationCarriesHeldWorkflowTaskToken checks, for each way a worker can get a Workflow Task, that the
// notification sent while it holds the task carries exactly the token the worker holds.
func (s *WorkflowHeartbeatNotificationSuite) TestNotificationCarriesHeldWorkflowTaskToken() {
	for _, tc := range []struct {
		name string
		// holdWorkflowTask returns the run ID and the token of the started Workflow Task the worker holds.
		holdWorkflowTask func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte)
	}{
		{
			// #scenario-1: Signal notifies a Workflow Task received through normal polling.
			name: "matching poll",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				return start.GetRunId(), s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken()
			},
		},
		{
			// #scenario-2: Signal notifies a Workflow Task received through sticky polling.
			name: "sticky poll",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken(),
					&workflowservice.RespondWorkflowTaskCompletedRequest{StickyAttributes: tv.StickyExecutionAttributes(10 * time.Second)})
				pollCtx, cancelPoll := context.WithCancel(s.Context())
				defer cancelPoll()
				client := env.FrontendClient()
				pollRequest := &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace:              env.Namespace().String(),
					TaskQueue:              tv.StickyTaskQueue(),
					Identity:               tv.WorkerIdentity(),
					WorkerInstanceKey:      tv.WorkerInstanceKey(),
					WorkerControlTaskQueue: tv.ControlQueueName(env.Namespace().String()),
					Capabilities: &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{
						SupportsWorkflowHeartbeatNotification: true,
					},
				}
				type pollResult struct {
					task *workflowservice.PollWorkflowTaskQueueResponse
					err  error
				}
				stickyTask := make(chan pollResult, 1)
				go func() {
					task, err := client.PollWorkflowTaskQueue(pollCtx, pollRequest)
					stickyTask <- pollResult{task: task, err: err}
				}()
				// Matching sends the task to the normal queue unless the sticky queue has a poller.
				s.Await(func(s *WorkflowHeartbeatNotificationSuite) {
					resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
						Namespace: env.Namespace().String(),
						TaskQueue: tv.StickyTaskQueue(),
					})
					s.NoError(err)
					s.NotEmpty(resp.GetPollers())
				}, 10*time.Second, 100*time.Millisecond)
				s.signal(env, tv, start.GetRunId())
				select {
				case result := <-stickyTask:
					s.NoError(result.err)
					s.NotEmpty(result.task.GetTaskToken())
					return start.GetRunId(), result.task.GetTaskToken()
				case <-pollCtx.Done():
					s.T().Fatalf("sticky workflow task poll did not finish: %v", pollCtx.Err())
					return "", nil
				}
			},
		},
		{
			// #scenario-3: Signal notifies the transient retry after a Workflow Task failure.
			name: "transient",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				_, err := env.FrontendClient().RespondWorkflowTaskFailed(s.Context(), &workflowservice.RespondWorkflowTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken(),
					Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
					Identity:  tv.WorkerIdentity(),
				})
				s.NoError(err)
				task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
				s.Equal(int32(2), task.GetAttempt())
				return start.GetRunId(), task.GetTaskToken()
			},
		},
		{
			// #scenario-4: Signal notifies a speculative Workflow Task created by an Update.
			name: "speculative",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken(),
					&workflowservice.RespondWorkflowTaskCompletedRequest{})
				// The update stays unanswered; the test only needs the speculative task it creates.
				task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), s.sendUpdate(env, tv, start.GetRunId()))
				s.Len(task.GetMessages(), 1)
				return start.GetRunId(), task.GetTaskToken()
			},
		},
		{
			// #scenario-5: Signal notifies the inline Workflow Task returned by a workflow heartbeat.
			name: "new workflow task returned inline",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				resp := s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken(),
					&workflowservice.RespondWorkflowTaskCompletedRequest{
						ForceCreateNewWorkflowTask: true,
						ReturnNewWorkflowTask:      true,
					})
				s.NotEmpty(resp.GetWorkflowTask().GetTaskToken())
				return start.GetRunId(), resp.GetWorkflowTask().GetTaskToken()
			},
		},
		{
			// #scenario-6: Signal notifies the Workflow Task returned by eager start.
			name: "eager workflow start",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, true)
				s.NotEmpty(start.GetEagerWorkflowTask().GetTaskToken())
				return start.GetRunId(), start.GetEagerWorkflowTask().GetTaskToken()
			},
		},
	} {
		s.Run(tc.name, func(s *WorkflowHeartbeatNotificationSuite) {
			env := s.newEnv()
			tv := env.Tv()
			runID, heldToken := tc.holdWorkflowTask(s, env, tv)

			s.signal(env, tv, runID)
			s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, runID, heldToken)
		})
	}
}

// TestUpdateAdmissionNotifiesHeldWorkflowTask covers the Update admission path, which skips persistence.
func (s *WorkflowHeartbeatNotificationSuite) TestUpdateAdmissionNotifiesHeldWorkflowTask() {
	hold := func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
		start := s.startWorkflow(env, tv, false)
		return start.GetRunId(), s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken()
	}

	// #scenario-7: Update admission notifies a running Workflow Task.
	s.Run("update", func(s *WorkflowHeartbeatNotificationSuite) {
		env := s.newEnv()
		tv := env.Tv()
		runID, heldToken := hold(s, env, tv)

		updateErr := s.sendUpdate(env, tv, runID)
		s.requireNotificationToken(s.receiveNotification(env, tv, updateErr), tv, runID, heldToken)
	})

	// #scenario-8: Update-with-Start notifies an existing workflow's running Workflow Task.
	s.Run("update-with-start on a running workflow", func(s *WorkflowHeartbeatNotificationSuite) {
		env := s.newEnv()
		tv := env.Tv()
		runID, heldToken := hold(s, env, tv)

		startRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:                uuid.NewString(),
			Namespace:                env.Namespace().String(),
			WorkflowId:               tv.WorkflowID(),
			WorkflowType:             tv.WorkflowType(),
			TaskQueue:                tv.TaskQueue(),
			Identity:                 tv.WorkerIdentity(),
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		}
		updateRequest := s.updateRequest(env, tv, "")
		multiOpErr := s.callInBackground(func(ctx context.Context) error {
			_, err := env.FrontendClient().ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: env.Namespace().String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					{Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{StartWorkflow: startRequest}},
					{Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{UpdateWorkflow: updateRequest}},
				},
			})
			return err
		})
		s.requireNotificationToken(s.receiveNotification(env, tv, multiOpErr), tv, runID, heldToken)
	})
}

// #scenario-9: Signal latency ends when the Workflow Task containing the signal completes.
func (s *WorkflowHeartbeatNotificationSuite) TestSignalProcessingLatencyFromHistory() {
	for _, tc := range []struct {
		name     string
		buffered bool
	}{
		{name: "unbuffered signals", buffered: false},
		{name: "buffered signals", buffered: true},
	} {
		s.Run(tc.name, func(s *WorkflowHeartbeatNotificationSuite) {
			env := s.newEnv()
			tv := env.Tv()
			start := s.startWorkflow(env, tv, false)
			task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
			if !tc.buffered {
				s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{})
			}
			for range 2 {
				s.signal(env, tv, start.GetRunId())
			}
			if tc.buffered {
				s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), task.GetTaskToken())
				task = s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{
					ForceCreateNewWorkflowTask: true,
					ReturnNewWorkflowTask:      true,
				}).GetWorkflowTask()
			} else {
				task = s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
			}
			s.NotEmpty(task.GetTaskToken())
			s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{})

			var signals []*historypb.HistoryEvent
			var completed *historypb.HistoryEvent
			for _, event := range env.RequireHistory(s, tv.WithRunID(start.GetRunId()).WorkflowExecution()) {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signals = append(signals, event)
				}
				if event.GetWorkflowTaskCompletedEventAttributes().GetStartedEventId() == task.GetStartedEventId() {
					completed = event
				}
			}
			s.NotNil(completed)
			s.Len(signals, 2)
			for _, signal := range signals {
				s.Less(signal.GetEventId(), task.GetStartedEventId())
				latency := completed.GetEventTime().AsTime().Sub(signal.GetEventTime().AsTime())
				s.GreaterOrEqual(latency, time.Duration(0))
				s.T().Logf("signal event %d -> Workflow Task completed event %d: %s", signal.GetEventId(), completed.GetEventId(), latency)
			}
		})
	}
}

// #scenario-10: Notification dispatch times out when no worker polls the control queue.
// TestNoPollerTimesOutAtDispatchTimeout checks that Matching answers with a RequestTimeout once
// WorkflowHeartbeatNotificationDispatchTimeout passes, so the dispatch goroutine lives about that long.
func (s *WorkflowHeartbeatNotificationSuite) TestNoPollerTimesOutAtDispatchTimeout() {
	const dispatchTimeout = 1500 * time.Millisecond
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowHeartbeatNotificationRolloutPercent, 100),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowHeartbeatNotificationDispatchTimeout, dispatchTimeout),
	)
	tv := env.Tv()
	start := s.startWorkflow(env, tv, false)
	s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	capture := env.StartNamespaceMetricCapture()

	s.signal(env, tv, start.GetRunId())

	s.Await(func(s *WorkflowHeartbeatNotificationSuite) {
		results := capture.Metric(metrics.WorkflowHeartbeatNotificationResults.Name())
		s.Len(results, 1)
		s.Equal("handler_timeout", results[0].Tags["result"])
	}, 10*time.Second, 100*time.Millisecond)
	latencies := capture.Metric(metrics.WorkflowHeartbeatNotificationDeliveryLatency.Name())
	s.Len(latencies, 1)
	if len(latencies) == 1 {
		_, ok := latencies[0].Value.(time.Duration)
		s.True(ok)
	}
}

// #scenario-11: Signal notifies the retry after a Workflow Task start-to-close timeout.
func (s *WorkflowHeartbeatNotificationSuite) TestNotificationAfterWorkflowTaskTimeout() {
	env := s.newEnv()
	tv := env.Tv()
	start, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
	})
	s.NoError(err)
	task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	retry := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	s.Equal(int32(2), retry.GetAttempt())
	s.NotEqual(task.GetTaskToken(), retry.GetTaskToken())
	s.ContainsHistoryEvents(`
  4 WorkflowTaskTimedOut
`, env.RequireHistory(s, tv.WithRunID(start.GetRunId()).WorkflowExecution()))

	s.signal(env, tv, start.GetRunId())
	s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), retry.GetTaskToken())
}

// #scenario-12: Each workflow heartbeat replaces the token carried by subsequent notifications.
func (s *WorkflowHeartbeatNotificationSuite) TestNotificationAfterRepeatedWorkflowHeartbeats() {
	env := s.newEnv()
	tv := env.Tv()
	start := s.startWorkflow(env, tv, false)
	task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	for range 3 {
		previousToken := task.GetTaskToken()
		task = s.completeWorkflowTask(env, previousToken, &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			ReturnNewWorkflowTask:      true,
		}).GetWorkflowTask()
		s.NotEmpty(task.GetTaskToken())
		s.NotEqual(previousToken, task.GetTaskToken())
		s.signal(env, tv, start.GetRunId())
		s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), task.GetTaskToken())
	}
}

// #scenario-13: A delayed notification gets a token mismatch after the worker switches tasks.
func (s *WorkflowHeartbeatNotificationSuite) TestStaleNotificationAfterWorkflowHeartbeat() {
	env := s.newEnv()
	tv := env.Tv()
	start := s.startWorkflow(env, tv, false)
	task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	s.signal(env, tv, start.GetRunId())
	currentTask := s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		ForceCreateNewWorkflowTask: true,
		ReturnNewWorkflowTask:      true,
	}).GetWorkflowTask()
	s.NotEmpty(currentTask.GetTaskToken())
	s.NotEqual(task.GetTaskToken(), currentTask.GetTaskToken())

	response, notification := s.pollNotification(env, tv, nil)
	s.requireNotificationToken(notification, tv, start.GetRunId(), task.GetTaskToken())
	s.respondNotification(env, tv, response.GetTaskToken(), &workerpb.WorkflowHeartbeatNotificationResult{
		Outcome:          workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_TASK_TOKEN_MISMATCH,
		CurrentTaskToken: currentTask.GetTaskToken(),
	})

	s.signal(env, tv, start.GetRunId())
	s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), currentTask.GetTaskToken())
}

func (s *WorkflowHeartbeatNotificationSuite) TestNotificationDisabled() {
	for _, tc := range []struct {
		name         string
		rollout      int
		capability   bool
		controlQueue bool
	}{
		// #scenario-14: Rollout disabled suppresses notifications for a capable worker.
		{name: "rollout disabled", rollout: 0, capability: true, controlQueue: true},
		// #scenario-15: A worker without WHN capability receives no notification.
		{name: "no capability", rollout: 100, capability: false, controlQueue: true},
		// #scenario-16: A worker without a control queue receives no notification.
		{name: "no control queue", rollout: 100, capability: true, controlQueue: false},
	} {
		s.Run(tc.name, func(s *WorkflowHeartbeatNotificationSuite) {
			env := s.newEnv()
			env.OverrideDynamicConfig(dynamicconfig.WorkflowHeartbeatNotificationRolloutPercent, tc.rollout)
			tv := env.Tv()
			start := s.startWorkflow(env, tv, false)
			poll := &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: tv.TaskQueue(),
				Identity:  tv.WorkerIdentity(),
				Capabilities: &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{
					SupportsWorkflowHeartbeatNotification: tc.capability,
				},
			}
			if tc.controlQueue {
				poll.WorkerControlTaskQueue = tv.ControlQueueName(env.Namespace().String())
			}
			task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), poll)
			s.NoError(err)
			s.NotEmpty(task.GetTaskToken())
			capture := env.StartNamespaceMetricCapture()
			s.signal(env, tv, start.GetRunId())
			s.Len(capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name()), 1)

			ctx, cancel := context.WithTimeout(s.Context(), common.MinLongPollTimeout+time.Second)
			defer cancel()
			response, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: tv.ControlQueueName(env.Namespace().String()),
					Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
				},
				Identity: tv.WorkerIdentity(),
			})
			if err != nil {
				s.True(common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
			} else {
				s.Empty(response.GetTaskToken())
				s.Nil(response.GetRequest())
			}
			s.Empty(capture.Metric(metrics.WorkflowHeartbeatNotificationResults.Name()))
		})
	}
}

// #scenario-17: A timer firing while the worker holds a Workflow Task triggers a notification.
func (s *WorkflowHeartbeatNotificationSuite) TestTimerFiredNotifiesHeldWorkflowTask() {
	env := s.newEnv()
	tv := env.Tv()
	start := s.startWorkflow(env, tv, false)
	task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	capture := env.StartNamespaceMetricCapture()
	currentTask := s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            tv.TimerID(),
				StartToFireTimeout: durationpb.New(time.Second),
			}},
		}},
		ForceCreateNewWorkflowTask: true,
		ReturnNewWorkflowTask:      true,
	}).GetWorkflowTask()
	s.NotEmpty(currentTask.GetTaskToken())
	s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), currentTask.GetTaskToken())
	inputs := capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
	s.Len(inputs, 1)
	s.Equal("TimerFired", inputs[0].Tags["event_type"])
}

// #scenario-18: A workflow cancellation request notifies the held Workflow Task.
func (s *WorkflowHeartbeatNotificationSuite) TestCancellationNotifiesHeldWorkflowTask() {
	env := s.newEnv()
	tv := env.Tv()
	start := s.startWorkflow(env, tv, false)
	task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
	_, err := env.FrontendClient().RequestCancelWorkflowExecution(s.Context(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: tv.WithRunID(start.GetRunId()).WorkflowExecution(),
		RequestId:         tv.RequestID(),
	})
	s.NoError(err)
	s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), task.GetTaskToken())
}

func (s *WorkflowHeartbeatNotificationSuite) TestActivityResultNotifiesHeldWorkflowTask() {
	for _, tc := range []struct {
		name      string
		eventType string
	}{
		// #scenario-19: Activity completion notifies the held Workflow Task.
		{name: "completed", eventType: "ActivityTaskCompleted"},
		// #scenario-20: A final activity failure notifies the held Workflow Task.
		{name: "failed", eventType: "ActivityTaskFailed"},
		// #scenario-21: A final activity timeout notifies the held Workflow Task.
		{name: "timed out", eventType: "ActivityTaskTimedOut"},
		// #scenario-22: Activity cancellation notifies the held Workflow Task.
		{name: "canceled", eventType: "ActivityTaskCanceled"},
	} {
		s.Run(tc.name, func(s *WorkflowHeartbeatNotificationSuite) {
			env := s.newEnv()
			tv := env.Tv()
			start := s.startWorkflow(env, tv, false)
			task := s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
			activityTimeout := 30 * time.Second
			if tc.name == "timed out" {
				activityTimeout = 3 * time.Second
			}
			s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          tv.ActivityID(),
						ActivityType:        tv.ActivityType(),
						TaskQueue:           tv.TaskQueue(),
						StartToCloseTimeout: durationpb.New(activityTimeout),
						RetryPolicy:         &commonpb.RetryPolicy{MaximumAttempts: 1},
					}},
				}},
			})
			activity, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: tv.TaskQueue(),
				Identity:  tv.WorkerIdentity(),
			})
			s.NoError(err)
			s.NotEmpty(activity.GetTaskToken())
			s.signal(env, tv, start.GetRunId())
			task = s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil)
			if tc.name == "canceled" {
				var scheduledEventID int64
				for _, event := range task.GetHistory().GetEvents() {
					if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
						scheduledEventID = event.GetEventId()
					}
				}
				s.NotZero(scheduledEventID)
				task = s.completeWorkflowTask(env, task.GetTaskToken(), &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
						Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
							ScheduledEventId: scheduledEventID,
						}},
					}},
					ForceCreateNewWorkflowTask: true,
					ReturnNewWorkflowTask:      true,
				}).GetWorkflowTask()
			}
			s.NotEmpty(task.GetTaskToken())
			capture := env.StartNamespaceMetricCapture()
			switch tc.name {
			case "completed":
				_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: env.Namespace().String(), TaskToken: activity.GetTaskToken(),
				})
			case "failed":
				_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: env.Namespace().String(), TaskToken: activity.GetTaskToken(),
					Failure: &failurepb.Failure{Message: "activity failed", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "test failure"},
					}},
				})
			case "canceled":
				_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
					Namespace: env.Namespace().String(), TaskToken: activity.GetTaskToken(),
				})
			case "timed out":
				// The activity's start-to-close timer supplies the input.
			default:
				s.T().Fatalf("unknown activity result: %s", tc.name)
			}
			s.NoError(err)
			s.requireNotificationToken(s.receiveNotification(env, tv, nil), tv, start.GetRunId(), task.GetTaskToken())
			inputs := capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
			s.Len(inputs, 1)
			s.Equal(tc.eventType, inputs[0].Tags["event_type"])
		})
	}
}
