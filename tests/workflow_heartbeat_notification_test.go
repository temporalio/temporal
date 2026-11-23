package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
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

// receiveNotification polls the worker control queue like an SDK worker and returns the notification it carries.
func (s *WorkflowHeartbeatNotificationSuite) receiveNotification(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	background <-chan error,
) *workerservicepb.ExecuteCommandsRequest {
	resp := callWatchingBackground(s, background, func(ctx context.Context) (*workflowservice.PollNexusTaskQueueResponse, error) {
		return env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tv.ControlQueueName(env.Namespace().String()), Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
	})
	var request workerservicepb.ExecuteCommandsRequest
	s.NoError(payload.Decode(resp.GetRequest().GetStartOperation().GetPayload(), &request))
	return &request
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
			name: "matching poll",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				return start.GetRunId(), s.pollWorkflowTask(env, tv, tv.TaskQueue(), nil).GetTaskToken()
			},
		},
		{
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

	s.Run("update", func(s *WorkflowHeartbeatNotificationSuite) {
		env := s.newEnv()
		tv := env.Tv()
		runID, heldToken := hold(s, env, tv)

		updateErr := s.sendUpdate(env, tv, runID)
		s.requireNotificationToken(s.receiveNotification(env, tv, updateErr), tv, runID, heldToken)
	})

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

// TestNoPollerTimesOutAtDispatchTimeout checks that Matching answers with a RequestTimeout once
// WorkflowHeartbeatNotificationDispatchTimeout passes, so the dispatch goroutine lives about that long.
func (s *WorkflowHeartbeatNotificationSuite) TestNoPollerTimesOutAtDispatchTimeout() {
	const dispatchTimeout = 500 * time.Millisecond
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
