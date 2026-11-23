package tests

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
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
) *workflowservice.PollWorkflowTaskQueueResponse {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	task, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              taskQueue,
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: tv.ControlQueueName(env.Namespace().String()),
		Capabilities: &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{
			SupportsWorkflowHeartbeatNotification: true,
		},
	})
	s.NoError(err)
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
		RequestId:         tv.Any().String(),
	})
	s.NoError(err)
}

// receiveNotification polls the worker control queue like an SDK worker and returns the notification it carries.
func (s *WorkflowHeartbeatNotificationSuite) receiveNotification(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
) *workerservicepb.ExecuteCommandsRequest {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tv.ControlQueueName(env.Namespace().String()), Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	var request workerservicepb.ExecuteCommandsRequest
	s.NoError(payload.Decode(resp.GetRequest().GetStartOperation().GetPayload(), &request))
	return &request
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
				return start.GetRunId(), s.pollWorkflowTask(env, tv, tv.TaskQueue()).GetTaskToken()
			},
		},
		{
			name: "sticky poll",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue()).GetTaskToken(),
					&workflowservice.RespondWorkflowTaskCompletedRequest{StickyAttributes: tv.StickyExecutionAttributes(10 * time.Second)})
				stickyTask := make(chan *workflowservice.PollWorkflowTaskQueueResponse, 1)
				go func() { stickyTask <- s.pollWorkflowTask(env, tv, tv.StickyTaskQueue()) }()
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
				return start.GetRunId(), s.Rcv(stickyTask).GetTaskToken()
			},
		},
		{
			name: "transient",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				_, err := env.FrontendClient().RespondWorkflowTaskFailed(s.Context(), &workflowservice.RespondWorkflowTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: s.pollWorkflowTask(env, tv, tv.TaskQueue()).GetTaskToken(),
					Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
					Identity:  tv.WorkerIdentity(),
				})
				s.NoError(err)
				task := s.pollWorkflowTask(env, tv, tv.TaskQueue())
				s.Equal(int32(2), task.GetAttempt())
				return start.GetRunId(), task.GetTaskToken()
			},
		},
		{
			name: "speculative",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue()).GetTaskToken(),
					&workflowservice.RespondWorkflowTaskCompletedRequest{})
				go func() {
					// The update stays unanswered; the test only needs the speculative task it creates.
					_, _ = env.FrontendClient().UpdateWorkflowExecution(s.Context(), &workflowservice.UpdateWorkflowExecutionRequest{
						Namespace:         env.Namespace().String(),
						WorkflowExecution: tv.WithRunID(start.GetRunId()).WorkflowExecution(),
						WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
						Request: &updatepb.Request{
							Meta:  &updatepb.Meta{UpdateId: tv.UpdateID()},
							Input: &updatepb.Input{Name: tv.HandlerName()},
						},
					})
				}()
				task := s.pollWorkflowTask(env, tv, tv.TaskQueue())
				s.Len(task.GetMessages(), 1)
				return start.GetRunId(), task.GetTaskToken()
			},
		},
		{
			name: "new workflow task returned inline",
			holdWorkflowTask: func(s *WorkflowHeartbeatNotificationSuite, env *testcore.TestEnv, tv *testvars.TestVars) (string, []byte) {
				start := s.startWorkflow(env, tv, false)
				resp := s.completeWorkflowTask(env, s.pollWorkflowTask(env, tv, tv.TaskQueue()).GetTaskToken(),
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
			request := s.receiveNotification(env, tv)

			s.Len(request.GetCommands(), 1)
			notification := request.GetCommands()[0].GetWorkflowHeartbeatNotification()
			s.NotNil(notification)
			s.Equal(heldToken, notification.GetTaskToken())
			s.Equal(tv.WorkflowID(), notification.GetExecution().GetWorkflowId())
			s.Equal(runID, notification.GetExecution().GetRunId())
		})
	}
}
