package tests

import (
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowHeartbeatNotificationTokenSuite struct {
	parallelsuite.Suite[*WorkflowHeartbeatNotificationTokenSuite]
}

func TestWorkflowHeartbeatNotificationTokenSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowHeartbeatNotificationTokenSuite{})
}

func (s *WorkflowHeartbeatNotificationTokenSuite) startWorkflow(env *testcore.TestEnv, eager bool) *workflowservice.StartWorkflowExecutionResponse {
	tv := env.Tv()
	resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.RequestID(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Identity:                 tv.WorkerIdentity(),
		WorkflowExecutionTimeout: durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(30 * time.Second),
		RequestEagerExecution:    eager,
	})
	s.NoError(err)
	return resp
}

// requireTokenRebuildable checks that the token the worker got equals the one History rebuilds from mutable state.
func (s *WorkflowHeartbeatNotificationTokenSuite) requireTokenRebuildable(env *testcore.TestEnv, taskToken []byte) {
	token, err := tasktoken.NewSerializer().Deserialize(taskToken)
	s.NoError(err)
	s.NotNil(token.GetClock())
	resp, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: token.GetWorkflowId(), RunId: token.GetRunId()},
		// Keep the cached mutable state: a speculative workflow task exists only there.
		SkipForceReload: true,
	})
	s.NoError(err)
	ms := resp.GetCacheMutableState()
	key := definition.NewWorkflowKey(env.NamespaceID().String(), token.GetWorkflowId(), token.GetRunId())
	s.ProtoEqual(token, workflow.NewStartedWorkflowTaskToken(key, ms.GetExecutionInfo()))
}

func (s *WorkflowHeartbeatNotificationTokenSuite) completeWithRebuildableToken(
	env *testcore.TestEnv,
	response *workflowservice.RespondWorkflowTaskCompletedRequest,
) func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
	return func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.requireTokenRebuildable(env, task.GetTaskToken())
		return response, nil
	}
}

func (s *WorkflowHeartbeatNotificationTokenSuite) TestStartedWorkflowTaskTokenRebuild() {
	s.Run("matching poll", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		s.startWorkflow(env, false)
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(env.Tv(),
			s.completeWithRebuildableToken(env, &workflowservice.RespondWorkflowTaskCompletedRequest{}))
		s.NoError(err)
	})

	s.Run("sticky poll", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()
		start := s.startWorkflow(env, false)
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					StickyAttributes: tv.StickyExecutionAttributes(10 * time.Second),
				}, nil
			})
		s.NoError(err)
		stickyResult := make(chan error, 1)
		go func() {
			_, pollErr := env.TaskPoller().
				PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: tv.StickyTaskQueue()}).
				HandleTask(tv, s.completeWithRebuildableToken(env, &workflowservice.RespondWorkflowTaskCompletedRequest{}))
			stickyResult <- pollErr
		}()
		// Matching sends the task to the normal queue unless the sticky queue has a poller.
		s.Await(func(s *WorkflowHeartbeatNotificationTokenSuite) {
			resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: tv.StickyTaskQueue(),
			})
			s.NoError(err)
			s.NotEmpty(resp.GetPollers())
		}, 10*time.Second, 100*time.Millisecond)
		_, err = env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: tv.WithRunID(start.GetRunId()).WorkflowExecution(),
			SignalName:        tv.SignalName(),
			RequestId:         tv.Any().String(),
		})
		s.NoError(err)
		s.NoError(s.Rcv(stickyResult))
	})

	s.Run("transient", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		s.startWorkflow(env, false)
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(env.Tv(),
			func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return nil, errors.New("fail the first attempt")
			})
		s.NoError(err)
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(env.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Equal(int32(2), task.GetAttempt())
				s.requireTokenRebuildable(env, task.GetTaskToken())
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
		s.NoError(err)
	})

	s.Run("speculative", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()
		start := s.startWorkflow(env, false)
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
		s.NoError(err)
		updateResult := make(chan error, 1)
		go func() {
			_, updateErr := env.FrontendClient().UpdateWorkflowExecution(s.Context(), &workflowservice.UpdateWorkflowExecutionRequest{
				Namespace:         env.Namespace().String(),
				WorkflowExecution: tv.WithRunID(start.GetRunId()).WorkflowExecution(),
				WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
				Request: &updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: tv.UpdateID()},
					Input: &updatepb.Input{Name: tv.HandlerName()},
				},
			})
			updateResult <- updateErr
		}()
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Len(task.GetMessages(), 1)
				s.requireTokenRebuildable(env, task.GetTaskToken())
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: env.UpdateAcceptCompleteMessages(tv, task.GetMessages()[0]),
				}, nil
			})
		s.NoError(err)
		s.NoError(s.Rcv(updateResult))
	})

	s.Run("new workflow task returned inline", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		s.startWorkflow(env, false)
		resp, err := env.TaskPoller().PollAndHandleWorkflowTask(env.Tv(),
			func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					ForceCreateNewWorkflowTask: true,
					ReturnNewWorkflowTask:      true,
				}, nil
			})
		s.NoError(err)
		s.NotNil(resp.GetWorkflowTask())
		_, err = env.TaskPoller().HandleWorkflowTask(env.Tv(), resp.GetWorkflowTask(),
			s.completeWithRebuildableToken(env, &workflowservice.RespondWorkflowTaskCompletedRequest{}))
		s.NoError(err)
	})

	s.Run("eager workflow start", func(s *WorkflowHeartbeatNotificationTokenSuite) {
		env := testcore.NewEnv(s.T())
		start := s.startWorkflow(env, true)
		s.NotNil(start.GetEagerWorkflowTask())
		_, err := env.TaskPoller().HandleWorkflowTask(env.Tv(), start.GetEagerWorkflowTask(),
			s.completeWithRebuildableToken(env, &workflowservice.RespondWorkflowTaskCompletedRequest{}))
		s.NoError(err)
	})
}
