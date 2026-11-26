package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSessionActivityWithoutPlacementRollsBackCommands(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableStickyActivityExecution, true))
	tv := env.Tv()
	started, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	poller := taskpoller.New(t, env.FrontendClient(), env.Namespace().String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		var commands []*commandpb.Command
		for _, useSession := range []bool{false, true} {
			sessionOptions := &sessionpb.ActivitySessionOptions{}
			id := "regular-activity"
			if useSession {
				id = "session-activity"
				sessionOptions.Placement = &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}
			}
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: id, ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
					StartToCloseTimeout: durationpb.New(time.Second),
					SessionOptions:      sessionOptions,
				}},
			})
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
	})
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.ErrorContains(t, err, "require the workflow to be bound to a worker servicing sessions")
	execution := tv.WithRunID(started.RunId).WorkflowExecution()
	failed := false
	for _, event := range env.GetHistory(env.Namespace().String(), execution) {
		require.NotEqual(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, event.GetEventType())
		if attrs := event.GetWorkflowTaskFailedEventAttributes(); attrs != nil {
			failed = true
			require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, attrs.GetCause())
		}
	}
	require.True(t, failed)
	description, err := env.FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), Execution: execution,
	})
	require.NoError(t, err)
	require.Empty(t, description.GetPendingActivities())
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.GetWorkflowExecutionInfo().GetStatus())
}

func TestWorkflowSessionActivityRouting(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableStickyActivityExecution, true))
	tv := env.Tv()
	_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	poller := taskpoller.New(t, env.FrontendClient(), env.Namespace().String())
	// A worker polls its sticky activity queue from startup; without a recent poll Matching treats it as gone.
	_, err = poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: multiSessionWorkerOptions(tv.StickyTaskQueue().GetName()),
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	}).HandleTask(tv, func(*workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.FailNow(t, "no activity task expected while registering the sticky activity poller")
		return nil, nil
	}, taskpoller.WithTimeout(stickyPollTimeout))
	require.Error(t, err)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		SessionOptions: multiSessionWorkerOptions(), WorkerInstanceKey: tv.StickyTaskQueue().GetName(),
	}).HandleTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		var commands []*commandpb.Command
		for _, useSession := range []bool{false, true} {
			sessionOptions := &sessionpb.ActivitySessionOptions{}
			id := "regular-activity"
			if useSession {
				id = "session-activity"
				sessionOptions.Placement = &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}
			}
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: id, ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
					ScheduleToCloseTimeout: durationpb.New(time.Minute), ScheduleToStartTimeout: durationpb.New(time.Minute),
					StartToCloseTimeout: durationpb.New(30 * time.Second), RequestEagerExecution: false,
					RetryPolicy:    &commonpb.RetryPolicy{InitialInterval: durationpb.New(10 * time.Millisecond), BackoffCoefficient: 1, MaximumAttempts: 2},
					SessionOptions: sessionOptions,
				}},
			})
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{StickyAttributes: tv.StickyExecutionAttributes(time.Minute), Commands: commands}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	stickyPoller := poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: multiSessionWorkerOptions(tv.StickyTaskQueue().GetName()),
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	})
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, "session-activity", task.ActivityId)
		require.Equal(t, int32(1), task.Attempt)
		return nil, temporal.NewApplicationError("retry on the same sticky destination", "retryable")
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, "session-activity", task.ActivityId)
		require.Equal(t, int32(2), task.Attempt)
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	_, err = poller.PollAndHandleActivityTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, "regular-activity", task.ActivityId)
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	description, err := env.FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), Execution: tv.WorkflowExecution(),
	})
	require.NoError(t, err)
	require.Empty(t, description.GetPendingActivities())
}

// TestMultiSessionWorkerRejectedWithVersioning tests that a multi-session worker cannot poll with worker versioning:
// its bindings do not follow versioning transitions yet.
func (s *Versioning3Suite) TestMultiSessionWorkerRejectedWithVersioning() {
	env := s.setupEnv(testcore.WithDynamicConfig(dynamicconfig.EnableStickyActivityExecution, true))
	tv := env.Tv()
	env.updateTaskQueueDeploymentDataWithRoutingConfig(s, tv, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimeNowPtrUtc(), RevisionNumber: 1,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT}}, nil, tqTypeWf)
	if independentDeployment || behavior == vbUnpinned {
		activityVersion := tv
		if independentDeployment {
			activityVersion = tv.WithBuildIDNumber(2)
		}
		env.updateTaskQueueDeploymentDataWithRoutingConfig(s, activityVersion, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(activityVersion.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimeNowPtrUtc(), RevisionNumber: 1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{activityVersion.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT}}, nil, tqTypeAct)
	}
	if behavior == vbPinned {
		env.startWorkflow(s, tv, tv.VersioningOverridePinned())
	} else {
		env.startWorkflow(s, tv, nil)
	}

	expectStickyRejection := behavior == vbUnpinned && independentDeployment
	var rejectedDispatch chan error
	if expectStickyRejection {
		rejectedDispatch = make(chan error, 1)
		defer env.InjectResponseFault(func(req, _ any, handlerErr error) error {
			addRequest, ok := req.(*matchingservice.AddActivityTaskRequest)
			if !ok || addRequest.GetTaskQueue().GetName() != tv.StickyTaskQueue().GetName() {
				return nil
			}
			select {
			case rejectedDispatch <- handlerErr:
			default:
			}
			return handlerErr
		})()
	}

	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	stickyActivityPoll := &workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), DeploymentOptions: tv.WorkerDeploymentOptions(true),
		SessionOptions: multiSessionWorkerOptions(tv.StickyTaskQueue().GetName()), WorkerInstanceKey: tv.WorkerInstanceKey(),
	}
	// A worker polls its sticky activity queue from startup; without a recent poll Matching treats it as gone.
	_, err := poller.PollActivityTask(stickyActivityPoll).HandleTask(tv, func(*workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		s.FailNow("no activity task expected while registering the sticky activity poller")
		return nil, nil
	}, taskpoller.WithTimeout(stickyPollTimeout))
	s.Require().Error(err)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
		TaskQueue:         tv.TaskQueue(),
		SessionOptions:    multiSessionWorkerOptions(tv.StickyTaskQueue().GetName()),
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	}).HandleTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		response := env.respondWftWithActivities(tv, tv, true, behavior, "session-activity")
		response.Commands[0].GetScheduleActivityTaskCommandAttributes().SessionOptions = &sessionpb.ActivitySessionOptions{
			Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}},
		}
		return response, nil
	}, taskpoller.WithTimeout(time.Minute))
	s.Require().NoError(err)
	if expectStickyRejection {
		s.Require().ErrorAs(await.Rcv(s.T(), rejectedDispatch), new(*serviceerrors.StickyWorkerUnavailable))
		description, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(), Execution: tv.WorkflowExecution(),
		})
		s.Require().NoError(err)
		s.Require().Len(description.GetPendingActivities(), 1)
		s.Require().Equal("session-activity", description.GetPendingActivities()[0].GetActivityId())
		s.Require().Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.GetPendingActivities()[0].GetState())
		s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.GetWorkflowExecutionInfo().GetStatus())
		return
	}

	_, err = poller.PollActivityTask(stickyActivityPoll).HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		s.Require().Equal("session-activity", task.ActivityId)
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(s.Context()))
	s.Require().NoError(err)
	description, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), Execution: tv.WorkflowExecution(),
	})
	s.Require().ErrorAs(err, new(*serviceerror.InvalidArgument))
	s.Require().ErrorContains(err, "multi-session workers do not support worker versioning")
}
