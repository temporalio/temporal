package passivepath

import (
	"context"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerVersioningSuite struct {
	parallelsuite.Suite[*WorkerVersioningSuite]
}

func TestWorkerVersioningSuite(t *testing.T) { parallelsuite.Run(t, &WorkerVersioningSuite{}) }

func (s *WorkerVersioningSuite) TestWorkerVersioningReschedulesPendingTasksThroughPassiveApply() {
	t := s.T()
	logger := log.NewNoopLogger()
	harness := NewHarness(logger)
	// Active execution can return the next workflow task inline without persisting
	// its transfer task. The allowance permits only extra passive tasks; missing
	// tasks still fail the comparison.
	harness.AllowPassiveOnlyTaskTypes("transfer/TransferWorkflowTask")

	tc := newSingleClusterWithGlobalNamespace(t, logger)
	t.Cleanup(tc.OverrideDynamicConfig(t, dynamicconfig.MatchingNumTaskqueueReadPartitions, 1))
	t.Cleanup(tc.OverrideDynamicConfig(t, dynamicconfig.MatchingNumTaskqueueWritePartitions, 1))

	namespaceName := "passivepath-versioning-" + common.GenerateRandomString(5)
	namespaceID := registerGlobalNamespace(t, tc, namespaceName)
	t.Cleanup(tc.InjectHook(t, testhooks.NewHook[testhooks.HistoryPassiveReplicationTestHook](
		testhooks.HistoryPassiveReplicationTest,
		harness,
	), namespaceID))

	tv1 := testvars.New(t).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	s.setCurrentDeployment(tc, namespaceID.String(), tv1, 1)

	startResponse, err := tc.FrontendClient().StartWorkflowExecution(
		testcore.NewContext(s.Context()),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:    tv1.RequestID(),
			Namespace:    namespaceName,
			WorkflowId:   tv1.WorkflowID(),
			WorkflowType: tv1.WorkflowType(),
			TaskQueue:    tv1.TaskQueue(),
			Identity:     tv1.WorkerIdentity(),
		},
	)
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tv1.WorkflowID(),
		RunId:      startResponse.GetRunId(),
	}

	poller := taskpoller.New(t, tc.FrontendClient(), namespaceName)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tv1.TaskQueue(),
		DeploymentOptions: tv1.WorkerDeploymentOptions(true),
	}).HandleTask(tv1, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.ProtoEqual(execution, task.GetWorkflowExecution())
		return versionedWorkflowTaskCompletion(tv1, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv1.ActivityID(),
					ActivityType:           tv1.ActivityType(),
					TaskQueue:              tv1.TaskQueue(),
					ScheduleToCloseTimeout: durationpb.New(time.Minute),
					StartToCloseTimeout:    durationpb.New(time.Minute),
				},
			},
		}), nil
	})
	s.NoError(err)

	// The signal creates an unstarted WFT before the activity initiates the transition.
	// Without it, StartDeploymentTransition would create a new WFT instead of exercising
	// the rescheduling path fixed by #8680.
	_, err = tc.FrontendClient().SignalWorkflowExecution(
		testcore.NewContext(s.Context()),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         namespaceName,
			WorkflowExecution: execution,
			SignalName:        tv1.SignalName(),
			Identity:          tv1.WorkerIdentity(),
		},
	)
	s.NoError(err)

	s.setCurrentDeployment(tc, namespaceID.String(), tv2, 2)

	type activityPollResult struct {
		task *workflowservice.PollActivityTaskQueueResponse
		err  error
	}
	activityResultCh := make(chan activityPollResult, 1)
	activityPollCtx, cancelActivityPoll := context.WithCancel(t.Context())
	activityPollDone := make(chan struct{})
	defer func() {
		cancelActivityPoll()
		await.Rcv(t, activityPollDone)
	}()
	go func() {
		defer close(activityPollDone)
		var activityTask *workflowservice.PollActivityTaskQueueResponse
		_, pollErr := poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
			TaskQueue:         tv2.TaskQueue(),
			DeploymentOptions: tv2.WorkerDeploymentOptions(true),
		}).HandleTask(tv2, func(
			task *workflowservice.PollActivityTaskQueueResponse,
		) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			activityTask = task
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		}, taskpoller.WithContext(activityPollCtx), taskpoller.WithTimeout(60*time.Second))
		activityResultCh <- activityPollResult{task: activityTask, err: pollErr}
	}()

	// The transition is started asynchronously by the v2 activity poll, so wait
	// until the version transition is recorded on the mutable state.
	s.Await(func(s *WorkerVersioningSuite) {
		describeResponse, err := tc.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(s.Context()),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespaceName,
				Execution: execution,
			},
		)
		s.NoError(err)
		s.ProtoEqual(
			tv2.DeploymentVersionTransition(),
			describeResponse.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersionTransition(),
		)
	}, 10*time.Second, 100*time.Millisecond)

	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tv2.TaskQueue(),
		DeploymentOptions: tv2.WorkerDeploymentOptions(true),
	}).HandleTask(tv2, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.ProtoEqual(execution, task.GetWorkflowExecution())
		return versionedWorkflowTaskCompletion(tv2), nil
	})
	s.NoError(err)

	activityResult := await.Rcv(t, activityResultCh)
	s.NoError(activityResult.err)
	s.ProtoEqual(execution, activityResult.task.GetWorkflowExecution())
	s.ProtoEqual(tv1.ActivityType(), activityResult.task.GetActivityType())

	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tv2.TaskQueue(),
		DeploymentOptions: tv2.WorkerDeploymentOptions(true),
	}).HandleTask(tv2, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.ProtoEqual(execution, task.GetWorkflowExecution())
		return versionedWorkflowTaskCompletion(tv2, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}), nil
	})
	s.NoError(err)

	describeResponse, err := tc.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(s.Context()),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespaceName,
			Execution: execution,
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, describeResponse.GetWorkflowExecutionInfo().GetStatus())
	versioningInfo := describeResponse.GetWorkflowExecutionInfo().GetVersioningInfo()
	s.Equal(enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, versioningInfo.GetBehavior())
	s.Equal(tv2.DeploymentSeries(), versioningInfo.GetDeploymentVersion().GetDeploymentName())
	s.Equal(tv2.BuildID(), versioningInfo.GetDeploymentVersion().GetBuildId())

	s.Empty(harness.Bailouts())
	s.Empty(harness.ApplyErrors())
	s.Positive(harness.Diverted())
	s.Equal(harness.Diverted(), harness.Applied())
	s.Positive(harness.StandbyExecutions())
}

func (s *WorkerVersioningSuite) setCurrentDeployment(
	tc *testcore.TestCluster,
	namespaceID string,
	tv *testvars.TestVars,
	revision int64,
) {
	s.T().Helper()
	_, err := tc.MatchingClient().SyncDeploymentUserData(
		testcore.NewContext(s.Context()),
		&matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    namespaceID,
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY},
			DeploymentName: tv.DeploymentSeries(),
			UpdateRoutingConfig: &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamppb.Now(),
				RevisionNumber:            revision,
			},
			UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
				tv.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		},
	)
	s.NoError(err)
}

func versionedWorkflowTaskCompletion(
	tv *testvars.TestVars,
	commands ...*commandpb.Command,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands:           commands,
		VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		DeploymentOptions:  tv.WorkerDeploymentOptions(true),
	}
}
