package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkerVersioningReschedulesPendingTasksThroughPassiveApply(t *testing.T) {
	logger := log.NewNoopLogger()
	harness := NewHarness(logger)
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
	setCurrentDeployment(t, tc, namespaceID.String(), tv1, 1)

	startResponse, err := tc.FrontendClient().StartWorkflowExecution(
		testcore.NewContext(t.Context()),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:    tv1.RequestID(),
			Namespace:    namespaceName,
			WorkflowId:   tv1.WorkflowID(),
			WorkflowType: tv1.WorkflowType(),
			TaskQueue:    tv1.TaskQueue(),
			Identity:     tv1.WorkerIdentity(),
		},
	)
	require.NoError(t, err)
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
		protorequire.ProtoEqual(t, execution, task.GetWorkflowExecution())
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
	require.NoError(t, err)

	// The signal creates an unstarted WFT before the activity initiates the transition.
	// Without it, StartDeploymentTransition would create a new WFT instead of exercising
	// the rescheduling path fixed by #8680.
	_, err = tc.FrontendClient().SignalWorkflowExecution(
		testcore.NewContext(t.Context()),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         namespaceName,
			WorkflowExecution: execution,
			SignalName:        tv1.SignalName(),
			Identity:          tv1.WorkerIdentity(),
		},
	)
	require.NoError(t, err)

	setCurrentDeployment(t, tc, namespaceID.String(), tv2, 2)

	type activityPollResult struct {
		task *workflowservice.PollActivityTaskQueueResponse
		err  error
	}
	activityResultCh := make(chan activityPollResult, 1)
	activityPollCtx, cancelActivityPoll := context.WithCancel(t.Context())
	activityPollDone := make(chan struct{})
	defer func() {
		cancelActivityPoll()
		<-activityPollDone
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
		}, taskpoller.WithContext(activityPollCtx), taskpoller.WithTimeout(30*time.Second))
		activityResultCh <- activityPollResult{task: activityTask, err: pollErr}
	}()

	await.RequireTrue(t, func() bool {
		describeResponse, describeErr := tc.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(t.Context()),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespaceName,
				Execution: execution,
			},
		)
		transitionVersion := describeResponse.GetWorkflowExecutionInfo().GetVersioningInfo().
			GetVersionTransition().GetDeploymentVersion()
		return describeErr == nil &&
			transitionVersion.GetDeploymentName() == tv2.DeploymentSeries() &&
			transitionVersion.GetBuildId() == tv2.BuildID()
	}, 10*time.Second, 100*time.Millisecond)

	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tv2.TaskQueue(),
		DeploymentOptions: tv2.WorkerDeploymentOptions(true),
	}).HandleTask(tv2, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		protorequire.ProtoEqual(t, execution, task.GetWorkflowExecution())
		return versionedWorkflowTaskCompletion(tv2), nil
	})
	require.NoError(t, err)

	activityResult := <-activityResultCh
	require.NoError(t, activityResult.err)
	protorequire.ProtoEqual(t, execution, activityResult.task.GetWorkflowExecution())
	protorequire.ProtoEqual(t, tv1.ActivityType(), activityResult.task.GetActivityType())

	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tv2.TaskQueue(),
		DeploymentOptions: tv2.WorkerDeploymentOptions(true),
	}).HandleTask(tv2, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		protorequire.ProtoEqual(t, execution, task.GetWorkflowExecution())
		return versionedWorkflowTaskCompletion(tv2, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}), nil
	})
	require.NoError(t, err)

	describeResponse, err := tc.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(t.Context()),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespaceName,
			Execution: execution,
		},
	)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, describeResponse.GetWorkflowExecutionInfo().GetStatus())
	versioningInfo := describeResponse.GetWorkflowExecutionInfo().GetVersioningInfo()
	require.Equal(t, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, versioningInfo.GetBehavior())
	require.Equal(t, tv2.DeploymentSeries(), versioningInfo.GetDeploymentVersion().GetDeploymentName())
	require.Equal(t, tv2.BuildID(), versioningInfo.GetDeploymentVersion().GetBuildId())

	require.Empty(t, harness.Bailouts())
	require.Empty(t, harness.ApplyErrors())
	require.Positive(t, harness.Diverted())
	require.Equal(t, harness.Diverted(), harness.Applied())
	require.Positive(t, harness.StandbyExecutions())
}

func setCurrentDeployment(
	t *testing.T,
	tc *testcore.TestCluster,
	namespaceID string,
	tv *testvars.TestVars,
	revision int64,
) {
	t.Helper()
	_, err := tc.MatchingClient().SyncDeploymentUserData(
		testcore.NewContext(t.Context()),
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
	require.NoError(t, err)
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
