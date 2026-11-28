package workerdeployment

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func syncUnversionedRampTestWorkflow(
	ctx workflow.Context,
	args *deploymentspb.WorkerDeploymentWorkflowArgs,
) error {
	runner := &WorkflowRunner{WorkerDeploymentWorkflowArgs: args}
	return runner.syncUnversionedRamp(ctx, &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: timestamppb.New(workflow.Now(ctx)),
		RampingSinceTime:  timestamppb.New(workflow.Now(ctx)),
		RampPercentage:    50,
	})
}

func syncVersionDataTestWorkflow(
	ctx workflow.Context,
	args *deploymentspb.WorkerDeploymentVersionWorkflowArgs,
) error {
	runner := &VersionWorkflowRunner{WorkerDeploymentVersionWorkflowArgs: args}
	return runner.syncVersionDataToTaskQueues(ctx, &deploymentspb.DeploymentVersionData{
		Version:           args.VersionState.Version,
		RoutingUpdateTime: timestamppb.New(workflow.Now(ctx)),
	})
}

func TestSyncUnversionedRampRetriesUntilBatchPropagationCompletes(t *testing.T) {
	t.Parallel()

	tv := testvars.New(t)
	var testSuite testsuite.WorkflowTestSuite
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(syncUnversionedRampTestWorkflow)

	var activities *Activities
	env.OnActivity(activities.DescribeVersionFromWorkerDeployment, mock.Anything, mock.Anything).Return(
		&deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult{
			TaskQueueInfos: []*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{
				{
					Name: tv.TaskQueue().Name,
					Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
			},
		},
		nil,
	)

	var syncAttempts atomic.Int32
	env.OnActivity(activities.SyncDeploymentVersionUserDataFromWorkerDeployment, mock.Anything, mock.Anything).Return(
		func(context.Context, *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if syncAttempts.Add(1) <= int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) {
				return nil, errors.New("transient sync failure")
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{
				TaskQueueMaxVersions: map[string]int64{tv.TaskQueue().Name: 1},
			}, nil
		},
	)

	var propagationAttempts atomic.Int32
	env.OnActivity(activities.CheckUnversionedRampUserDataPropagation, mock.Anything, mock.Anything).Return(
		func(context.Context, *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			if propagationAttempts.Add(1) <= int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) {
				return errors.New("transient propagation failure")
			}
			return nil
		},
	)

	env.ExecuteWorkflow(syncUnversionedRampTestWorkflow, &deploymentspb.WorkerDeploymentWorkflowArgs{
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			SyncBatchSize: 1,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: tv.DeploymentVersionString(),
			},
		},
	})

	require.NoError(t, env.GetWorkflowError())
	expectedAttempts := int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) + 1
	require.Equal(t, expectedAttempts, syncAttempts.Load())
	require.Equal(t, expectedAttempts, propagationAttempts.Load())
	env.AssertExpectations(t)
}

func TestSyncVersionDataRetriesUntilBatchPropagationCompletes(t *testing.T) {
	t.Parallel()

	tv := testvars.New(t)
	var testSuite testsuite.WorkflowTestSuite
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(syncVersionDataTestWorkflow)

	var activities *VersionActivities
	var syncAttempts atomic.Int32
	env.OnActivity(activities.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(context.Context, *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if syncAttempts.Add(1) <= int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) {
				return nil, errors.New("transient sync failure")
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{
				TaskQueueMaxVersions: map[string]int64{tv.TaskQueue().Name: 1},
			}, nil
		},
	)

	var propagationAttempts atomic.Int32
	env.OnActivity(activities.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(
		func(context.Context, *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			if propagationAttempts.Add(1) <= int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) {
				return errors.New("transient propagation failure")
			}
			return nil
		},
	)

	env.ExecuteWorkflow(syncVersionDataTestWorkflow, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		VersionState: &deploymentspb.VersionLocalState{
			Version:       tv.DeploymentVersion(),
			SyncBatchSize: 1,
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				tv.TaskQueue().Name: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
		},
	})

	require.NoError(t, env.GetWorkflowError())
	expectedAttempts := int32(defaultActivityOptions.RetryPolicy.MaximumAttempts) + 1
	require.Equal(t, expectedAttempts, syncAttempts.Load())
	require.Equal(t, expectedAttempts, propagationAttempts.Load())
	env.AssertExpectations(t)
}
