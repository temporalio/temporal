package workerdeployment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerDeploymentSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller             *gomock.Controller
	env                    *testsuite.TestWorkflowEnvironment
	workerDeploymentClient *ClientImpl
}

func TestWorkerDeploymentSuite(t *testing.T) {
	suite.Run(t, new(WorkerDeploymentSuite))
}

func (s *WorkerDeploymentSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(Workflow)

	// Initialize an empty ClientImpl to use its helper methods
	s.workerDeploymentClient = &ClientImpl{}
}

func (s *WorkerDeploymentSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

// Test_SetCurrentVersion_RejectStaleConcurrentUpdate tests that a stale concurrent update is rejected.
//
// The scenario that this test is testing is as follows:
// Two *identical* (same version, same identity) SetCurrentVersion updates are sent such that
// both of them are accepted by the validator and are scheduled to be processed.
// Since updates are processed sequentially, update #1 is processed first and then update #2.
// Update #2 should be rejected because by the time it is processed, update #1 would have been
// processed and the state would have changed.
func (s *WorkerDeploymentSuite) Test_SetCurrentVersion_RejectStaleConcurrentUpdate() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	updateArgs := &deploymentspb.SetCurrentVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 tv.DeploymentVersionString(),
		IgnoreMissingTaskQueues: true,
	}

	s.env.RegisterDelayedCallback(func() {
		// Firing update #1
		s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
				// Firing Update #2 which shall gets processed after Update #1 gets completed.
				s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						s.Fail("update #2 should have been accepted by the validator")
					},
					OnAccept: func() {
					},
					OnComplete: func(a interface{}, err error) {
						// Update #2 clears the validator and waits for the first update to complete. Once it starts
						// being processed, it should be rejected since completion of the first update changed the state.
						s.ErrorContains(err, errNoChangeType)
					},
				}, updateArgs)
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)

	}, 0*time.Millisecond)

	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return 1000
		}
		return Workflow(ctx, maxVersionsGetter, args)
	}

	s.env.ExecuteWorkflow(deploymentWorkflow, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		// Add version to deployment's local state since it's a prerequisite for SetCurrentVersion.
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				tv.DeploymentVersionString(): {
					Version: tv.DeploymentVersionString(),
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_SetRampingVersion_RejectStaleConcurrentUpdate tests that a stale concurrent update is rejected.
//
// The scenario that this test is testing is as follows:
// Two *identical* (same version, same identity) SetRampingVersion updates are sent such that
// both of them are accepted by the validator and are scheduled to be processed.
// Since updates are processed sequentially, update #1 is processed first and then update #2.
// Update #2 should be rejected because by the time it is processed, update #1 would have been
// processed and the state would have changed.
func (s *WorkerDeploymentSuite) Test_SetRampingVersion_RejectStaleConcurrentUpdate() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	updateArgs := &deploymentspb.SetRampingVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 tv.DeploymentVersionString(),
		Percentage:              50,
		IgnoreMissingTaskQueues: true,
	}

	s.env.RegisterDelayedCallback(func() {
		// Firing Update #1.
		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
				// Firing Update #2 which shall get processed after Update #1 gets completed.
				s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						s.Fail("update #2 should have been accepted by the validator")
					},
					OnAccept: func() {
					},
					OnComplete: func(a interface{}, err error) {
						// Update #2 clears the validator and waits for the first update to complete. Once it starts
						// being processed, it should be rejected since completion of the first update changed the state.
						s.ErrorContains(err, errNoChangeType)
					},
				}, updateArgs)

			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)

	}, 0*time.Millisecond)

	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return 1000
		}
		return Workflow(ctx, maxVersionsGetter, args)
	}

	s.env.ExecuteWorkflow(deploymentWorkflow, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				tv.DeploymentVersionString(): {
					Version: tv.DeploymentVersionString(),
				},
			},
		},
	})
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkerDeploymentSuite) Test_SyncUnversionedRamp_SingleTaskQueue() {
	workers := 1
	s.syncUnversionedRampInBatches(workers)
}

func (s *WorkerDeploymentSuite) Test_SyncUnversionedRamp_MultipleTaskQueues() {
	workers := 100
	s.syncUnversionedRampInBatches(workers)
}

func (s *WorkerDeploymentSuite) syncUnversionedRampInBatches(totalWorkers int) {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	taskQueueInfos := make([]*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo, totalWorkers)
	for i := 0; i < totalWorkers; i++ {
		taskQueueInfos[i] = &deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{
			Name: tv.TaskQueue().Name + fmt.Sprintf("%03d", i),
			Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		}
	}
	// Mock the DescribeVersionFromWorkerDeployment activity to return numWorker taskQueues
	s.env.OnActivity(a.DescribeVersionFromWorkerDeployment, mock.Anything, mock.Anything).Return(
		&deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult{
			TaskQueueInfos: taskQueueInfos,
		}, nil)

	// Mock the SyncDeploymentVersionUserData activity and expect it to be called totalWorkers times
	var totalBatches int
	batchSize := int(s.workerDeploymentClient.getSyncBatchSize())
	if totalWorkers%batchSize == 0 {
		totalBatches = totalWorkers / batchSize
	} else {
		totalBatches = totalWorkers/batchSize + 1
	}

	s.env.OnActivity(a.SyncDeploymentVersionUserDataFromWorkerDeployment, mock.Anything, mock.Anything).Times(totalBatches).Return(nil, nil)

	s.env.RegisterDelayedCallback(func() {

		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, &deploymentspb.SetRampingVersionArgs{
			Version: worker_versioning.UnversionedVersionId,
		})

	}, 0*time.Millisecond)

	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return 1000
		}
		return Workflow(ctx, maxVersionsGetter, args)
	}

	s.env.ExecuteWorkflow(deploymentWorkflow, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateTime:    timestamppb.New(time.Now()),
			SyncBatchSize: int32(s.workerDeploymentClient.getSyncBatchSize()), // initialize the sync batch size
			// Initialize the routing config with the current version (tv.DeploymentVersionString()).
			// This simulates a scenario where the worker deployment already has a current version,
			// which is a prerequisite for ramping to an unversioned state.
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: tv.DeploymentVersionString(),
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

}
