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
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VersionWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller             *gomock.Controller
	env                    *testsuite.TestWorkflowEnvironment
	workerDeploymentClient *ClientImpl
	workflowVersion        DeploymentWorkflowVersion
}

func TestVersionWorkflowSuite(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		suite.Run(t, &VersionWorkflowSuite{workflowVersion: InitialVersion})
	})
	t.Run("v1", func(t *testing.T) {
		suite.Run(t, &VersionWorkflowSuite{workflowVersion: AsyncSetCurrentAndRamping})
	})
}

func (s *VersionWorkflowSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()

	// Provide getter functions for drainage refresh interval and visibility grace period
	drainageRefreshGetter := func() any {
		return 5 * time.Minute
	}
	visibilityGraceGetter := func() any {
		return 3 * time.Minute
	}

	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		return VersionWorkflow(ctx, nil, drainageRefreshGetter, visibilityGraceGetter, args)
	}
	s.env.RegisterWorkflowWithOptions(versionWorkflow, workflow.RegisterOptions{Name: WorkerDeploymentVersionWorkflowType})

	// Initialize an empty ClientImpl to use its helper methods
	s.workerDeploymentClient = &ClientImpl{}
}

func (s *VersionWorkflowSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

// Test_SyncState_BatchSize verifies if the right number of batches are created during the SyncDeploymentVersionUserData activity
func (s *VersionWorkflowSuite) Test_SyncState_Batch_SingleTaskQueue() {
	// TODO: refactor this test so it creates a version with the TQ already added to it and then
	// test batching when SyncVersionState is called on it.
	// In this form, the test is flaky because it does not account for possible CaN happening
	// due to register worker calls.
	s.T().Skip()

	workers := 1
	s.syncStateInBatches(workers)
}

func (s *VersionWorkflowSuite) Test_SyncState_Batch_MultipleTaskQueues() {
	// TODO: refactor this test so it creates a version with 500 TQs already added to it and then
	// test batching when SyncVersionState is called on it.
	// In this form, the test does not pass because it does not account for all the CaNs happening
	// due to register worker calls.
	s.T().Skip()

	workers := 500
	s.syncStateInBatches(workers)
}

func (s *VersionWorkflowSuite) syncStateInBatches(totalWorkers int) {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	// Mocking the start deployment workflow activity
	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil)

	for workerNum := 0; workerNum < totalWorkers; workerNum++ {
		s.env.RegisterDelayedCallback(func() {

			registerWorkerArgs := &deploymentspb.RegisterWorkerInVersionArgs{
				TaskQueueName: tv.TaskQueue().Name + fmt.Sprintf("%03d", workerNum),
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				MaxTaskQueues: 100,
				Version:       tv.DeploymentVersionString(),
			}
			s.env.UpdateWorkflow(RegisterWorkerInDeploymentVersion, "", &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					s.Fail("register worker in version should not have failed with error %v", err)
				},
				OnAccept: func() {
				},
				OnComplete: func(i interface{}, err error) {
				},
			}, registerWorkerArgs)
		}, 1*time.Millisecond)

		s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, &deploymentspb.SyncDeploymentVersionUserDataRequest{
			Version: tv.DeploymentVersion(),
			Sync: []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
				{
					Name:  tv.TaskQueue().Name + fmt.Sprintf("%03d", workerNum),
					Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
					Data: &deploymentspb.DeploymentVersionData{
						Version:           tv.DeploymentVersion(),
						RoutingUpdateTime: nil,
						CurrentSinceTime:  nil,
						RampingSinceTime:  nil,
						RampPercentage:    0,
					},
				},
			},
			ForgetVersion: false,
		}).Once().Return(&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{
				tv.TaskQueue().Name + fmt.Sprintf("%03d", workerNum): 1,
			},
		}, nil)
	}

	// Mocking the SyncDeploymentVersionUserData + CheckWorkerDeploymentUserDataPropagation activity which
	// are called when registering a worker in the version workflow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Times(totalWorkers).Return(nil)

	batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:       tv.DeploymentVersion(),
		ForgetVersion: false,
	}

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RampingSinceTime:  nil,
			RampPercentage:    0,
		}

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync state update should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(i interface{}, err error) {

			},
		}, syncStateArgs)
	}, 30*time.Millisecond)

	for i := 0; i < totalWorkers; i++ {
		syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tv.TaskQueue().Name + fmt.Sprintf("%03d", i),
			Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Data: &deploymentspb.DeploymentVersionData{
				Version:           tv.DeploymentVersion(),
				RoutingUpdateTime: now,
				CurrentSinceTime:  now,
				RampingSinceTime:  nil,
				RampPercentage:    0,
			},
		})

		if len(syncReq.Sync) == int(s.workerDeploymentClient.getSyncBatchSize()) {
			batches = append(batches, syncReq.Sync)
			syncReq.Sync = make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
		}
	}

	if len(syncReq.Sync) > 0 {
		batches = append(batches, syncReq.Sync)
	}

	// SyncDeploymentVersionUserData should be called # of batches times with the right batch argument.
	for _, batch := range batches {
		s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, &deploymentspb.SyncDeploymentVersionUserDataRequest{
			Version:       tv.DeploymentVersion(),
			Sync:          batch,
			ForgetVersion: false,
		}).Once().Return(nil, nil)
	}

	// starting the version workflow
	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			CreateTime:        nil,
			RoutingUpdateTime: nil,
			CurrentSinceTime:  nil,                                 // not current
			RampingSinceTime:  nil,                                 // not ramping
			RampPercentage:    0,                                   // not ramping
			DrainageInfo:      &deploymentpb.VersionDrainageInfo{}, // not draining or drained
			Metadata:          nil,
			SyncBatchSize:     int32(s.workerDeploymentClient.getSyncBatchSize()), // initialize the sync batch size
		},
	})
}

// Test_SyncRoutingConfigAsync tests that routing config is propagated asynchronously
// when RoutingConfig is provided in SyncVersionStateUpdateArgs
func (s *VersionWorkflowSuite) Test_SyncRoutingConfigAsync() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Setup task queues in version state
	taskQueueName1 := tv.TaskQueue().Name + "001"
	taskQueueName2 := tv.TaskQueue().Name + "002"

	routingConfig := &deploymentpb.RoutingConfig{
		CurrentVersion:            tv.DeploymentVersionString(),
		CurrentVersionChangedTime: now,
		RevisionNumber:            5,
	}

	// Mock SyncDeploymentVersionUserData activity - async mode expects batches with UpdateRoutingConfig
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			// Verify that UpdateRoutingConfig is present (async mode)
			s.NotNil(req.UpdateRoutingConfig, "UpdateRoutingConfig should be present in async mode")
			s.Equal(routingConfig.RevisionNumber, req.UpdateRoutingConfig.RevisionNumber)
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName1: 10,
					taskQueueName2: 11,
				},
			}, nil
		},
	).Maybe()

	// Mock propagation check activity
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock the SignalExternalWorkflow call that happens after async propagation completes (optional)
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RampingSinceTime:  nil,
			RampPercentage:    0,
			RoutingConfig:     routingConfig, // Async mode: provide routing config
		}

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync state update should not have failed", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncStateArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName1: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
				taskQueueName2: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_AsyncPropagationsPreventsCanUntilComplete tests that the workflow does not
// continue-as-new while async propagations are in progress
func (s *VersionWorkflowSuite) Test_AsyncPropagationsPreventsCanUntilComplete() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	routingConfig := &deploymentpb.RoutingConfig{
		CurrentVersion:            tv.DeploymentVersionString(),
		CurrentVersionChangedTime: now,
		RevisionNumber:            5,
	}

	// Mock SyncDeploymentVersionUserData to return with task queue max versions
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}, nil,
	).Maybe()

	// Mock CheckWorkerDeploymentUserDataPropagation with a delay to simulate async processing
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		After(50 * time.Millisecond).Return(nil).Maybe()

	// Mock the SignalExternalWorkflow call (optional)
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig, // Async mode
		}

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync state update should not have failed")
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncStateArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_Success tests successful deletion of a version
func (s *VersionWorkflowSuite) Test_DeleteVersion_Success() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion (ForgetVersion = true)
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			s.True(req.ForgetVersion, "ForgetVersion should be true during deletion")
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName: 10,
				},
			}, nil
		},
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage: false,
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "delete version should complete without error")
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_QueryAfterDeletion tests that querying a deleted version returns an error
func (s *VersionWorkflowSuite) Test_DeleteVersion_QueryAfterDeletion() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}, nil,
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Send delete update
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage: false,
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "delete version should complete without error")
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	// Query after deletion - should fail
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeVersion)
		s.Require().Error(err, "query should fail after deletion")
		s.Nil(val)
		s.Contains(err.Error(), "worker deployment version deleted")
	}, 5*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_FailsWhenDraining tests that deletion fails when version is draining
func (s *VersionWorkflowSuite) Test_DeleteVersion_FailsWhenDraining() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage: false, // Do not skip drainage check
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().Error(err, "delete version should fail when version is draining")
				s.Contains(err.Error(), ErrVersionIsDraining)
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			DrainageInfo: &deploymentpb.VersionDrainageInfo{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastCheckedTime: now,
				LastChangedTime: now,
			},
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_SucceedsWhenDrainingWithSkipFlag tests that deletion succeeds when version is draining but SkipDrainage is true
func (s *VersionWorkflowSuite) Test_DeleteVersion_SucceedsWhenDrainingWithSkipFlag() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}, nil,
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage: true, // Skip drainage check
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "delete version should succeed when SkipDrainage is true")
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			DrainageInfo: &deploymentpb.VersionDrainageInfo{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastCheckedTime: now,
				LastChangedTime: now,
			},
			SyncBatchSize:             25, // Use explicit batch size
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_FailsWithActivePollers tests that deletion fails when version has active pollers
func (s *VersionWorkflowSuite) Test_DeleteVersion_FailsWithActivePollers() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Mock CheckIfTaskQueuesHavePollers - return true (has pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(true, nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage: false,
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().Error(err, "delete version should fail when version has active pollers")
				s.Contains(err.Error(), ErrVersionHasPollers)
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_QueryBeforeDeletion tests that querying before deletion works normally
func (s *VersionWorkflowSuite) Test_DeleteVersion_QueryBeforeDeletion() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Query before deletion - should succeed
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeVersion)
		s.Require().NoError(err, "query should succeed before deletion")
		s.NotNil(val)

		var resp deploymentspb.QueryDescribeVersionResponse
		err = val.Get(&resp)
		s.Require().NoError(err)
		s.NotNil(resp.VersionState)
		s.Equal(tv.BuildID(), resp.VersionState.Version.BuildId)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_AsyncPropagation tests that with AsyncPropagation enabled:
// 1. The workflow doesn't block waiting for propagation to complete
// 2. The query returns error after deletion is initiated
func (s *VersionWorkflowSuite) Test_DeleteVersion_AsyncPropagation() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData - simulate a long-running operation
	// In async mode, the workflow should NOT wait for this to complete
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		After(100*time.Millisecond). // Simulate slow propagation
		Return(
			&deploymentspb.SyncDeploymentVersionUserDataResponse{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName: 10,
				},
			}, nil,
		).Maybe()

	// Mock propagation check - also slow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		After(100 * time.Millisecond).
		Return(nil).Maybe()

	// Send delete update with AsyncPropagation enabled
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage:     false,
			AsyncPropagation: true, // Enable async propagation
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "delete version should complete without error even with async propagation")
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	// Query immediately after deletion - should fail with "version deleted" error
	// This verifies that deleteVersion flag is set immediately, not after propagation completes
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeVersion)
		s.Require().Error(err, "query should fail after deletion even in async mode")
		s.Nil(val)
		s.Contains(err.Error(), "worker deployment version deleted")
	}, 10*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_AsyncPropagation_BlocksWorkerRegistration tests that:
// 1. When deletion with async propagation is in progress, worker registration is blocked
// 2. Worker registration completes after propagation finishes
// 3. Another delete call can be accepted after registration finishes
func (s *VersionWorkflowSuite) Test_DeleteVersion_AsyncPropagation_BlocksWorkerRegistration() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().Name
	newTaskQueueName := tv.TaskQueue().Name + "_new"

	// Track whether operations completed
	deleteCompleted := false
	workerRegistrationStarted := false
	workerRegistrationCompleted := false

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion - simulate VERY slow propagation
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		After(200 * time.Millisecond). // Slow propagation - 200ms
		Return(
			func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
				// This is the delete propagation
				return &deploymentspb.SyncDeploymentVersionUserDataResponse{
					TaskQueueMaxVersions: map[string]int64{
						taskQueueName: 10,
					},
				}, nil
			},
		).Once()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		Return(
			func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
				// This is for worker registration
				s.True(deleteCompleted, "worker registration sync should happen after delete propagation completes")
				return &deploymentspb.SyncDeploymentVersionUserDataResponse{
					TaskQueueMaxVersions: map[string]int64{
						newTaskQueueName: 1,
					},
				}, nil
			},
		).Once()

	// Mock propagation check - also slow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	// Send delete update with AsyncPropagation enabled at 1ms
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := &deploymentspb.DeleteVersionArgs{
			SkipDrainage:     false,
			AsyncPropagation: true, // Enable async propagation
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "delete version should complete without error")
				deleteCompleted = true
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	// Try to register a worker at 50ms (while propagation is still happening)
	s.env.RegisterDelayedCallback(func() {
		workerRegistrationStarted = true
		registerWorkerArgs := &deploymentspb.RegisterWorkerInVersionArgs{
			TaskQueueName: newTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersionString(),
		}

		s.env.UpdateWorkflow(RegisterWorkerInDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("register worker should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				// Worker registration should complete only after propagation finishes
				s.True(deleteCompleted, "worker registration should complete after delete propagation")
				s.Require().NoError(err, "register worker should complete without error")
				workerRegistrationCompleted = true
			},
		}, registerWorkerArgs)
	}, 50*time.Millisecond)

	// Verify worker registration is blocked at 100ms (should not have completed yet)
	s.env.RegisterDelayedCallback(func() {
		s.True(workerRegistrationStarted, "worker registration should have started")
		s.False(workerRegistrationCompleted, "worker registration should still be blocked at 100ms")
	}, 100*time.Millisecond)

	// Send another delete call after everything completes to ensure it's accepted
	s.env.RegisterDelayedCallback(func() {
		s.True(workerRegistrationCompleted, "worker registration should have completed by now")

		deleteArgs2 := &deploymentspb.DeleteVersionArgs{
			SkipDrainage:     false,
			AsyncPropagation: true,
		}

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("second delete should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err, "second delete should complete without error")
			},
		}, deleteArgs2)
	}, 500*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().Error(s.env.GetWorkflowError()) // CaN
	s.True(workerRegistrationCompleted, "worker registration should have completed")
}
