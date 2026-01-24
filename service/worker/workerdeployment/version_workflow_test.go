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
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
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
	t.Run("v2", func(t *testing.T) {
		suite.Run(t, &VersionWorkflowSuite{workflowVersion: VersionDataRevisionNumber})
	})
}

func (s *VersionWorkflowSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()

	// Provide getter functions for drainage refresh interval and visibility grace period
	drainageRefreshGetter := func() time.Duration { return 5 * time.Minute }
	visibilityGraceGetter := func() time.Duration { return 3 * time.Minute }
	workflowVersionGetter := func() DeploymentWorkflowVersion { return s.workflowVersion }

	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		return VersionWorkflow(ctx, workflowVersionGetter, drainageRefreshGetter, visibilityGraceGetter, args)
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

			registerWorkerArgs := deploymentspb.RegisterWorkerInVersionArgs_builder{
				TaskQueueName: tv.TaskQueue().GetName() + fmt.Sprintf("%03d", workerNum),
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				MaxTaskQueues: 100,
				Version:       tv.DeploymentVersionString(),
			}.Build()
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

		s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, deploymentspb.SyncDeploymentVersionUserDataRequest_builder{
			Version: tv.DeploymentVersion(),
			Sync: []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
				deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData_builder{
					Name:  tv.TaskQueue().GetName() + fmt.Sprintf("%03d", workerNum),
					Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
					Data: deploymentspb.DeploymentVersionData_builder{
						Version:           tv.DeploymentVersion(),
						RoutingUpdateTime: nil,
						CurrentSinceTime:  nil,
						RampingSinceTime:  nil,
						RampPercentage:    0,
					}.Build(),
				}.Build(),
			},
			ForgetVersion: false,
		}.Build()).Once().Return(deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{
				tv.TaskQueue().GetName() + fmt.Sprintf("%03d", workerNum): 1,
			},
		}.Build(), nil)
	}

	// Mocking the SyncDeploymentVersionUserData + CheckWorkerDeploymentUserDataPropagation activity which
	// are called when registering a worker in the version workflow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Times(totalWorkers).Return(nil)

	batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
	syncReq := deploymentspb.SyncDeploymentVersionUserDataRequest_builder{
		Version:       tv.DeploymentVersion(),
		ForgetVersion: false,
	}.Build()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RampingSinceTime:  nil,
			RampPercentage:    0,
		}.Build()

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
		syncReq.SetSync(append(syncReq.GetSync(), deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData_builder{
			Name:  tv.TaskQueue().GetName() + fmt.Sprintf("%03d", i),
			Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Data: deploymentspb.DeploymentVersionData_builder{
				Version:           tv.DeploymentVersion(),
				RoutingUpdateTime: now,
				CurrentSinceTime:  now,
				RampingSinceTime:  nil,
				RampPercentage:    0,
			}.Build(),
		}.Build()))

		if len(syncReq.GetSync()) == int(s.workerDeploymentClient.getSyncBatchSize()) {
			batches = append(batches, syncReq.GetSync())
			syncReq.SetSync(make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0))
		}
	}

	if len(syncReq.GetSync()) > 0 {
		batches = append(batches, syncReq.GetSync())
	}

	// SyncDeploymentVersionUserData should be called # of batches times with the right batch argument.
	for _, batch := range batches {
		s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, deploymentspb.SyncDeploymentVersionUserDataRequest_builder{
			Version:       tv.DeploymentVersion(),
			Sync:          batch,
			ForgetVersion: false,
		}.Build()).Once().Return(nil, nil)
	}

	// starting the version workflow
	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			CreateTime:        nil,
			RoutingUpdateTime: nil,
			CurrentSinceTime:  nil,                                 // not current
			RampingSinceTime:  nil,                                 // not ramping
			RampPercentage:    0,                                   // not ramping
			DrainageInfo:      &deploymentpb.VersionDrainageInfo{}, // not draining or drained
			Metadata:          nil,
			SyncBatchSize:     int32(s.workerDeploymentClient.getSyncBatchSize()), // initialize the sync batch size
		}.Build(),
	}.Build())
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
	taskQueueName1 := tv.TaskQueue().GetName() + "001"
	taskQueueName2 := tv.TaskQueue().GetName() + "002"

	routingConfig := deploymentpb.RoutingConfig_builder{
		CurrentVersion:            tv.DeploymentVersionString(),
		CurrentVersionChangedTime: now,
		RevisionNumber:            5,
	}.Build()

	// Mock SyncDeploymentVersionUserData activity - async mode expects batches with UpdateRoutingConfig
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			// Verify that UpdateRoutingConfig is present (async mode)
			s.NotNil(req.GetUpdateRoutingConfig(), "UpdateRoutingConfig should be present in async mode")
			s.Equal(routingConfig.GetRevisionNumber(), req.GetUpdateRoutingConfig().GetRevisionNumber())
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName1: 10,
					taskQueueName2: 11,
				},
			}.Build(), nil
		},
	).Maybe()

	// Mock propagation check activity
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock the SignalExternalWorkflow call that happens after async propagation completes (optional)
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RampingSinceTime:  nil,
			RampPercentage:    0,
			RoutingConfig:     routingConfig, // Async mode: provide routing config
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName1: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
				taskQueueName2: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

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

	taskQueueName := tv.TaskQueue().GetName()

	routingConfig := deploymentpb.RoutingConfig_builder{
		CurrentVersion:            tv.DeploymentVersionString(),
		CurrentVersionChangedTime: now,
		RevisionNumber:            5,
	}.Build()

	// Mock SyncDeploymentVersionUserData to return with task queue max versions
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}.Build(), nil,
	).Maybe()

	// Mock CheckWorkerDeploymentUserDataPropagation with a delay to simulate async processing
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		After(50 * time.Millisecond).Return(nil).Maybe()

	// Mock the SignalExternalWorkflow call (optional)
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig, // Async mode
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_Success tests successful deletion of a version
func (s *VersionWorkflowSuite) Test_DeleteVersion_Success() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion (ForgetVersion = true)
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			s.True(req.GetForgetVersion(), "ForgetVersion should be true during deletion")
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName: 10,
				},
			}.Build(), nil
		},
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage: false,
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_QueryAfterDeletion tests that querying a deleted version returns an error
func (s *VersionWorkflowSuite) Test_DeleteVersion_QueryAfterDeletion() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}.Build(), nil,
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Send delete update
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage: false,
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

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

	taskQueueName := tv.TaskQueue().GetName()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage: false, // Do not skip drainage check
		}.Build()

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().Error(err, "delete version should fail when version is draining")
				var applicationError *temporal.ApplicationError
				s.Require().ErrorAs(err, &applicationError)
				s.Equal(errVersionIsDraining, applicationError.Type())
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			DrainageInfo: deploymentpb.VersionDrainageInfo_builder{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastCheckedTime: now,
				LastChangedTime: now,
			}.Build(),
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_SucceedsWhenDrainingWithSkipFlag tests that deletion succeeds when version is draining but SkipDrainage is true
func (s *VersionWorkflowSuite) Test_DeleteVersion_SucceedsWhenDrainingWithSkipFlag() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData for deletion
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueName: 10,
			},
		}.Build(), nil,
	).Maybe()

	// Mock propagation check
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage: true, // Skip drainage check
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			DrainageInfo: deploymentpb.VersionDrainageInfo_builder{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastCheckedTime: now,
				LastChangedTime: now,
			}.Build(),
			SyncBatchSize:             25, // Use explicit batch size
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_FailsWithActivePollers tests that deletion fails when version has active pollers
func (s *VersionWorkflowSuite) Test_DeleteVersion_FailsWithActivePollers() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Mock CheckIfTaskQueuesHavePollers - return true (has pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(true, nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage: false,
		}.Build()

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().Error(err, "delete version should fail when version has active pollers")
				var applicationError *temporal.ApplicationError
				s.Require().ErrorAs(err, &applicationError)
				s.Equal(errVersionHasPollers, applicationError.Type())
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_QueryBeforeDeletion tests that querying before deletion works normally
func (s *VersionWorkflowSuite) Test_DeleteVersion_QueryBeforeDeletion() {
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Query before deletion - should succeed
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeVersion)
		s.Require().NoError(err, "query should succeed before deletion")
		s.NotNil(val)

		var resp deploymentspb.QueryDescribeVersionResponse
		err = val.Get(&resp)
		s.Require().NoError(err)
		s.NotNil(resp.GetVersionState())
		s.Equal(tv.BuildID(), resp.GetVersionState().GetVersion().GetBuildId())
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

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

	taskQueueName := tv.TaskQueue().GetName()

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock SyncDeploymentVersionUserData - simulate a long-running operation
	// In async mode, the workflow should NOT wait for this to complete
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		After(100*time.Millisecond). // Simulate slow propagation
		Return(
			deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{
					taskQueueName: 10,
				},
			}.Build(), nil,
		).Maybe()

	// Mock propagation check - also slow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		After(100 * time.Millisecond).
		Return(nil).Maybe()

	// Send delete update with AsyncPropagation enabled
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage:     false,
			AsyncPropagation: true, // Enable async propagation
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteVersion_AsyncPropagation_BlocksWorkerRegistration tests that:
// 1. When deletion with async propagation is in progress, worker registration is blocked
// 2. Worker registration completes after propagation finishes
// 3. Another delete call can be accepted after registration finishes
func (s *VersionWorkflowSuite) Test_DeleteVersion_AsyncPropagation_BlocksWorkerRegistration() {
	s.skipFromVersion(VersionDataRevisionNumber)
	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()
	newTaskQueueName := tv.TaskQueue().GetName() + "_new"

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
				return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
					TaskQueueMaxVersions: map[string]int64{
						taskQueueName: 10,
					},
				}.Build(), nil
			},
		).Once()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		Return(
			func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
				// This is for worker registration
				s.True(deleteCompleted, "worker registration sync should happen after delete propagation completes")
				return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
					TaskQueueMaxVersions: map[string]int64{
						newTaskQueueName: 1,
					},
				}.Build(), nil
			},
		).Once()

	// Mock propagation check - also slow
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	// Send delete update with AsyncPropagation enabled at 1ms
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage:     false,
			AsyncPropagation: true, // Enable async propagation
		}.Build()

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
		registerWorkerArgs := deploymentspb.RegisterWorkerInVersionArgs_builder{
			TaskQueueName: newTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersionString(),
		}.Build()

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

		deleteArgs2 := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage:     false,
			AsyncPropagation: true,
		}.Build()

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

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Require().Error(s.env.GetWorkflowError()) // CaN
	s.True(workerRegistrationCompleted, "worker registration should have completed")
}

// Test_RegisterWorker_IncrementsRevisionNumber_WhenRevivingDeletedVersion tests that the revision number
// is incremented when a worker registers on a version that was previously deleted
func (s *VersionWorkflowSuite) Test_RegisterWorker_IncrementsRevisionNumber_WhenRevivingDeletedVersion() {
	s.skipBeforeVersion(VersionDataRevisionNumber)

	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()
	newTaskQueueName := tv.TaskQueue().GetName() + "_new"

	// Mock CheckIfTaskQueuesHavePollers - return false (no pollers)
	s.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// Mock delete and register propagation
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if req.HasUpsertVersionData() && req.GetUpsertVersionData().GetDeleted() {
				// This is the delete call
				s.Equal(int64(6), req.GetUpsertVersionData().GetRevisionNumber(), "Revision number should be incremented from 5 to 6 on delete")
				return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
					TaskQueueMaxVersions: map[string]int64{taskQueueName: 10},
				}.Build(), nil
			}
			// This is a register worker propagation call
			s.NotNil(req.GetUpsertVersionData(), "UpsertVersionData should be present for registration")
			s.False(req.GetUpsertVersionData().GetDeleted(), "Deleted should be false after revival")
			s.Equal(int64(7), req.GetUpsertVersionData().GetRevisionNumber(), "Revision number should be incremented from 6 to 7 on revival")
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{newTaskQueueName: 1},
			}.Build(), nil
		},
	).Times(2)

	// Make propagation check take long enough so register worker happens before workflow exits
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).After(100 * time.Millisecond).Return(nil).Maybe()

	// Delete the version
	s.env.RegisterDelayedCallback(func() {
		deleteArgs := deploymentspb.DeleteVersionArgs_builder{
			SkipDrainage:     false,
			AsyncPropagation: true,
		}.Build()

		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	// Register worker to revive the version
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: timestamppb.New(time.Now()),
			RevisionNumber:            10,
		}.Build()

		registerArgs := deploymentspb.RegisterWorkerInVersionArgs_builder{
			TaskQueueName: newTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersionString(),
			RoutingConfig: routingConfig,
		}.Build()

		s.env.UpdateWorkflow(RegisterWorkerInDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("register should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, registerArgs)
	}, 50*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			RevisionNumber:            5,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_SyncState_IncrementsRevisionNumber_InAsyncMode tests that revision numbers are tracked
// correctly through state syncs in async mode
func (s *VersionWorkflowSuite) Test_SyncState_IncrementsRevisionNumber_InAsyncMode() {
	s.skipBeforeVersion(VersionDataRevisionNumber)

	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			s.NotNil(req.GetUpsertVersionData(), "UpsertVersionData should be present in async mode")
			s.NotNil(req.GetUpdateRoutingConfig(), "UpdateRoutingConfig should be present in async mode")

			// Revision should increment from 0 to 1
			s.Equal(int64(1), req.GetUpsertVersionData().GetRevisionNumber(), "Sync should have revision 1")

			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{taskQueueName: 1},
			}.Build(), nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Sync with routing config
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentDeploymentVersion:  tv.ExternalDeploymentVersion(),
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_MultipleSyncStates_BlocksCaNUntilAllComplete tests that CaN is blocked while
// multiple async propagations are in progress
func (s *VersionWorkflowSuite) Test_MultipleSyncStates_BlocksCaNUntilAllComplete() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	syncCallCount := 0
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			syncCallCount++
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{taskQueueName: int64(syncCallCount * 10)},
			}.Build(), nil
		},
	).Maybe()

	// Mock propagation check with delay to simulate slow propagation
	propagationCheckCount := 0
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, req *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			propagationCheckCount++
			return nil
		}).
		After(100 * time.Millisecond).
		Maybe()

	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// First sync
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("first sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	// Second sync shortly after
	s.env.RegisterDelayedCallback(func() {
		newTime := timestamppb.New(time.Now().Add(5 * time.Second))
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: newTime,
			RevisionNumber:            6,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: newTime,
			CurrentSinceTime:  newTime,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("second sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 10*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	// Both propagations should have completed before CaN
	s.Equal(2, propagationCheckCount, "Both propagations should complete before CaN")
}

// Test_SyncState_And_RegisterWorker_ConcurrentPropagations tests concurrent async propagations
// from different update types
func (s *VersionWorkflowSuite) Test_SyncState_And_RegisterWorker_ConcurrentPropagations() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()
	newTaskQueueName := tv.TaskQueue().GetName() + "_new"

	syncActivityCalls := 0
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			syncActivityCalls++
			if req.HasUpdateRoutingConfig() && len(req.GetSync()) == 1 && req.GetSync()[0].GetName() == taskQueueName {
				// This is the SyncState propagation
				return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
					TaskQueueMaxVersions: map[string]int64{taskQueueName: 10},
				}.Build(), nil
			}
			// This is the RegisterWorker propagation
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
				TaskQueueMaxVersions: map[string]int64{newTaskQueueName: 1},
			}.Build(), nil
		},
	).Maybe()

	propagationChecks := 0
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, req *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			propagationChecks++
			return nil
		}).
		After(50 * time.Millisecond).
		Maybe()

	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Start SyncState
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	// Start RegisterWorker
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		registerArgs := deploymentspb.RegisterWorkerInVersionArgs_builder{
			TaskQueueName: newTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersionString(),
			RoutingConfig: routingConfig,
		}.Build()

		s.env.UpdateWorkflow(RegisterWorkerInDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("register should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, registerArgs)
	}, 10*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	// Both syncs should complete
	s.GreaterOrEqual(syncActivityCalls, 2, "Both propagations should complete before CaN")
	// Only syncVersionState should wait for propagation
	s.GreaterOrEqual(propagationChecks, 1, "Both propagations should complete before CaN")
}

// Test_SyncState_SignalsPropagationComplete_WithCorrectRevisionNumber tests that the deployment
// workflow is signaled when async propagation completes with the correct revision number
func (s *VersionWorkflowSuite) Test_SyncState_SignalsPropagationComplete_WithCorrectRevisionNumber() {
	s.skipBeforeVersion(VersionDataRevisionNumber)

	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{taskQueueName: 10},
		}.Build(), nil,
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock the external signal (propagation complete signal is sent after async propagation)
	var capturedSignalArg *deploymentspb.PropagationCompletionInfo
	expectedWorkflowID := GenerateDeploymentWorkflowID(tv.DeploymentSeries())
	s.env.OnSignalExternalWorkflow(
		mock.Anything,
		expectedWorkflowID,
		"",
		PropagationCompleteSignal,
		mock.Anything,
	).Return(func(namespace string, workflowID string, runID string, signalName string, arg interface{}) error {
		capturedSignalArg = arg.(*deploymentspb.PropagationCompletionInfo)
		return nil
	}).Maybe()

	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5, // This should be in the signal
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())

	// Verify that the signal was sent with the correct revision number
	s.Require().NotNil(capturedSignalArg, "PropagationCompleteSignal should have been sent")
	s.Equal(int64(5), capturedSignalArg.GetRevisionNumber(), "Signal should contain the correct revision number")
	s.Equal(tv.BuildID(), capturedSignalArg.GetBuildId(), "Signal should contain the correct build ID")
}

// Test_RegisterWorker_DoesNotSignalPropagationComplete tests that worker registration
// doesn't signal the deployment workflow about propagation completion
func (s *VersionWorkflowSuite) Test_RegisterWorker_DoesNotSignalPropagationComplete() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	newTaskQueueName := tv.TaskQueue().GetName() + "_new"

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		deploymentspb.SyncDeploymentVersionUserDataResponse_builder{
			TaskQueueMaxVersions: map[string]int64{newTaskQueueName: 1},
		}.Build(), nil,
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		s.Fail("Should not signal propagation complete for worker registration")
	}).Maybe()

	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		registerArgs := deploymentspb.RegisterWorkerInVersionArgs_builder{
			TaskQueueName: newTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersionString(),
			RoutingConfig: routingConfig,
		}.Build()

		s.env.UpdateWorkflow(RegisterWorkerInDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("register should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, registerArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies:         map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{},
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_BatchTaskQueuesForSync_SingleBatch tests batching when task queues fit in one batch
func (s *VersionWorkflowSuite) Test_BatchTaskQueuesForSync_SingleBatch() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create 5 task queues, batch size is 25, so should be single batch
	taskQueues := make(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData)
	for i := 0; i < 5; i++ {
		tqName := fmt.Sprintf("%s_%d", tv.TaskQueue().GetName(), i)
		taskQueues[tqName] = deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
			TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
			},
		}.Build()
	}

	syncCallCount := 0
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			syncCallCount++
			s.Len(req.GetSync(), 5, "Should sync all 5 task queues in one batch")
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies:         taskQueues,
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Equal(1, syncCallCount, "Should make exactly 1 sync call for single batch")
}

// Test_BatchTaskQueuesForSync_MultipleBatches tests batching when task queues span multiple batches
func (s *VersionWorkflowSuite) Test_BatchTaskQueuesForSync_MultipleBatches() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create 50 task queues, batch size is 25, so should be 2 batches
	taskQueues := make(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData)
	for i := 0; i < 50; i++ {
		tqName := fmt.Sprintf("%s_%03d", tv.TaskQueue().GetName(), i)
		taskQueues[tqName] = deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
			TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
			},
		}.Build()
	}

	syncCallCount := 0
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			syncCallCount++
			s.Len(req.GetSync(), 25, "Each batch should contain 25 task queues")
			taskQueuesToCheck := make(map[string]int64, 24)
			for i, tq := range req.GetSync() {
				if i == 0 {
					// skip the first task queue as if it did not update and doesn't need check
					continue
				}
				taskQueuesToCheck[tq.GetName()] = 10
			}
			return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{TaskQueueMaxVersions: taskQueuesToCheck}.Build(), nil
		},
	).Twice()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			s.True(len(req.GetTaskQueueMaxVersions()) == 25 || len(req.GetTaskQueueMaxVersions()) == 23, "Should check all task queues except two")
			return nil
		}).Twice()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies:         taskQueues,
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Equal(2, syncCallCount, "Should make exactly 2 sync calls for 2 batches")
}

// Test_BatchTaskQueuesForSync_PartialLastBatch tests batching with a partial last batch
func (s *VersionWorkflowSuite) Test_BatchTaskQueuesForSync_PartialLastBatch() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create 27 task queues, batch size is 10, so should be 3 batches: 10, 10, 7
	taskQueues := make(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData)
	for i := 0; i < 27; i++ {
		tqName := fmt.Sprintf("%s_%02d", tv.TaskQueue().GetName(), i)
		taskQueues[tqName] = deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
			TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
			},
		}.Build()
	}

	syncCallCount := 0
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			syncCallCount++
			if syncCallCount <= 2 {
				s.Len(req.GetSync(), 10, "First two batches should contain 10 task queues each")
			} else {
				s.Len(req.GetSync(), 7, "Last batch should contain 7 task queues")
			}
			// Return no task queues as if none were updated
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		s.Fail("CheckWorkerDeploymentUserDataPropagation should not be called")
	}).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies:         taskQueues,
			RevisionNumber:            0,
			SyncBatchSize:             10, // Use smaller batch size for this test
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.Equal(3, syncCallCount, "Should make exactly 3 sync calls for 3 batches")
}

// Test_FindNewVersionStatusFromRoutingConfig_CurrentToRamping tests status transition from CURRENT to RAMPING
func (s *VersionWorkflowSuite) Test_FindNewVersionStatusFromRoutingConfig_CurrentToRamping() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())
	rampingTime := timestamppb.New(time.Now().Add(5 * time.Second))

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if req.HasUpsertVersionData() {
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, req.GetUpsertVersionData().GetStatus(),
					"Version status should transition to RAMPING")
				if s.workflowVersion >= VersionDataRevisionNumber {
					s.Equal(int64(1), req.GetUpsertVersionData().GetRevisionNumber(), "Revision number should be incremented")
				}
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Make version RAMPING instead of CURRENT
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString() + "_other"),
			CurrentVersion:                      tv.DeploymentVersionString() + "_other",
			CurrentVersionChangedTime:           rampingTime,
			RampingDeploymentVersion:            tv.ExternalDeploymentVersion(),
			RampingVersion:                      tv.DeploymentVersionString(),
			RampingVersionChangedTime:           rampingTime,
			RampingVersionPercentage:            30,
			RampingVersionPercentageChangedTime: rampingTime,
			RevisionNumber:                      6,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: rampingTime,
			RampingSinceTime:  rampingTime,
			RampPercentage:    30,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
				resp := result.(*deploymentspb.SyncVersionStateResponse)
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, resp.GetSummary().GetStatus(),
					"Version should be RAMPING")
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			CurrentSinceTime:          now,
			RoutingUpdateTime:         now,
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_FindNewVersionStatusFromRoutingConfig_RampingToDraining tests status transition from RAMPING to DRAINING
func (s *VersionWorkflowSuite) Test_FindNewVersionStatusFromRoutingConfig_RampingToDraining() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())
	drainTime := timestamppb.New(time.Now().Add(10 * time.Second))

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if req.HasUpsertVersionData() {
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, req.GetUpsertVersionData().GetStatus(),
					"Version status should transition to DRAINING")
				if s.workflowVersion >= VersionDataRevisionNumber {
					s.Equal(int64(1), req.GetUpsertVersionData().GetRevisionNumber(), "Revision number should be incremented")
				}
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Remove version from routing entirely (neither current nor ramping)
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString() + "_other"),
			CurrentVersion:                      tv.DeploymentVersionString() + "_other",
			CurrentVersionChangedTime:           drainTime,
			RampingVersionPercentageChangedTime: drainTime,
			RevisionNumber:                      7,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: drainTime,
			CurrentSinceTime:  nil,
			RampingSinceTime:  nil,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
				resp := result.(*deploymentspb.SyncVersionStateResponse)
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, resp.GetSummary().GetStatus(),
					"Version should be DRAINING")
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			RampingSinceTime:          now,
			RampPercentage:            30,
			RoutingUpdateTime:         now,
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_FindNewVersionStatusFromRoutingConfig_InactiveToCurrent tests status transition from INACTIVE to CURRENT
func (s *VersionWorkflowSuite) Test_FindNewVersionStatusFromRoutingConfig_InactiveToCurrent() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if req.HasUpsertVersionData() {
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, req.GetUpsertVersionData().GetStatus(),
					"Version status should transition to CURRENT (not DRAINING)")
				if s.workflowVersion >= VersionDataRevisionNumber {
					s.Equal(int64(1), req.GetUpsertVersionData().GetRevisionNumber(), "Revision number should be incremented")
				}
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Make version CURRENT for first time (never activated before)
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentDeploymentVersion:  tv.ExternalDeploymentVersion(),
			CurrentVersion:            tv.DeploymentVersionString(),
			CurrentVersionChangedTime: now,
			RevisionNumber:            5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
				resp := result.(*deploymentspb.SyncVersionStateResponse)
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, resp.GetSummary().GetStatus(),
					"Version should be CURRENT, not DRAINING")
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			CurrentSinceTime:          nil, // Never activated
			RampingSinceTime:          nil, // Never activated
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_UpdateStateFromRoutingConfig_UpdatesTimestamps tests that state is correctly updated from routing config
func (s *VersionWorkflowSuite) Test_UpdateStateFromRoutingConfig_UpdatesTimestamps() {
	tv := testvars.New(s.T())
	currentTime := timestamppb.New(time.Now())
	rampingTime := timestamppb.New(time.Now().Add(5 * time.Second))

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		&deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil,
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set version as RAMPING
	s.env.RegisterDelayedCallback(func() {
		routingConfig := deploymentpb.RoutingConfig_builder{
			CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString() + "_other"),
			CurrentVersion:                      tv.DeploymentVersionString() + "_other",
			CurrentVersionChangedTime:           currentTime,
			RampingDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
			RampingVersion:                      tv.DeploymentVersionString(),
			RampingVersionChangedTime:           rampingTime,
			RampingVersionPercentage:            45,
			RampingVersionPercentageChangedTime: rampingTime,
			RevisionNumber:                      5,
		}.Build()

		syncArgs := deploymentspb.SyncVersionStateUpdateArgs_builder{
			RoutingUpdateTime: rampingTime,
			RampingSinceTime:  rampingTime,
			RampPercentage:    45,
			RoutingConfig:     routingConfig,
		}.Build()

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync should not be rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.Require().NoError(err)
				resp := result.(*deploymentspb.SyncVersionStateResponse)
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, resp.GetSummary().GetStatus())
				s.Equal(rampingTime.AsTime(), resp.GetSummary().GetRampingSinceTime().AsTime(),
					"RampingSinceTime should match routing config")
				s.Equal(rampingTime.AsTime(), resp.GetSummary().GetRoutingUpdateTime().AsTime(),
					"RoutingUpdateTime should use percentage changed time for ramping")
				s.Nil(resp.GetSummary().GetCurrentSinceTime(), "CurrentSinceTime should be nil")
			},
		}, syncArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status:                    enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			RevisionNumber:            0,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DrainageStatusChange_TriggersAsyncPropagation_WithRevisionNumber tests that drainage status
// changes trigger async propagation when using version data revision numbers
func (s *VersionWorkflowSuite) Test_DrainageStatusChange_TriggersAsyncPropagation_WithRevisionNumber() {
	s.skipBeforeVersion(VersionDataRevisionNumber)

	tv := testvars.New(s.T())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueName := tv.TaskQueue().GetName()

	// Track if async propagation was triggered (not the old sync method)
	asyncPropagationTriggered := false
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.SyncDeploymentVersionUserDataRequest) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
			if req.HasUpsertVersionData() && req.GetUpsertVersionData().GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED {
				asyncPropagationTriggered = true
				s.Equal(int64(6), req.GetUpsertVersionData().GetRevisionNumber())
			}
			return &deploymentspb.SyncDeploymentVersionUserDataResponse{}, nil
		},
	).Maybe()

	// Mock the drainage activity to return DRAINED status
	s.env.OnActivity(a.GetVersionDrainageStatus, mock.Anything, mock.Anything).Return(
		deploymentpb.VersionDrainageInfo_builder{
			Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
			LastCheckedTime: timestamppb.New(time.Now()),
			LastChangedTime: timestamppb.New(time.Now()),
		}.Build(), nil,
	).Maybe()

	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	now := timestamppb.New(time.Now())
	s.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, deploymentspb.WorkerDeploymentVersionWorkflowArgs_builder{
		NamespaceName: tv.NamespaceName().String(),
		NamespaceId:   tv.NamespaceID().String(),
		VersionState: deploymentspb.VersionLocalState_builder{
			Version: deploymentspb.WorkerDeploymentVersion_builder{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			}.Build(),
			TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				taskQueueName: deploymentspb.VersionLocalState_TaskQueueFamilyData_builder{
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				}.Build(),
			},
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			DrainageInfo: deploymentpb.VersionDrainageInfo_builder{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastCheckedTime: now,
				LastChangedTime: now,
			}.Build(),
			RevisionNumber:            5,
			SyncBatchSize:             int32(s.workerDeploymentClient.getSyncBatchSize()),
			StartedDeploymentWorkflow: true,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.True(asyncPropagationTriggered, "Async propagation should be triggered for drainage status change")
}

func (s *VersionWorkflowSuite) skipBeforeVersion(version DeploymentWorkflowVersion) {
	if s.workflowVersion < version {
		s.T().Skipf("test supports version %v and newer", version)
	}
}

func (s *VersionWorkflowSuite) skipFromVersion(version DeploymentWorkflowVersion) {
	if s.workflowVersion >= version {
		s.T().Skipf("test supports version older than %v", version)
	}
}
