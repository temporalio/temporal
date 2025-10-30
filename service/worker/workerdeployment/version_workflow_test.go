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
}

func TestVersionWorkflowSuite(t *testing.T) {
	suite.Run(t, new(VersionWorkflowSuite))
}

func (s *VersionWorkflowSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()
	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		return VersionWorkflow(ctx, nil, nil, args)
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
			for _, sync := range req.Sync {
				s.NotNil(sync.UpdateRoutingConfig, "UpdateRoutingConfig should be present in async mode")
				s.Equal(routingConfig.RevisionNumber, sync.UpdateRoutingConfig.RevisionNumber)
			}
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
				s.Fail("sync state update should not have failed with error %v", err)
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.NoError(err)
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
				s.NoError(err)
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

// Test_AsyncPropagationSkipsUnchangedTaskQueues tests that async propagation
// only waits for task queues where the routing config actually changed (maxVersion != -1)
func (s *VersionWorkflowSuite) Test_AsyncPropagationSkipsUnchangedTaskQueues() {
	tv := testvars.New(s.T())
	now := timestamppb.New(time.Now())

	var a *VersionActivities
	s.env.RegisterActivity(a.StartWorkerDeploymentWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()

	taskQueueChanged := tv.TaskQueue().Name + "changed"
	taskQueueUnchanged := tv.TaskQueue().Name + "unchanged"

	routingConfig := &deploymentpb.RoutingConfig{
		CurrentVersion:            tv.DeploymentVersionString(),
		CurrentVersionChangedTime: now,
		RevisionNumber:            5,
	}

	// Mock SyncDeploymentVersionUserData - return -1 for unchanged task queue
	s.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).Return(
		&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{
				taskQueueChanged:   10, // Changed
				taskQueueUnchanged: -1, // Unchanged - should be filtered out
			},
		}, nil,
	).Maybe()

	// Mock propagation check - should only be called with the changed task queue
	s.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
			// Verify only the changed task queue is being checked
			s.Contains(req.TaskQueueMaxVersions, taskQueueChanged)
			s.NotContains(req.TaskQueueMaxVersions, taskQueueUnchanged)
			return nil
		},
	).Maybe()

	// Mock the SignalExternalWorkflow call (optional)
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: now,
			CurrentSinceTime:  now,
			RoutingConfig:     routingConfig,
		}

		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("sync state update should not have failed")
			},
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				s.NoError(err)
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
				taskQueueChanged: {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
					},
				},
				taskQueueUnchanged: {
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
