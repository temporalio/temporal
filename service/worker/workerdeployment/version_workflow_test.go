package workerdeployment

import (
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
	s.env.OnActivity(a.StartWorkerDeploymentWorkflow, mock.Anything, mock.Anything).Once().Return(nil)

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
