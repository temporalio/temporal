// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VersionWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment
}

func TestVersionWorkflowSuite(t *testing.T) {
	suite.Run(t, new(VersionWorkflowSuite))
}

func (s *VersionWorkflowSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(VersionWorkflow)
}

func (s *VersionWorkflowSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

// Test_SyncState_BatchSize verifies if the right number of batches are created during the SyncDeploymentVersionUserData activity
func (s *VersionWorkflowSuite) Test_SyncState_Batch_SingleTaskQueue() {
	workers := 1
	s.syncStateInBatches(workers)
}

func (s *VersionWorkflowSuite) Test_SyncState_Batch_MultipleTaskQueues() {
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

		if len(syncReq.Sync) == syncBatchSize {
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
	s.env.ExecuteWorkflow(VersionWorkflow, &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
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
		},
	})
}
