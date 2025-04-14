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
)

type WorkerDeploymentSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment
}

func TestWorkerDeploymentSuite(t *testing.T) {
	suite.Run(t, new(WorkerDeploymentSuite))
}

func (s *WorkerDeploymentSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(Workflow)
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

	// Adding the version to worker deployment.
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(AddVersionToWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(interface{}, error) {
			},
		}, &deploymentspb.AddVersionUpdateArgs{
			Version: tv.DeploymentVersionString(),
		})
	}, 0*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		updateArgs := &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 tv.DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
		}
		// Setting the new version as current version
		s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)
	}, 1*time.Millisecond)

	// Update #1 clears the validator and is then halted during activity processing. This is
	// to simulate the workflow giving up control which shall allow update #2 to clear the validator.
	// However, update #2 cannot yet start being processed since update #1 holds the workflow lock and
	// will only release it after it has completed processing.
	haltUpdate := make(chan struct{})
	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			if args.Version == tv.DeploymentVersionString() {
				// Halt the activity to simulate workflow giving up control while processing the first update.
				<-haltUpdate
			}
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	s.env.RegisterDelayedCallback(func() {
		updateArgs := &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 tv.DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
		}
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
	}, 1*time.Millisecond)

	// Close the channel to unblock the activity and allow update #1 to complete.
	// Note: This is called after update #2 begins being processed since it's fired after 2 milliseconds.
	s.env.RegisterDelayedCallback(func() {
		close(haltUpdate)
	}, 2*time.Millisecond)

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

	// Adding the version to worker deployment.
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(AddVersionToWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(interface{}, error) {
			},
		}, &deploymentspb.AddVersionUpdateArgs{
			Version: tv.DeploymentVersionString(),
		})
	}, 0*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		updateArgs := &deploymentspb.SetRampingVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 tv.DeploymentVersionString(),
			Percentage:              50,
			IgnoreMissingTaskQueues: true,
		}
		// Setting the new version as current version
		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)
	}, 1*time.Millisecond)

	// Update #1 clears the validator and is then halted during activity processing. This is
	// to simulate the workflow giving up control which shall allow update #2 to clear the validator.
	// However, update #2 cannot yet start being processed since update #1 holds the workflow lock and
	// will only release it after it has completed processing.
	haltUpdate := make(chan struct{})
	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			if args.Version == tv.DeploymentVersionString() {
				// Halt the activity to simulate workflow giving up control while processing the first update.
				<-haltUpdate
			}
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	s.env.RegisterDelayedCallback(func() {
		updateArgs := &deploymentspb.SetRampingVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 tv.DeploymentVersionString(),
			Percentage:              50,
			IgnoreMissingTaskQueues: true,
		}
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
	}, 1*time.Millisecond)

	// Close the channel to unblock the activity and allow update #1 to complete.
	// Note: This is called after update #2 begins being processed since it's fired after 2 milliseconds.
	s.env.RegisterDelayedCallback(func() {
		close(haltUpdate)
	}, 2*time.Millisecond)

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

	// To call the UnversionedRamp activity, we first set the current version to be tv.DeploymentVersionString() and then ramp "unversioned".

	// Step 1: Register the new version in the worker deployment.
	var a *Activities
	s.env.OnActivity(a.RegisterWorkerInVersion, mock.Anything, mock.Anything).Once().Return(nil)
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(RegisterWorkerInWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, &deploymentspb.RegisterWorkerInWorkerDeploymentArgs{
			TaskQueueName: tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MaxTaskQueues: 100,
			Version:       tv.DeploymentVersion(),
		})
	}, 0*time.Millisecond)

	// Step 2: Set current version to be tv.DeploymentVersionString()
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(nil, nil)
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Version:                 tv.DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	// Step 3: Ramp "__unversioned__"

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
	if totalWorkers%syncBatchSize == 0 {
		totalBatches = totalWorkers / syncBatchSize
	} else {
		totalBatches = totalWorkers/syncBatchSize + 1
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
	}, 2*time.Millisecond)

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
	})

	s.True(s.env.IsWorkflowCompleted())

}
