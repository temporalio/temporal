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
	"strings"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
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
	s.env.SetWorkflowRunTimeout(30 * time.Second)
}

func (s *WorkerDeploymentSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

// To incorporate testVars in our tests.
func (s *WorkerDeploymentSuite) Name() string {
	fullName := s.T().Name()
	if len(fullName) <= 30 {
		return fullName
	}
	short := fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
	return strings.Replace(short, ".", "|", -1)
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
	tv := testvars.New(s)
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
				s.Fail("update should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)
	}, 1*time.Millisecond)

	// Update #1 clears the validator but is then halted because of a sleep in the activity. This is
	// to simulate the workflow giving up control which shall allow update #2 to clear the validator.
	// However, update #2 cannot yet start being processed since update #1 holds the lock and will only
	// release it after it has completed processing.
	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			if args.Version == tv.DeploymentVersionString() {
				// Sleep in the activity to simulate workflow giving up control while processing the first update.
				time.Sleep(1 * time.Second)
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
				// Update #2 clears the validator and waits for the first update to complete. However,
				// during processing, it should be rejected since completion of the first update changed the state.
				s.ErrorContains(err, errNoChangeType)
			},
		}, updateArgs)
	}, 1*time.Millisecond)

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
	// Workflow times out so an error is expected.
	s.Error(s.env.GetWorkflowError())
	s.ErrorContains(s.env.GetWorkflowError(), "deadline exceeded (type: ScheduleToClose)")
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
	tv := testvars.New(s)
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
			IgnoreMissingTaskQueues: true,
			Percentage:              50,
		}
		// Setting the new version as current version
		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
			},
		}, updateArgs)
	}, 1*time.Millisecond)

	// Update #1 clears the validator but is then halted because of a sleep in the activity. This is
	// to simulate the workflow giving up control which shall allow update #2 to clear the validator.
	// However, update #2 cannot yet start being processed since update #1 holds the lock and will only
	// release it after it has completed processing.
	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			if args.Version == tv.DeploymentVersionString() {
				// Sleep in the activity to simulate workflow giving up control while processing the first update.
				time.Sleep(1 * time.Second)
			}
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	s.env.RegisterDelayedCallback(func() {
		updateArgs := &deploymentspb.SetRampingVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 tv.DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
			Percentage:              50,
		}
		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #2 should have been accepted by the validator")
			},
			OnAccept: func() {
			},
			OnComplete: func(a interface{}, err error) {
				// Update #2 clears the validator and waits for the first update to complete. However,
				// during processing, it should be rejected since completion of the first update changed the state.
				s.ErrorContains(err, errNoChangeType)
			},
		}, updateArgs)
	}, 1*time.Millisecond)

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
	// Workflow times out so an error is expected.
	s.Error(s.env.GetWorkflowError())
	s.ErrorContains(s.env.GetWorkflowError(), "deadline exceeded (type: ScheduleToClose)")
}
