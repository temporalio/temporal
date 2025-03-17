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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
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

// Test_SyncState_BatchSize verifies if the right number of batches are created during the sync state activity
func (s *VersionWorkflowSuite) Test_SyncState_BatchSize() {
	tv := testvars.New(s.T())

	s.env.RegisterDelayedCallback(func() {
		syncStateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: timestamppb.New(time.Now()),
			CurrentSinceTime:  timestamppb.New(time.Now()),
			RampingSinceTime:  timestamppb.New(time.Now()),
			RampPercentage:    100,
		}
		s.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
			},
		}, syncStateArgs)
	})
}
