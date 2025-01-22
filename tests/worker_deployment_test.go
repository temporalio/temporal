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

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	WorkerDeploymentSuite struct {
		testcore.FunctionalTestSuite
	}
)

func TestWorkerDeploymentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkerDeploymentSuite))
}

func (s *WorkerDeploymentSuite) SetupSuite() {
	s.FunctionalTestSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.EnableDeploymentVersions.Key():                   true,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key():        true,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,
	}))
}

func (s *WorkerDeploymentSuite) SetupTest() {
	s.FunctionalTestSuite.SetupTest()
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *WorkerDeploymentSuite) pollFromDeployment(ctx context.Context, taskQueue *taskqueuepb.TaskQueue,
	deploymentName string, version string) {
	tv := testvars.New(s)

	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  tv.Any().String(),
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			UseVersioning:        true,
			BuildId:              version,
			DeploymentSeriesName: deploymentName,
		},
	})
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	// Starting two versions of the deployment
	firstVersion := tv.WithDeploymentVersionNumber(1)
	secondVersion := tv.WithDeploymentVersionNumber(2)

	go s.pollFromDeployment(ctx, tv.TaskQueue(), tv.DeploymentName(), firstVersion.DeploymentVersion())
	go s.pollFromDeployment(ctx, tv.TaskQueue(), tv.DeploymentName(), secondVersion.DeploymentVersion())

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentName(),
		})
		a.NoError(err)
		a.NotNil(resp.GetWorkerDeploymentInfo())
		a.Equal(tv.DeploymentName(), resp.GetWorkerDeploymentInfo().GetName())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		a.Equal(2, len(resp.GetWorkerDeploymentInfo().GetVersionSummaries()))

		if len(resp.GetWorkerDeploymentInfo().GetVersionSummaries()) < 2 {
			return
		}
		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetVersion())
		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetVersion())

		versions := []string{
			resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetVersion(),
			resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetVersion(),
		}
		a.Contains(versions, firstVersion.DeploymentVersion())
		a.Contains(versions, secondVersion.DeploymentVersion())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetCreateTime())
		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetCreateTime())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetCreateTime())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithDeploymentVersionNumber(1)
	secondVersion := tv.WithDeploymentVersionNumber(2)

	// Start deployment version workflow + worker-deployment workflow. Only one version is stared manually
	// to prevent erroring out in the successive DescribeWorkerDeployment call.
	go s.pollFromDeployment(ctx, tv.TaskQueue(), tv.DeploymentName(), firstVersion.DeploymentVersion())

	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentName(),
		})
		a.NoError(err)
		a.Equal("", resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version
	_, _ = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentName(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion()},
	})

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentName(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersion(), resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set second version as current version
	_, _ = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentName(),
		Version:        &wrapperspb.StringValue{Value: secondVersion.DeploymentVersion()},
	})

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentName(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersion(), resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion_Idempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithDeploymentVersionNumber(1)

	// Set first version as current version
	resp, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentName(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion()},
	})
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal("", resp.PreviousVersion)

	// Set first version as current version again
	resp, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentName(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion()},
	})
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal(firstVersion.DeploymentVersion(), resp.PreviousVersion)
}

// Name is used by testvars. We use a shortened test name in variables so that physical task queue IDs
// do not grow larger than DB column limit (currently as low as 272 chars).
func (s *WorkerDeploymentSuite) Name() string {
	fullName := s.T().Name()
	if len(fullName) <= 30 {
		return fullName
	}
	return fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
}
