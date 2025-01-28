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
	"math/rand"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
func (s *WorkerDeploymentSuite) pollFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	// Starting two versions of the deployment
	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	go s.pollFromDeployment(ctx, firstVersion)
	go s.pollFromDeployment(ctx, secondVersion)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.NotNil(resp.GetWorkerDeploymentInfo())
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentInfo().GetName())

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
		a.Contains(versions, firstVersion.DeploymentVersion().GetVersion())
		a.Contains(versions, secondVersion.DeploymentVersion().GetVersion())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetCreateTime())
		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetCreateTime())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetCreateTime())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	// Start deployment version workflow + worker-deployment workflow. Only one version is stared manually
	// to prevent erroring out in the successive DescribeWorkerDeployment call.
	go s.pollFromDeployment(ctx, firstVersion)

	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal("", resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version
	_, _ = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion().GetVersion()},
	})

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersion().GetVersion(), resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set second version as current version
	_, _ = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        &wrapperspb.StringValue{Value: secondVersion.DeploymentVersion().GetVersion()},
	})

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersion().GetVersion(), resp.GetWorkerDeploymentInfo().GetRoutingInfo().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion_Idempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)

	// Set first version as current version
	resp, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion().GetVersion()},
	})
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal("", resp.PreviousVersion)

	// Set first version as current version again
	resp, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        &wrapperspb.StringValue{Value: firstVersion.DeploymentVersion().GetVersion()},
	})
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal(firstVersion.DeploymentVersion().GetVersion(), resp.PreviousVersion)
}

// TestConcurrentSetCurrentVersion_Poll tests that no error is thrown when concurrent operations
// try to set a current version and poll from the deployment.
func (s *WorkerDeploymentSuite) TestConcurrentSetCurrentVersion_Poll() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	go s.pollFromDeployment(ctx, tv)

	// Set current version concurrently
	s.setCurrentVersion(ctx, tv)
}

// Testing ListWorkerDeployments

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_OneVersion_OneDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	go s.pollFromDeployment(ctx, tv.WithBuildIDNumber(rand.Intn(1000)))

	expectedDeploymentSummaries := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		&deploymentpb.RoutingInfo{},
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{expectedDeploymentSummaries})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(rand.Intn(1000))
	secondVersion := tv.WithBuildIDNumber(rand.Intn(1000))
	routingInfo := &deploymentpb.RoutingInfo{
		CurrentVersion:           firstVersion.DeploymentVersion().GetVersion(),
		CurrentVersionUpdateTime: timestamppb.Now(),
	}

	s.setCurrentVersion(ctx, firstVersion)      // starts first version's version workflow
	go s.pollFromDeployment(ctx, secondVersion) // starts second version's version workflow

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		routingInfo,
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_OnePage() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	expectedDeploymentSummaries := s.createVersionsInDeployments(ctx, tv, 2)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, expectedDeploymentSummaries)
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_MultiplePages() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	expectedDeploymentSummaries := s.createVersionsInDeployments(ctx, tv, 5)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
		PageSize:  1,
	}, expectedDeploymentSummaries)
}

func (s *WorkerDeploymentSuite) setCurrentVersion(ctx context.Context, tv *testvars.TestVars) {
	resp, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        &wrapperspb.StringValue{Value: tv.DeploymentVersion().GetVersion()},
	})
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal("", resp.PreviousVersion)
}

func (s *WorkerDeploymentSuite) createVersionsInDeployments(ctx context.Context, tv *testvars.TestVars, n int) []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	var expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary

	for i := 0; i < n; i++ {
		deployment := tv.WithDeploymentSeriesNumber(i)
		version := deployment.WithBuildIDNumber(i)

		s.setCurrentVersion(ctx, version)

		expectedDeployment := s.buildWorkerDeploymentSummary(
			deployment.DeploymentSeries(),
			timestamppb.Now(),
			&deploymentpb.RoutingInfo{
				CurrentVersion:           version.DeploymentVersion().GetVersion(),
				CurrentVersionUpdateTime: timestamppb.Now(),
			},
		)
		expectedDeploymentSummaries = append(expectedDeploymentSummaries, expectedDeployment)
	}

	return expectedDeploymentSummaries
}

func (s *WorkerDeploymentSuite) verifyWorkerDeploymentSummary(
	expectedSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
	actualSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) bool {
	maxDurationBetweenTimeStamps := 1 * time.Second
	if expectedSummary.Name != actualSummary.Name {
		s.Logger.Info("Name mismatch")
		return false
	}
	if expectedSummary.CreateTime.AsTime().Sub(actualSummary.CreateTime.AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Create time mismatch")
		return false
	}

	// Current version checks
	if expectedSummary.RoutingInfo.GetCurrentVersion() != actualSummary.RoutingInfo.GetCurrentVersion() {
		s.Logger.Info("Current version mismatch")
		return false
	}
	if expectedSummary.RoutingInfo.GetCurrentVersionUpdateTime().AsTime().Sub(actualSummary.RoutingInfo.GetCurrentVersionUpdateTime().AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Current version update time mismatch")
		return false
	}

	// Ramping version checks
	if expectedSummary.RoutingInfo.GetRampingVersion() != actualSummary.RoutingInfo.GetRampingVersion() {
		s.Logger.Info("Ramping version mismatch")
		return false
	}
	if expectedSummary.RoutingInfo.GetRampingVersionPercentage() != actualSummary.RoutingInfo.GetRampingVersionPercentage() {
		s.Logger.Info("Ramping version percentage mismatch")
		return false
	}
	if expectedSummary.RoutingInfo.GetRampingVersionUpdateTime().AsTime().Sub(actualSummary.RoutingInfo.GetRampingVersionUpdateTime().AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Ramping version update time mismatch")
		return false
	}

	return true
}

func (s *WorkerDeploymentSuite) listWorkerDeployments(ctx context.Context, request *workflowservice.ListWorkerDeploymentsRequest) ([]*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary, error) {
	var resp *workflowservice.ListWorkerDeploymentsResponse
	var err error
	var deploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary
	for resp == nil || len(resp.NextPageToken) > 0 {
		resp, err = s.FrontendClient().ListWorkerDeployments(ctx, request)
		if err != nil {
			return nil, err
		}
		deploymentSummaries = append(deploymentSummaries, resp.GetWorkerDeployments()...)
		request.NextPageToken = resp.NextPageToken
	}
	return deploymentSummaries, nil
}

func (s *WorkerDeploymentSuite) startAndValidateWorkerDeployments(
	ctx context.Context,
	request *workflowservice.ListWorkerDeploymentsRequest,
	expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) {

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		actualDeploymentSummaries, err := s.listWorkerDeployments(ctx, request)
		a.NoError(err)
		if len(actualDeploymentSummaries) < len(expectedDeploymentSummaries) {
			return
		}

		for _, expectedDeploymentSummary := range expectedDeploymentSummaries {
			deploymentSummaryValidated := false
			for _, actualDeploymentSummary := range actualDeploymentSummaries {
				deploymentSummaryValidated = deploymentSummaryValidated ||
					s.verifyWorkerDeploymentSummary(expectedDeploymentSummary, actualDeploymentSummary)
			}
			a.True(deploymentSummaryValidated)
		}
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) buildWorkerDeploymentSummary(
	deploymentName string, createTime *timestamppb.Timestamp,
	routingInfo *deploymentpb.RoutingInfo,
) *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	return &workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		Name:        deploymentName,
		CreateTime:  createTime,
		RoutingInfo: routingInfo,
	}
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
