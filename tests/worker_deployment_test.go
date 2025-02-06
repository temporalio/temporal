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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (s *WorkerDeploymentSuite) pollFromDeploymentExpectFail(ctx context.Context, tv *testvars.TestVars) {
	_, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.Error(err)
}

func (s *WorkerDeploymentSuite) ensureCreateVersion(ctx context.Context, tv *testvars.TestVars) {
	s.Eventually(func() bool {
		respV, _ := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		return len(respV.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()) > 0
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) ensureCreateVersionInDeployment(
	tv *testvars.TestVars,
) {
	v := tv.DeploymentVersionString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		res, _ := s.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})

		found := false
		if res != nil {
			for _, vs := range res.GetWorkerDeploymentInfo().GetVersionSummaries() {
				if vs.GetVersion() == v {
					found = true
				}
			}
		}
		a.True(found)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) ensureCreateDeployment(
	tv *testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		res, _ := s.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
		a.NotNil(res)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) startVersionWorkflow(ctx context.Context, tv *testvars.TestVars) {
	go s.pollFromDeployment(ctx, tv)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeploymentVersionLimits() {
	// TODO (carly): check the error messages that poller receives in each case and make sense they are informative and appropriate (e.g. do not expose internal stuff)

	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	tv := testvars.New(s)

	// First deployment version should be fine
	go s.pollFromDeployment(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// pollers of second version in the same deployment should be rejected
	s.pollFromDeploymentExpectFail(ctx, tv.WithBuildIDNumber(2))

	// But first version of another deployment fine
	tv2 := tv.WithDeploymentSeriesNumber(2)
	go s.pollFromDeployment(ctx, tv2)
	s.ensureCreateVersionInDeployment(tv2)

	// pollers of the second TQ in the same deployment version should be rejected
	s.pollFromDeploymentExpectFail(ctx, tv.WithTaskQueueNumber(2))
}

func (s *WorkerDeploymentSuite) TestNamespaceDeploymentsLimit() {
	// TODO (carly): check the error messages that poller receives in each case and make sense they are informative and appropriate (e.g. do not expose internal stuff)
	s.T().Skip() // Need to separate this test so other tests do not create deployment in the same NS

	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxDeployments, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	tv := testvars.New(s)

	// First deployment version should be fine
	go s.pollFromDeployment(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// pollers of the second deployment version should be rejected
	s.pollFromDeploymentExpectFail(ctx, tv.WithDeploymentSeriesNumber(2))
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_TwoVersions() {
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
		a.Contains(versions, firstVersion.DeploymentVersionString())
		a.Contains(versions, secondVersion.DeploymentVersionString())

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

	go s.pollFromDeployment(ctx, firstVersion)

	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version
	s.setCurrentVersion(ctx, firstVersion, worker_versioning.UnversionedVersionId, true, "")

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set a new second version and set it as the current version
	go s.pollFromDeployment(ctx, secondVersion)
	s.setCurrentVersion(ctx, secondVersion, firstVersion.DeploymentVersionString(), true, "")

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestConflictToken_Describe_SetCurrent_SetRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(ctx, firstVersion)
	go s.pollFromDeployment(ctx, secondVersion)

	var cT []byte
	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	s.ensureCreateVersionInDeployment(firstVersion)
	// Set first version as current version
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		ConflictToken:  cT,
	})
	s.Nil(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	// Set a new second version and set it as the current version
	go s.pollFromDeployment(ctx, secondVersion)
	_, _ = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentSeries(),
		Version:                 secondVersion.DeploymentVersionString(),
		Percentage:              5,
		ConflictToken:           cT,
		IgnoreMissingTaskQueues: true, // here until we have 'has version started' safeguard in place
	})
	s.Nil(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestConflictToken_SetCurrent_SetRamping_Wrong() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(ctx, firstVersion)

	cTWrong, _ := time.Now().MarshalBinary() // wrong token
	// Wait until deployment exists
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version with wrong token
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		ConflictToken:  cTWrong,
	})
	s.NotNil(err)

	// Set first version as ramping version with wrong token
	_, err = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		Percentage:     5,
		ConflictToken:  cTWrong,
	})
	s.NotNil(err)
}

func (s *WorkerDeploymentSuite) TestSetCurrent_NoDeploymentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	// Setting version as current version should error since there is no created deployment version
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		Version:                 tv.DeploymentVersionString(),
		IgnoreMissingTaskQueues: true,
	})
	s.Error(err)
	s.Contains(err.Error(), "workflow not found for ID")
}
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Idempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)

	s.startVersionWorkflow(ctx, firstVersion)

	// Set first version as current version
	s.setCurrentVersion(ctx, firstVersion, worker_versioning.UnversionedVersionId, true, "")

	// Set first version as current version again
	s.setCurrentVersion(ctx, firstVersion, firstVersion.DeploymentVersionString(), true, "")
}

// Testing ListWorkerDeployments

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_OneVersion_OneDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	s.startVersionWorkflow(ctx, tv)

	expectedDeploymentSummaries := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		&deploymentpb.RoutingConfig{},
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{expectedDeploymentSummaries})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_NoRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            firstVersion.DeploymentVersionString(),
		CurrentVersionChangedTime: timestamppb.Now(),
	}

	s.startVersionWorkflow(ctx, firstVersion)
	s.startVersionWorkflow(ctx, secondVersion)

	s.setCurrentVersion(ctx, firstVersion, worker_versioning.UnversionedVersionId, true, "")
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

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_OneRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	currentVersionVars := tv.WithBuildIDNumber(1)
	rampingVersionVars := tv.WithBuildIDNumber(2)

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            currentVersionVars.DeploymentVersionString(),
		CurrentVersionChangedTime: timestamppb.Now(),
		RampingVersion:            rampingVersionVars.DeploymentVersionString(),
		RampingVersionPercentage:  50,
		RampingVersionChangedTime: timestamppb.Now(),
	}

	s.startVersionWorkflow(ctx, currentVersionVars)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "") // starts first version's version workflow + set it to current
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	}) // starts second version's version workflow + set it to ramping

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

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_RampingVersionPercentageChange_RampingChangedTime() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tv := testvars.New(s)

	s.startVersionWorkflow(ctx, tv)
	s.setAndVerifyRampingVersion(ctx, tv, false, 50, true, "", nil) // set version as ramping
	rampingVersionChangedTime := timestamppb.Now()

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            worker_versioning.UnversionedVersionId,
		CurrentVersionChangedTime: nil,
		RampingVersion:            tv.DeploymentVersionString(),
		RampingVersionPercentage:  50,
		RampingVersionChangedTime: rampingVersionChangedTime,
	}

	// to simulate time passing before the next ramping version update
	//nolint:forbidigo
	time.Sleep(2 * time.Second)

	// modify ramping version percentage
	s.setAndVerifyRampingVersion(ctx, tv, false, 75, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    tv.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

	// only the ramping version percentage should be updated, not the ramping version update time
	// since we are not changing the ramping version
	routingInfo.RampingVersionPercentage = 75

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		rampingVersionChangedTime,
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tv := testvars.New(s)

	expectedDeploymentSummaries := s.createVersionsInDeployments(ctx, tv, 5)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
		PageSize:  1,
	}, expectedDeploymentSummaries)
}

// Testing SetWorkerDeploymentRampingVersion
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Ramping_With_Current() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	currentVersionVars := tv.WithBuildIDNumber(2)

	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.startVersionWorkflow(ctx, currentVersionVars)

	// set version as ramping
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: tv.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: timestamppb.Now(),
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: nil,
			},
		},
	})

	// set current version
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "")

	// fresh DescribeWorkerDeployment call
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: tv.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: timestamppb.Now(),
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
			},
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_DuplicateRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	rampingVersionVars := testvars.New(s).WithBuildIDNumber(1)

	s.startVersionWorkflow(ctx, rampingVersionVars)

	// set version as ramping
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: rampingVersionVars.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: rampingVersionVars.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: timestamppb.Now(),
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: nil,
			},
		},
	})

	// setting version as ramping again
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Invalid_SetCurrent_To_Ramping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	currentVersionVars := testvars.New(s).WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, currentVersionVars)
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "")

	expectedError := fmt.Errorf("Ramping version %s is already current", currentVersionVars.DeploymentVersionString())
	s.setAndVerifyRampingVersion(ctx, currentVersionVars, false, 50, true, expectedError.Error(), nil) // setting current version to ramping should fails

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: currentVersionVars.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: currentVersionVars.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",  // no ramping info should be set
				RampingVersionPercentage:  0,   // no ramping info should be set
				RampingVersionChangedTime: nil, // no ramping info should be set
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
			},
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_ModifyExistingRampVersionPercentage() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil) // set version as ramping

	// modify ramping version percentage
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 75, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_WithCurrent_Unset_Ramp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	currentVersionVars := tv.WithBuildIDNumber(2)

	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.startVersionWorkflow(ctx, currentVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil)                // set version as ramping
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "") // set version as curent

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: tv.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: timestamppb.Now(),
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
			},
		},
	})

	// unset ramping version
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: tv.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",
				RampingVersionPercentage:  0,
				RampingVersionChangedTime: nil,
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
			},
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_SetRampingAsCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil)
	// set ramping version as current
	s.setCurrentVersion(ctx, rampingVersionVars, worker_versioning.UnversionedVersionId, true, "")

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name: tv.DeploymentSeries(),
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",                // no ramping info should be set
				RampingVersionPercentage:  0,                 // no ramping info should be set
				RampingVersionChangedTime: timestamppb.Now(), // ramping version got updated to ""
				CurrentVersion:            rampingVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
			},
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_NoCurrent_Unset_Ramp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil)
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})
}

// Should see that the current version of the task queues becomes unversioned
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_NoRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)
	currentVars := tv.WithBuildIDNumber(1)
	go s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersion(ctx, currentVars)

	// check that the current version's task queues have current version unversioned to start
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// set current and check that the current version's task queues have new current version

	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set current unversioned and check that the current version's task queues have current version unversioned again
	s.setCurrentVersionUnversionedOption(ctx, currentVars, true, currentVars.DeploymentVersionString(), true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// check that deployment has current version == __unversioned__
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
}

// Should see that the current version of the task queue becomes unversioned, and the unversioned ramping version of the task queue is removed
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_PromoteUnversionedRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)
	currentVars := tv.WithBuildIDNumber(1)
	s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersion(ctx, currentVars)

	// make the current version versioned, so that we can set ramp to unversioned
	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, "", nil)
	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)

	// set current to unversioned
	s.setCurrentVersionUnversionedOption(ctx, tv, true, currentVars.DeploymentVersionString(), true, "")

	// check that the current version's task queues have ramping version == "" and current version == "__unversioned__"
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
}

// Should see it fail because unversioned is already current
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Unversioned_UnversionedCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)
	rampingVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVars)
	s.setAndVerifyRampingVersionUnversionedOption(ctx, rampingVars, true, false, 50, true, "Ramping version __unversioned__ is already current", nil)
}

// Should see that the ramping version of the task queues in the current version is unversioned
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Unversioned_VersionedCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)
	currentVars := tv.WithBuildIDNumber(1)
	s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersion(ctx, currentVars)

	// check that the current version's task queues have ramping version == ""
	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, "", nil)

	// check that deployment has ramping version == __unversioned__
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())

	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
}

func (s *WorkerDeploymentSuite) TestTwoPollers_EnsureCreateVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer func() {
		cancel()
	}()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1)
	tv2 := tv.WithBuildIDNumber(2)

	go s.pollFromDeployment(ctx, tv1)
	go s.pollFromDeployment(ctx, tv2)
	s.ensureCreateVersion(ctx, tv1)
	s.ensureCreateVersion(ctx, tv2)
}

func (s *WorkerDeploymentSuite) verifyTaskQueueVersioningInfo(ctx context.Context, tq *taskqueuepb.TaskQueue, expectedCurrentVersion, expectedRampingVersion string, expectedPercentage float32) {
	tqDesc, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: tq,
	})
	s.Nil(err)
	s.Equal(expectedCurrentVersion, tqDesc.GetVersioningInfo().GetCurrentVersion())
	s.Equal(expectedRampingVersion, tqDesc.GetVersioningInfo().GetRampingVersion())
	s.Equal(expectedPercentage, tqDesc.GetVersioningInfo().GetRampingVersionPercentage())
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_ValidDelete() {
	s.T().Skip("skipping this test for now until I make TTL of pollerHistoryTTL configurable by dynamic config.")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv1.DeploymentSeries(), tv1.BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}
	input := &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: timestamppb.New(time.Now()),
		LastCheckedTime: timestamppb.New(time.Now()),
	}
	marshaledData, err := input.Marshal()
	s.NoError(err)
	signalPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
				Data: marshaledData,
			},
		},
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, tv1.ClientIdentity())
	s.Nil(err)

	// Wait for pollers going away
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv1.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		assert.NoError(t, err)
		assert.Empty(t, resp.Pollers)
	}, 10*time.Second, time.Second)

	// delete succeeds
	s.tryDeleteVersion(ctx, tv1, true)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version)
			}
		}
	}, time.Second*5, time.Millisecond*200)

	// Deleting the worker deployment should succeed since there are no associated versions left
	_, err = s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// Describe Worker Deployment should give not found
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		_, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.Error(err)
		var nfe *serviceerror.NotFound
		a.True(errors.As(err, &nfe))
	}, time.Second*5, time.Millisecond*200)

	// ListDeployments should not show the closed/deleted Worker Deployment
	listResp, err := s.FrontendClient().ListWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	})
	s.Nil(err)
	for _, dInfo := range listResp.GetWorkerDeployments() {
		s.NotEqual(tv1.DeploymentSeries(), dInfo.GetName())
	}
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_Idempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		_, err := s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
			Identity:       tv1.ClientIdentity(),
		})
		a.NoError(err)
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_InvalidDelete() {

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version and deployment workflow to exist
	go s.pollFromDeployment(ctx, tv1)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if a.NotEmpty(resp.GetWorkerDeploymentInfo().GetVersionSummaries()) {
			a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].Version)
		}
	}, time.Second*5, time.Millisecond*200)

	// Delete the worker deployment should fail since there are versions associated with it
	_, err := s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Error(err)
}

func (s *WorkerDeploymentSuite) tryDeleteVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	expectSuccess bool,
) {
	_, err := s.FrontendClient().DeleteWorkerDeploymentVersion(ctx, &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
	})
	if expectSuccess {
		s.Nil(err)
	} else {
		s.Error(err)
	}
}

// todo: add validations for VersionSummaries
func (s *WorkerDeploymentSuite) verifyDescribeWorkerDeployment(
	actualResp *workflowservice.DescribeWorkerDeploymentResponse,
	expectedResp *workflowservice.DescribeWorkerDeploymentResponse,
) {
	// relaxed timestamp constraint since the tests make sync calls, which could theoretically take seconds.
	maxDurationBetweenTimeStamps := 2 * time.Second

	s.True((actualResp == nil) == (expectedResp == nil))
	s.True((actualResp.GetWorkerDeploymentInfo() == nil) == (expectedResp.GetWorkerDeploymentInfo() == nil))
	s.True((actualResp.GetWorkerDeploymentInfo().GetRoutingConfig() == nil) == (expectedResp.GetWorkerDeploymentInfo().GetRoutingConfig() == nil))
	s.Equal(expectedResp.GetWorkerDeploymentInfo().GetName(), actualResp.GetWorkerDeploymentInfo().GetName())

	s.True(expectedResp.GetWorkerDeploymentInfo().GetCreateTime().AsTime().Sub(actualResp.GetWorkerDeploymentInfo().GetCreateTime().AsTime()) < maxDurationBetweenTimeStamps)

	actualRoutingInfo := actualResp.GetWorkerDeploymentInfo().GetRoutingConfig()
	expectedRoutingInfo := expectedResp.GetWorkerDeploymentInfo().GetRoutingConfig()

	s.Equal(expectedRoutingInfo.GetRampingVersion(), actualRoutingInfo.GetRampingVersion())
	s.Equal(expectedRoutingInfo.GetRampingVersionPercentage(), actualRoutingInfo.GetRampingVersionPercentage())
	s.True(expectedRoutingInfo.GetRampingVersionChangedTime().AsTime().Sub(actualRoutingInfo.GetRampingVersionChangedTime().AsTime()) < maxDurationBetweenTimeStamps)

	s.Equal(expectedRoutingInfo.GetCurrentVersion(), actualRoutingInfo.GetCurrentVersion())
	s.True(expectedRoutingInfo.GetCurrentVersionChangedTime().AsTime().Sub(actualRoutingInfo.GetCurrentVersionChangedTime().AsTime()) < maxDurationBetweenTimeStamps)

}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
	expectedResp *workflowservice.SetWorkerDeploymentRampingVersionResponse,
) {
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, false, unset, percentage, ignoreMissingTaskQueues, expectedError, expectedResp)
}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersionUnversionedOption(
	ctx context.Context,
	tv *testvars.TestVars,
	unversioned bool,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
	expectedResp *workflowservice.SetWorkerDeploymentRampingVersionResponse,
) {
	version := tv.DeploymentVersionString()
	if unversioned {
		version = worker_versioning.UnversionedVersionId
	}
	if unset {
		version = ""
		percentage = 0
	}
	if !unversioned && !unset {
		s.ensureCreateVersionInDeployment(tv)
	} else {
		s.ensureCreateDeployment(tv)
	}
	resp, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		Version:                 version,
		Percentage:              float32(percentage),
		Identity:                tv.Any().String(),
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
	s.Equal(expectedResp.GetPreviousVersion(), resp.GetPreviousVersion())
	s.Equal(expectedResp.GetPreviousPercentage(), resp.GetPreviousPercentage())
}

func (s *WorkerDeploymentSuite) setCurrentVersion(ctx context.Context, tv *testvars.TestVars, previousCurrent string, ignoreMissingTaskQueues bool, expectedError string) {
	s.setCurrentVersionUnversionedOption(ctx, tv, false, previousCurrent, ignoreMissingTaskQueues, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionUnversionedOption(ctx context.Context, tv *testvars.TestVars, unversioned bool, previousCurrent string, ignoreMissingTaskQueues bool, expectedError string) {
	version := tv.DeploymentVersionString()
	if unversioned {
		version = worker_versioning.UnversionedVersionId
		s.ensureCreateDeployment(tv)
	} else {
		s.ensureCreateVersionInDeployment(tv)
	}

	resp, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		Version:                 version,
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal(previousCurrent, resp.PreviousVersion)
}

func (s *WorkerDeploymentSuite) createVersionsInDeployments(ctx context.Context, tv *testvars.TestVars, n int) []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	var expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary

	for i := 0; i < n; i++ {
		deployment := tv.WithDeploymentSeriesNumber(i)
		version := deployment.WithBuildIDNumber(i)

		s.startVersionWorkflow(ctx, version)
		s.setCurrentVersion(ctx, version, worker_versioning.UnversionedVersionId, true, "")

		expectedDeployment := s.buildWorkerDeploymentSummary(
			deployment.DeploymentSeries(),
			timestamppb.Now(),
			&deploymentpb.RoutingConfig{
				CurrentVersion:            version.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
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
	maxDurationBetweenTimeStamps := 5 * time.Second
	if expectedSummary.Name != actualSummary.Name {
		s.Logger.Info("Name mismatch")
		return false
	}
	if expectedSummary.CreateTime.AsTime().Sub(actualSummary.CreateTime.AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Create time mismatch")
		return false
	}

	// Current version checks
	if expectedSummary.RoutingConfig.GetCurrentVersion() != actualSummary.RoutingConfig.GetCurrentVersion() {
		s.Logger.Info("Current version mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetCurrentVersionChangedTime().AsTime().Sub(actualSummary.RoutingConfig.GetCurrentVersionChangedTime().AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Current version update time mismatch")
		return false
	}

	// Ramping version checks
	if expectedSummary.RoutingConfig.GetRampingVersion() != actualSummary.RoutingConfig.GetRampingVersion() {
		s.Logger.Info("Ramping version mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetRampingVersionPercentage() != actualSummary.RoutingConfig.GetRampingVersionPercentage() {
		s.Logger.Info("Ramping version percentage mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetRampingVersionChangedTime().AsTime().Sub(actualSummary.RoutingConfig.GetRampingVersionChangedTime().AsTime()) > maxDurationBetweenTimeStamps {
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
	routingConfig *deploymentpb.RoutingConfig,
) *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	return &workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		Name:          deploymentName,
		CreateTime:    createTime,
		RoutingConfig: routingConfig,
	}
}

// Name is used by testvars. We use a shortened test name in variables so that physical task queue IDs
// do not grow larger than DB column limit (currently as low as 272 chars).
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
