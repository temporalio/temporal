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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testvars"
	deploymentwf "go.temporal.io/server/service/worker/deployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*

tests to write:

1. TestBasics to test basic deployment workflow start and use DescribeDeployment to query the deployment
2. Tests to register worker in a deployment and using DescribeDeployment for verification
*/

type (
	DeploymentSuite struct {
		testcore.FunctionalTestBase
		*require.Assertions
		sdkClient sdkclient.Client
	}
)

func (s *DeploymentSuite) setAssertions() {
	s.Assertions = require.New(s.T())
}

func TestDeploymentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DeploymentSuite))
}

func (s *DeploymentSuite) SetupSuite() {
	s.setAssertions()
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableDeployments.Key():                  true,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true,
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key():        true,
		dynamicconfig.MatchingEnableDeployments.Key():                  true,
		dynamicconfig.WorkerEnableDeployment.Key():                     true,

		// Reachability
		dynamicconfig.ReachabilityCacheOpenWFsTTL.Key():   testReachabilityCacheOpenWFsTTL,
		dynamicconfig.ReachabilityCacheClosedWFsTTL.Key(): testReachabilityCacheClosedWFsTTL,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,
	}
	s.SetDynamicConfigOverrides(dynamicConfigOverrides)
	s.FunctionalTestBase.SetupSuite("testdata/es_cluster.yaml")
}

func (s *DeploymentSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *DeploymentSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.setAssertions()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
}

func (s *DeploymentSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *DeploymentSuite) pollFromDeployment(ctx context.Context, taskQueue *taskqueuepb.TaskQueue,
	deployment *deploymentpb.Deployment) {
	a := s.Assertions

	_, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: taskQueue,
		Identity:  "random",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			UseVersioning:        true,
			BuildId:              deployment.BuildId,
			DeploymentSeriesName: deployment.SeriesName,
		},
	})
	if !errors.Is(err, context.Canceled) {
		// ctx canceled error is expected in most test cases
		a.Error(err)
	}
}

func (s *DeploymentSuite) TestDescribeDeployment_RegisterTaskQueue() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// presence of internally used delimiters (:) or escape
	// characters shouldn't break functionality
	seriesName := testcore.RandomizeStr("my-series|:|:")
	buildID := testcore.RandomizeStr("bgt:|")

	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}
	numberOfDeployments := 1

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, taskQueue, workerDeployment)

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeDeployment(ctx, &workflowservice.DescribeDeploymentRequest{
			Namespace:  s.Namespace(),
			Deployment: workerDeployment,
		})
		a.NoError(err)
		a.NotNil(resp.GetDeploymentInfo())
		a.NotNil(resp.GetDeploymentInfo().GetDeployment())

		a.Equal(seriesName, resp.GetDeploymentInfo().GetDeployment().GetSeriesName())
		a.Equal(buildID, resp.GetDeploymentInfo().GetDeployment().GetBuildId())

		a.Equal(numberOfDeployments, len(resp.GetDeploymentInfo().GetTaskQueueInfos()))
		if len(resp.GetDeploymentInfo().GetTaskQueueInfos()) < numberOfDeployments {
			return
		}
		a.Equal(taskQueue.Name, resp.GetDeploymentInfo().GetTaskQueueInfos()[0].Name)
		a.Equal(false, resp.GetDeploymentInfo().GetIsCurrent())
		// todo (Shivam) - please add a check for current time
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentSuite) TestGetCurrentDeployment_NoCurrentDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	seriesName := testcore.RandomizeStr("my-series")
	buildID := testcore.RandomizeStr("bgt")
	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	workflowID := deploymentwf.GenerateDeploymentSeriesWorkflowID(seriesName)
	query := fmt.Sprintf("WorkflowId = '%s' AND TemporalNamespaceDivision IS NOT NULL", workflowID)
	notFoundErr := fmt.Sprintf("workflow not found for ID: %s", workflowID)

	// GetCurrentDeployment on a non-existing series returns an error
	resp, err := s.FrontendClient().GetCurrentDeployment(ctx, &workflowservice.GetCurrentDeploymentRequest{
		Namespace:  s.Namespace(),
		SeriesName: seriesName,
	})
	s.Error(err)
	s.Equal(err.Error(), notFoundErr)
	s.Nil(resp)

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, taskQueue, workerDeployment)

	// Verify the existence of a deployment series
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: s.Namespace(),
			Query:     query,
		})
		a.NoError(err)
		a.Equal(int64(1), resp.GetCount())
	}, time.Second*5, time.Millisecond*200)

	// Fetch series workflow's current deployment - will be nil since we haven't set it
	resp, err = s.FrontendClient().GetCurrentDeployment(ctx, &workflowservice.GetCurrentDeploymentRequest{
		Namespace:  s.Namespace(),
		SeriesName: seriesName,
	})
	s.NoError(err)
	s.Nil(resp.GetCurrentDeploymentInfo())
}

// addDeploymentsAndVerifyListDeployments does the following:
// verifyDeploymentListInfo checks the equality between two DeploymentListInfo objects
func (s *DeploymentSuite) verifyDeploymentListInfo(expectedDeploymentListInfo *deploymentpb.DeploymentListInfo, receivedDeploymentListInfo *deploymentpb.DeploymentListInfo) bool {
	maxDurationBetweenTimeStamps := 1 * time.Millisecond
	if expectedDeploymentListInfo.Deployment.SeriesName != receivedDeploymentListInfo.Deployment.SeriesName {
		return false
	}
	if expectedDeploymentListInfo.Deployment.BuildId != receivedDeploymentListInfo.Deployment.BuildId {
		return false
	}
	if expectedDeploymentListInfo.IsCurrent != receivedDeploymentListInfo.IsCurrent {
		return false
	}
	if expectedDeploymentListInfo.CreateTime.AsTime().Sub(receivedDeploymentListInfo.CreateTime.AsTime()) > maxDurationBetweenTimeStamps {
		return false
	}
	return true
}

// verifyDeployments does the following:
// - makes a list deployments call with/without seriesFilter
// - validates the response with expectedDeployments
func (s *DeploymentSuite) verifyDeployments(ctx context.Context, request *workflowservice.ListDeploymentsRequest,
	expectedDeployments []*deploymentpb.DeploymentListInfo) {

	// list deployment call
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().ListDeployments(ctx, request)
		a.NoError(err)
		a.NotNil(resp)
		if resp == nil {
			return
		}
		a.NotNil(resp.GetDeployments())

		// check to stop eventuallyWithT from panicking since
		// it collects all possible errors
		if len(resp.GetDeployments()) < 1 {
			return
		}

		for _, expectedDeploymentListInfo := range expectedDeployments {

			deploymentListInfoValidated := false
			for _, receivedDeploymentListInfo := range resp.GetDeployments() {

				deploymentListInfoValidated = deploymentListInfoValidated ||
					s.verifyDeploymentListInfo(expectedDeploymentListInfo, receivedDeploymentListInfo)
			}
			a.True(deploymentListInfoValidated)
		}
	}, time.Second*5, time.Millisecond*200)
}

// startDeploymentsAndValidateList does the following:
// - starts deployment workflow(s)
// - calls verifyDeployments which lists + validates Deployments
func (s *DeploymentSuite) startDeploymentsAndValidateList(deploymentInfo []*deploymentpb.DeploymentListInfo, seriesFilter string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start deployment workflow(s)
	for _, listInfo := range deploymentInfo {
		go s.pollFromDeployment(ctx, taskQueue, listInfo.Deployment)
	}

	var expectedDeployments []*deploymentpb.DeploymentListInfo
	request := &workflowservice.ListDeploymentsRequest{
		Namespace: s.Namespace(),
	}
	if seriesFilter != "" {
		request.SeriesName = seriesFilter

		// pass only those deployments for verification which have seriesName == seriesFilter
		for _, dInfo := range deploymentInfo {
			if dInfo.Deployment.SeriesName == seriesFilter {
				expectedDeployments = append(expectedDeployments, dInfo)
			}
		}
	} else {
		// pass all deployments for verification which have been started
		expectedDeployments = deploymentInfo
	}

	s.verifyDeployments(ctx, request, expectedDeployments)
}

func (s *DeploymentSuite) buildDeploymentInfo(numberOfDeployments int) []*deploymentpb.DeploymentListInfo {
	deploymentInfo := make([]*deploymentpb.DeploymentListInfo, 0)
	for i := 0; i < numberOfDeployments; i++ {
		seriesName := testcore.RandomizeStr("my-series")
		buildID := testcore.RandomizeStr("bgt")
		indDeployment := &deploymentpb.Deployment{
			SeriesName: seriesName,
			BuildId:    buildID,
		}
		deploymentListInfo := &deploymentpb.DeploymentListInfo{
			Deployment: indDeployment,
			IsCurrent:  false,
			CreateTime: timestamppb.Now(),
		}
		deploymentInfo = append(deploymentInfo, deploymentListInfo)
	}

	return deploymentInfo
}

func (s *DeploymentSuite) TestListDeployments_VerifySingleDeployment() {
	deploymentInfo := s.buildDeploymentInfo(1)
	s.startDeploymentsAndValidateList(deploymentInfo, "")
}

func (s *DeploymentSuite) TestListDeployments_MultipleDeployments() {
	deploymentInfo := s.buildDeploymentInfo(5)
	s.startDeploymentsAndValidateList(deploymentInfo, "")
}

func (s *DeploymentSuite) TestListDeployments_MultipleDeployments_WithSeriesFilter() {
	deploymentInfo := s.buildDeploymentInfo(2)
	seriesFilter := deploymentInfo[0].Deployment.SeriesName
	s.startDeploymentsAndValidateList(deploymentInfo, seriesFilter)
}

// TODO Shivam - refactor the above test cases TestListDeployments_WithSeriesNameFilter + TestListDeployments_WithoutSeriesNameFilter
// Refactoring should be done in a way where we are validating the exact deployment (based on how many we create) - right now,
// the tests do validate the read API logic but are not the most assertive

// TODO Shivam - Add more getCurrentDeployment tests when SetCurrentDefaultBuildID API has been defined

func (s *DeploymentSuite) TestGetDeploymentReachability_OverrideUnversioned() {
	ctx := context.Background()

	// presence of internally used delimiters (:) or escape
	// characters shouldn't break functionality
	seriesName := testcore.RandomizeStr("my-series|:|:")
	buildID := testcore.RandomizeStr("bgt:|")
	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	s.createDeploymentAndWaitForExist(workerDeployment, taskQueue)

	// non-current deployment is unreachable
	s.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// start an unversioned workflow, set pinned deployment override --> deployment should be reachable
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}

	// set override on our new unversioned workflow
	updateOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: workerDeployment,
		},
	}
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: updateOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), updateOpts))

	// describe workflow and check that the versioning info has the override
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, updateOpts.GetVersioningOverride())
	// check that the deployment is now reachable, since an open workflow is using it via override
	s.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// TODO (carly): once sdk allows starting a deployment worker, start worker, complete workflow, and check for CLOSED_ONLY
	// TODO (carly): once SetCurrentDeployment is ready, check that a current deployment is reachable even with no workflows
	// TODO (carly): test starting a workflow execution on a current deployment, then getting reachability with no override
	// TODO (carly): check cache times (do I need to do this in functional when I have cache time tests in unit?)
}

func (s *DeploymentSuite) TestGetDeploymentReachability_NotFound() {
	ctx := context.Background()

	// presence of internally used delimiters (:) or escape
	// characters shouldn't break functionality
	seriesName := testcore.RandomizeStr("my-series|:|:")
	buildID := testcore.RandomizeStr("bgt:|")
	resp, err := s.FrontendClient().GetDeploymentReachability(ctx, &workflowservice.GetDeploymentReachabilityRequest{
		Namespace: s.Namespace(),
		Deployment: &deploymentpb.Deployment{
			SeriesName: seriesName,
			BuildId:    buildID,
		},
	})
	var notFound *serviceerror.NotFound
	s.NotNil(err)
	s.True(errors.As(err, &notFound))
	s.Nil(resp)
}

func (s *DeploymentSuite) checkDescribeWorkflowAfterOverride(
	ctx context.Context,
	wf *commonpb.WorkflowExecution,
	expectedOverride *workflowpb.VersioningOverride,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace(),
			Execution: wf,
		})
		a.NoError(err)
		a.NotNil(resp)
		a.NotNil(resp.GetWorkflowExecutionInfo())
		a.True(proto.Equal(expectedOverride, resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride()))
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *DeploymentSuite) checkDeploymentReachability(
	ctx context.Context,
	deploy *deploymentpb.Deployment,
	expectedReachability enumspb.DeploymentReachability,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().GetDeploymentReachability(ctx, &workflowservice.GetDeploymentReachabilityRequest{
			Namespace:  s.Namespace(),
			Deployment: deploy,
		})
		a.NoError(err)
		a.Equal(expectedReachability, resp.GetReachability())
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *DeploymentSuite) createDeploymentAndWaitForExist(
	deployment *deploymentpb.Deployment,
	tq *taskqueuepb.TaskQueue,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	// Start a deployment workflow
	go s.pollFromDeployment(ctx, tq, deployment)

	// Wait for the deployment to exist
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeDeployment(ctx, &workflowservice.DescribeDeploymentRequest{
			Namespace:  s.Namespace(),
			Deployment: deployment,
		})
		a.NoError(err)
		a.NotNil(resp.GetDeploymentInfo())
		a.NotNil(resp.GetDeploymentInfo().GetDeployment())
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedThenUnset() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	unpinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			Deployment: nil,
		},
	}

	// 1. Set unpinned override --> describe workflow shows the override
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())

	// 2. Unset using empty update opts with mutation mask --> describe workflow shows no more override
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), &workflowpb.WorkflowExecutionOptions{}))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedThenUnset() {
	ctx := context.Background()

	// presence of internally used delimiters (:) or escape
	// characters shouldn't break functionality
	seriesName := testcore.RandomizeStr("my-series|:|:")
	buildID := testcore.RandomizeStr("bgt:|")
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	pinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: workerDeployment,
		},
	}
	noOpts := &workflowpb.WorkflowExecutionOptions{}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(workerDeployment, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set pinned override on our new unversioned workflow --> describe workflow shows the override + deployment is reachable
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOpts.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// 2. Unset with empty update opts with mutation mask --> describe workflow shows no more override + deployment is unreachable
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), noOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
	s.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_EmptyFields() {
	ctx := context.Background()

	// presence of internally used delimiters (:) or escape
	// characters shouldn't break functionality
	seriesName := testcore.RandomizeStr("my-series|:|:")
	buildID := testcore.RandomizeStr("bgt:|")
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	pinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: workerDeployment,
		},
	}

	// 1. Pinned update with empty mask --> describe workflow shows no change
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), &workflowpb.WorkflowExecutionOptions{}))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetPinned() {
	ctx := context.Background()
	tv := testvars.New(s)
	tq := tv.TaskQueue()
	series := tv.DeploymentSeries()

	// start an unversioned workflow
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq.GetName()}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	deploymentA := &deploymentpb.Deployment{
		SeriesName: series,
		BuildId:    tv.BuildId("A"),
	}
	deploymentB := &deploymentpb.Deployment{
		SeriesName: series,
		BuildId:    tv.BuildId("B"),
	}
	pinnedOptsA := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentA,
		},
	}
	pinnedOptsB := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentB,
		},
	}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(deploymentA, tq)
	s.createDeploymentAndWaitForExist(deploymentB, tq)

	// 1. Set pinned override A --> describe workflow shows the override + deployment A is reachable
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
	s.checkDeploymentReachability(ctx, deploymentB, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// 3. Set pinned override B --> describe workflow shows the override + deployment B is reachable, A unreachable
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsB,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsB))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsB.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
	s.checkDeploymentReachability(ctx, deploymentB, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetUnpinned() {
	ctx := context.Background()
	tv := testvars.New(s)
	tq := tv.TaskQueue()

	// start an unversioned workflow
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq.GetName()}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	unpinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			Deployment: nil,
		},
	}

	// 1. Set unpinned override --> describe workflow shows the override
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())

	// 1. Set unpinned override --> describe workflow shows the override
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetPinned() {
	ctx := context.Background()
	tv := testvars.New(s)
	tq := tv.TaskQueue()
	series := tv.DeploymentSeries()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq.GetName()}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	unpinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			Deployment: nil,
		},
	}
	deploymentA := &deploymentpb.Deployment{
		SeriesName: series,
		BuildId:    tv.BuildId("A"),
	}
	pinnedOptsA := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentA,
		},
	}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set unpinned override --> describe workflow shows the override + deploymentA is unreachable
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// 1. Set pinned override A --> describe workflow shows the override + deploymentA is reachable
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (s *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetUnpinned() {
	ctx := context.Background()
	tv := testvars.New(s)
	tq := tv.TaskQueue()
	series := tv.DeploymentSeries()

	// start an unversioned workflow
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq.GetName()}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	unpinnedOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			Deployment: nil,
		},
	}
	deploymentA := &deploymentpb.Deployment{
		SeriesName: series,
		BuildId:    tv.BuildId("A"),
	}
	pinnedOptsA := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentA,
		},
	}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(deploymentA, tq)

	// 1. Set pinned override A --> describe workflow shows the override + deploymentA is reachable
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// 1. Set unpinned override --> describe workflow shows the override + deploymentA is unreachable
	updateResp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
}

func (s *DeploymentSuite) TestStartWorkflowExecution_WithPinnedOverride() {
	ctx := context.Background()
	deploymentA := &deploymentpb.Deployment{
		SeriesName: "seriesName",
		BuildId:    "A",
	}
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
		Deployment: deploymentA,
	}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	resp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          s.Namespace(),
		WorkflowId:         "test-workflow-id1",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id1",
		RequestId:          uuid.New(),
		VersioningOverride: override,
	})

	s.NoError(err)
	s.True(resp.GetStarted())
	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id1",
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (s *DeploymentSuite) TestStartWorkflowExecution_WithUnpinnedOverride() {
	ctx := context.Background()
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		Deployment: nil,
	}

	resp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          s.Namespace(),
		WorkflowId:         "test-workflow-id2",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id2",
		RequestId:          uuid.New(),
		VersioningOverride: override,
	})

	s.NoError(err)
	s.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id2",
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

func (s *DeploymentSuite) TestSignalWithStartWorkflowExecution_WithPinnedOverride() {
	ctx := context.Background()
	deploymentA := &deploymentpb.Deployment{
		SeriesName: "seriesName",
		BuildId:    "A",
	}
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
		Deployment: deploymentA,
	}

	// create deployment so that GetDeploymentReachability doesn't error
	s.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          s.Namespace(),
		WorkflowId:         "test-workflow-id3",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id3",
		RequestId:          uuid.New(),
		SignalName:         "test-signal3",
		SignalInput:        nil,
		VersioningOverride: override,
	})

	s.NoError(err)
	s.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id3",
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
	s.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (s *DeploymentSuite) TestSignalWithStartWorkflowExecution_WithUnpinnedOverride() {
	ctx := context.Background()
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		Deployment: nil,
	}

	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          s.Namespace(),
		WorkflowId:         "test-workflow-id4",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id4",
		RequestId:          uuid.New(),
		SignalName:         "test-signal4",
		SignalInput:        nil,
		VersioningOverride: override,
	})

	s.NoError(err)
	s.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id4",
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *DeploymentSuite) Name() string {
	fullName := s.T().Name()
	return fullName[len(fullName)-20:]
}
