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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/worker/deployment"
	"go.temporal.io/server/tests/testcore"
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

func (d *DeploymentSuite) setAssertions() {
	d.Assertions = require.New(d.T())
}

func TestDeploymentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DeploymentSuite))
}

func (d *DeploymentSuite) SetupSuite() {
	d.setAssertions()
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
	d.SetDynamicConfigOverrides(dynamicConfigOverrides)
	d.FunctionalTestBase.SetupSuite("testdata/es_cluster.yaml")
}

func (d *DeploymentSuite) TearDownSuite() {
	d.FunctionalTestBase.TearDownSuite()
}

func (d *DeploymentSuite) SetupTest() {
	d.FunctionalTestBase.SetupTest()
	d.setAssertions()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  d.FrontendGRPCAddress(),
		Namespace: d.Namespace(),
	})
	if err != nil {
		d.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	d.sdkClient = sdkClient
}

func (d *DeploymentSuite) TearDownTest() {
	if d.sdkClient != nil {
		d.sdkClient.Close()
	}
}

// startDeploymentWorkflows calls PollWorkflowTaskQueue to start deployment related workflows
func (d *DeploymentSuite) startDeploymentWorkflows(ctx context.Context, taskQueue *taskqueuepb.TaskQueue,
	deployment *deploymentpb.Deployment, errChan chan error) {
	_, err := d.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: d.Namespace(),
		TaskQueue: taskQueue,
		Identity:  "random",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			UseVersioning:        true,
			BuildId:              deployment.BuildId,
			DeploymentSeriesName: deployment.SeriesName,
		},
	})
	select {
	case <-ctx.Done():
		return
	case errChan <- err:
	}
}

func (d *DeploymentSuite) TestDescribeDeployment_RegisterTaskQueue() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

	errChan := make(chan error)
	defer close(errChan)

	// Starting a deployment workflow
	go func() {
		d.startDeploymentWorkflows(ctx, taskQueue, workerDeployment, errChan)
	}()

	// Querying the Deployment
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := d.FrontendClient().DescribeDeployment(ctx, &workflowservice.DescribeDeploymentRequest{
			Namespace:  d.Namespace(),
			Deployment: workerDeployment,
		})
		a.NoError(err)
		a.NotNil(resp.DeploymentInfo)
		a.NotNil(resp.DeploymentInfo.Deployment)

		a.Equal(seriesName, resp.DeploymentInfo.Deployment.SeriesName)
		a.Equal(buildID, resp.DeploymentInfo.Deployment.BuildId)

		a.Equal(numberOfDeployments, len(resp.DeploymentInfo.TaskQueueInfos))
		if len(resp.DeploymentInfo.TaskQueueInfos) < numberOfDeployments {
			return
		}
		a.Equal(taskQueue.Name, resp.DeploymentInfo.TaskQueueInfos[0].Name)
		a.Equal(false, resp.DeploymentInfo.IsCurrent)
		// todo (Shivam) - please add a check for current time
	}, time.Second*5, time.Millisecond*200)

	// todo (Shivam) - cancel if pollers are still awake
	<-ctx.Done()
	select {
	case err := <-errChan:
		d.Fail("Expected error channel to be empty but got error %w", err)
	default:
	}
}

func (d *DeploymentSuite) TestGetCurrentDeployment_NoCurrentDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	seriesName := testcore.RandomizeStr("my-series")
	buildID := testcore.RandomizeStr("bgt")
	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workerDeployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	errChan := make(chan error)
	defer close(errChan)

	workflowID := deployment.GenerateDeploymentSeriesWorkflowID(seriesName)
	query := fmt.Sprintf("WorkflowId = '%s'", workflowID)
	notFoundErr := fmt.Sprintf("workflow not found for ID: %s", workflowID)

	// GetCurrentDeployment on a non-existing series returns an error
	resp, err := d.FrontendClient().GetCurrentDeployment(ctx, &workflowservice.GetCurrentDeploymentRequest{
		Namespace:  d.Namespace(),
		SeriesName: seriesName,
	})
	d.Error(err)
	d.Equal(err.Error(), notFoundErr)
	d.Nil(resp)

	// Starting a deployment workflow
	go func() {
		d.startDeploymentWorkflows(ctx, taskQueue, workerDeployment, errChan)
	}()

	// Verify the existence of a deployment series
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := d.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: d.Namespace(),
			Query:     query,
		})
		a.NoError(err)
		a.Equal(int64(1), resp.GetCount())
	}, time.Second*5, time.Millisecond*200)

	// Fetch series workflow's current deployment - will be nil since we haven't set it
	resp, err = d.FrontendClient().GetCurrentDeployment(ctx, &workflowservice.GetCurrentDeploymentRequest{
		Namespace:  d.Namespace(),
		SeriesName: seriesName,
	})
	d.NoError(err)
	d.Nil(resp.GetCurrentDeploymentInfo())

	// todo (Shivam) - cancel if pollers are still awake
	<-ctx.Done()
	select {
	case err := <-errChan:
		d.Fail("Expected error channel to be empty but got error %w", err)
	default:
	}
}

// addDeploymentsAndVerifyListDeployments does the following:
// verifyDeploymentListInfo checks the equality between two DeploymentListInfo objects
func (d *DeploymentSuite) verifyDeploymentListInfo(expectedDeploymentListInfo *deploymentpb.DeploymentListInfo, receivedDeploymentListInfo *deploymentpb.DeploymentListInfo) bool {
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
func (d *DeploymentSuite) verifyDeployments(ctx context.Context, request *workflowservice.ListDeploymentsRequest,
	expectedDeployments []*deploymentpb.DeploymentListInfo) {

	// list deployment call
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := d.FrontendClient().ListDeployments(ctx, request)
		a.NoError(err)
		a.NotNil(resp)
		if resp == nil {
			return
		}
		a.NotNil(resp.Deployments)

		// check to stop eventuallyWithT from panicking since
		// it collects all possible errors
		if len(resp.Deployments) < 1 {
			return
		}

		for _, expectedDeploymentListInfo := range expectedDeployments {

			deploymentListInfoValidated := false
			for _, receivedDeploymentListInfo := range resp.Deployments {

				deploymentListInfoValidated = deploymentListInfoValidated ||
					d.verifyDeploymentListInfo(expectedDeploymentListInfo, receivedDeploymentListInfo)
			}
			a.True(deploymentListInfoValidated)
		}
	}, time.Second*5, time.Millisecond*200)
}

// startlistAndValidateDeployments does the following:
// - starts deployment workflow(s)
// - calls verifyDeployments which lists + validates Deployments
func (d *DeploymentSuite) startlistAndValidateDeployments(deploymentInfo []*deploymentpb.DeploymentListInfo, seriesFilter string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	taskQueue := &taskqueuepb.TaskQueue{Name: "deployment-test", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	errChan := make(chan error)
	defer close(errChan)

	// Start deployment workflow(s)
	for _, listInfo := range deploymentInfo {
		go func() {
			d.startDeploymentWorkflows(ctx, taskQueue, listInfo.Deployment, errChan)
		}()
	}

	var expectedDeployments []*deploymentpb.DeploymentListInfo
	request := &workflowservice.ListDeploymentsRequest{
		Namespace: d.Namespace(),
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

	d.verifyDeployments(ctx, request, expectedDeployments)

	<-ctx.Done()
	select {
	case err := <-errChan:
		d.Fail("Expected error channel to be empty but got error %w", err)
	default:
	}
}

func (d *DeploymentSuite) buildDeploymentInfo(numberOfDeployments int) []*deploymentpb.DeploymentListInfo {
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

func (d *DeploymentSuite) TestListDeployments_VerifySingleDeployment() {
	deploymentInfo := d.buildDeploymentInfo(1)
	d.startlistAndValidateDeployments(deploymentInfo, "")
}

func (d *DeploymentSuite) TestListDeployments_MultipleDeployments() {
	deploymentInfo := d.buildDeploymentInfo(5)
	d.startlistAndValidateDeployments(deploymentInfo, "")
}

func (d *DeploymentSuite) TestListDeployments_MultipleDeployments_WithSeriesFilter() {
	deploymentInfo := d.buildDeploymentInfo(2)
	seriesFilter := deploymentInfo[0].Deployment.SeriesName
	d.startlistAndValidateDeployments(deploymentInfo, seriesFilter)
}

// TODO Shivam - refactor the above test cases TestListDeployments_WithSeriesNameFilter + TestListDeployments_WithoutSeriesNameFilter
// Refactoring should be done in a way where we are validating the exact deployment (based on how many we create) - right now,
// the tests do validate the read API logic but are not the most assertive

// TODO Shivam - Add more getCurrentDeployment tests when SetCurrentDefaultBuildID API has been defined

func (d *DeploymentSuite) TestGetDeploymentReachability_OverrideUnversioned() {
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
	errChan := make(chan error)
	defer close(errChan)

	// Start a deployment workflow
	go func() {
		d.startDeploymentWorkflows(ctx, taskQueue, workerDeployment, errChan)
	}()

	// Wait for the deployment to exist
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := d.FrontendClient().DescribeDeployment(ctx, &workflowservice.DescribeDeploymentRequest{
			Namespace:  d.Namespace(),
			Deployment: workerDeployment,
		})
		a.NoError(err)
		a.NotNil(resp.DeploymentInfo)
		a.NotNil(resp.DeploymentInfo.Deployment)
	}, time.Second*5, time.Millisecond*200)
	<-ctx.Done()
	select {
	case err := <-errChan:
		d.Fail("Expected error channel to be empty but got error %w", err)
	default:
	}

	// non-current deployment is unreachable
	d.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// start an unversioned workflow, set pinned deployment override --> deployment should be reachable
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: updateOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), updateOpts))

	// describe workflow and check that the versioning info has the override
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, updateOpts.GetVersioningOverride())
	// check that the deployment is now reachable, since an open workflow is using it via override
	d.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// TODO (carly): once sdk allows starting a deployment worker, start worker, complete workflow, and check for CLOSED_ONLY
	// TODO (carly): once SetCurrentDeployment is ready, check that a current deployment is reachable even with no workflows
	// TODO (carly): test starting a workflow execution on a current deployment, then getting reachability with no override
	// TODO (carly): check cache times (do I need to do this in functional when I have cache time tests in unit?)
}

func (d *DeploymentSuite) checkDescribeWorkflowAfterOverride(
	ctx context.Context,
	wf *commonpb.WorkflowExecution,
	expectedOverride *workflowpb.VersioningOverride,
) {
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := d.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: d.Namespace(),
			Execution: wf,
		})
		a.NoError(err)
		a.NotNil(resp)
		a.NotNil(resp.GetWorkflowExecutionInfo())
		a.NotNil(resp.GetWorkflowExecutionInfo().GetVersioningInfo())
		a.True(proto.Equal(expectedOverride, resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride()))
	}, 5*time.Second, 50*time.Millisecond)
}

func (d *DeploymentSuite) checkDeploymentReachability(
	ctx context.Context,
	deployment *deploymentpb.Deployment,
	expectedReachability enumspb.DeploymentReachability,
) {
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := d.FrontendClient().GetDeploymentReachability(ctx, &workflowservice.GetDeploymentReachabilityRequest{
			Namespace:  d.Namespace(),
			Deployment: deployment,
		})
		a.NoError(err)
		a.Equal(expectedReachability, resp.GetReachability())
	}, 5*time.Second, 50*time.Millisecond)
}

func (d *DeploymentSuite) createDeploymentAndWaitForExist(
	deployment *deploymentpb.Deployment,
	tq *taskqueuepb.TaskQueue,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	errChan := make(chan error)
	defer close(errChan)
	// Start a deployment workflow
	go func() {
		d.startDeploymentWorkflows(ctx, tq, deployment, errChan)
	}()

	// Wait for the deployment to exist
	d.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := d.FrontendClient().DescribeDeployment(ctx, &workflowservice.DescribeDeploymentRequest{
			Namespace:  d.Namespace(),
			Deployment: deployment,
		})
		a.NoError(err)
		a.NotNil(resp.DeploymentInfo)
		a.NotNil(resp.DeploymentInfo.Deployment)
	}, time.Second*5, time.Millisecond*200)
	<-ctx.Done()
	select {
	case err := <-errChan:
		d.Fail("Expected error channel to be empty but got error %w", err)
	default:
	}
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedThenUnset() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())

	// 2. Unset using empty update opts with mutation mask --> describe workflow shows no more override
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), &workflowpb.WorkflowExecutionOptions{}))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedThenUnset() {
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
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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

	// create deployment so that GetDeploymentReachability doesn't error
	d.createDeploymentAndWaitForExist(workerDeployment, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set pinned override on our new unversioned workflow --> describe workflow shows the override + deployment is reachable
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOpts.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// 2. Unset with empty update opts with mutation mask --> describe workflow shows no more override + deployment is unreachable
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
	d.checkDeploymentReachability(ctx, workerDeployment, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_EmptyFields() {
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
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), &workflowpb.WorkflowExecutionOptions{}))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, nil)
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetPinned() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	deploymentA := &deploymentpb.Deployment{
		SeriesName: "seriesName",
		BuildId:    "A",
	}
	deploymentB := &deploymentpb.Deployment{
		SeriesName: "seriesName",
		BuildId:    "B",
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
	d.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})
	d.createDeploymentAndWaitForExist(deploymentB, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set pinned override A --> describe workflow shows the override + deployment A is reachable
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
	d.checkDeploymentReachability(ctx, deploymentB, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// 3. Set pinned override B --> describe workflow shows the override + deployment B is reachable, A unreachable
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsB,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsB))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsB.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
	d.checkDeploymentReachability(ctx, deploymentB, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetUnpinned() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())

	// 1. Set unpinned override --> describe workflow shows the override
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetPinned() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
		SeriesName: "seriesName",
		BuildId:    "A",
	}
	pinnedOptsA := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentA,
		},
	}

	// create deployment so that GetDeploymentReachability doesn't error
	d.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set unpinned override --> describe workflow shows the override + deploymentA is unreachable
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)

	// 1. Set pinned override A --> describe workflow shows the override + deploymentA is reachable
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (d *DeploymentSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetUnpinned() {
	ctx := context.Background()

	// start an unversioned workflow
	unversionedTQ := "unversioned-test-tq"
	run, err := d.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	d.NoError(err)
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
		SeriesName: "seriesName",
		BuildId:    "A",
	}
	pinnedOptsA := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: deploymentA,
		},
	}

	// create deployment so that GetDeploymentReachability doesn't error
	d.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: unversionedTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	// 1. Set pinned override A --> describe workflow shows the override + deploymentA is reachable
	updateResp, err := d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: pinnedOptsA,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), pinnedOptsA))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, pinnedOptsA.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)

	// 1. Set unpinned override --> describe workflow shows the override + deploymentA is unreachable
	updateResp, err = d.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                d.Namespace(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: unpinnedOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	d.NoError(err)
	d.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), unpinnedOpts))
	d.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, unpinnedOpts.GetVersioningOverride())
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE)
}

func (d *DeploymentSuite) TestStartWorkflowExecution_WithPinnedOverride() {
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
	d.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	resp, err := d.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          d.Namespace(),
		WorkflowId:         "test-workflow-id1",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id1",
		RequestId:          "test-request-id1",
		VersioningOverride: override,
	})

	d.NoError(err)
	d.True(resp.GetStarted())
	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id1",
		RunId:      resp.GetRunId(),
	}
	d.checkDescribeWorkflowAfterOverride(ctx, wf, override)
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (d *DeploymentSuite) TestStartWorkflowExecution_WithUnpinnedOverride() {
	ctx := context.Background()
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		Deployment: nil,
	}

	resp, err := d.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          d.Namespace(),
		WorkflowId:         "test-workflow-id2",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id2",
		RequestId:          "test-request-id2",
		VersioningOverride: override,
	})

	d.NoError(err)
	d.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id2",
		RunId:      resp.GetRunId(),
	}
	d.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

func (d *DeploymentSuite) TestSignalWithStartWorkflowExecution_WithPinnedOverride() {
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
	d.createDeploymentAndWaitForExist(deploymentA, &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL})

	resp, err := d.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          d.Namespace(),
		WorkflowId:         "test-workflow-id3",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id3",
		RequestId:          "test-request-id3",
		SignalName:         "test-signal3",
		SignalInput:        nil,
		VersioningOverride: override,
	})

	d.NoError(err)
	d.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id3",
		RunId:      resp.GetRunId(),
	}
	d.checkDescribeWorkflowAfterOverride(ctx, wf, override)
	d.checkDeploymentReachability(ctx, deploymentA, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE)
}

func (d *DeploymentSuite) TestSignalWithStartWorkflowExecution_WithUnpinnedOverride() {
	ctx := context.Background()
	override := &workflowpb.VersioningOverride{
		Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		Deployment: nil,
	}

	resp, err := d.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          d.Namespace(),
		WorkflowId:         "test-workflow-id4",
		WorkflowType:       &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:           "test-id4",
		RequestId:          "test-request-id4",
		SignalName:         "test-signal4",
		SignalInput:        nil,
		VersioningOverride: override,
	})

	d.NoError(err)
	d.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-id4",
		RunId:      resp.GetRunId(),
	}
	d.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}
