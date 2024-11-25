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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
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
}

func (d *DeploymentSuite) TearDownTest() {
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
