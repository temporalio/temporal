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

package deployment

import (
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deployspb "go.temporal.io/server/api/deployment/v1"
)

type (
	DeploymentLocalState struct {
		*deployspb.DeploymentWorkflowArgs
		DeploymentName string
		BuildID        string
	}

	// DeploymentWorkflowRunner holds the local state for a deployment workflow
	DeploymentWorkflowRunner struct {
		*DeploymentLocalState
		ctx     workflow.Context
		a       *DeploymentActivities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler
	}
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		ScheduleToCloseTimeout: 1 * time.Hour,
		StartToCloseTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
			MaximumInterval: 60 * time.Second,
		},
	}
)

const (
	UpdateDeploymentSignalName        = "update_deployment"
	UpdateDeploymentBuildIDSignalName = "update_deployment_build_id"
	ForceCANSignalName                = "force-continue-as-new"

	DeploymentWorkflowIDPrefix = "temporal-sys-deployment:"
)

// parseDeploymentWorkflowID parses the workflowID, to extract DeploymentName and BuildID,
// for the execution of a Deployment workflow.
func parseDeploymentWorkflowID(workflowID string) (deploymentName string, buildID string, err error) {
	// Split by ":"
	parts := strings.Split(workflowID, ":")
	if len(parts) != 2 {
		return "", "", serviceerror.NewInvalidArgument("invalid format for workflowID")
	}

	deploymentBuildIDWorkflowID := strings.Split(parts[1], "-")
	if len(deploymentBuildIDWorkflowID) != 2 {
		return "", "", serviceerror.NewInvalidArgument("invalid format for workflowID")
	}

	// Length and character checks for deploymentName and buildID are performed in matching
	deploymentName = deploymentBuildIDWorkflowID[0]
	buildID = deploymentBuildIDWorkflowID[1]
	return deploymentName, buildID, nil
}

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs *deployspb.DeploymentWorkflowArgs) error {
	// Extract buildID and deploymentName from workflowID
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID

	deploymentName, buildID, err := parseDeploymentWorkflowID(workflowID)
	if err != nil {
		return err
	}

	deploymentWorkflowRunner := &DeploymentWorkflowRunner{
		DeploymentLocalState: &DeploymentLocalState{
			DeploymentWorkflowArgs: deploymentWorkflowArgs,
			DeploymentName:         deploymentName,
			BuildID:                buildID,
		},
		ctx:     ctx,
		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentWorkflowArgs.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentWorkflowArgs.NamespaceName}),
	}
	return deploymentWorkflowRunner.run()
}

func (d *DeploymentWorkflowRunner) run() error {

	// Set up Query Handlers here:
	err := workflow.SetQueryHandler(d.ctx, "deploymentTaskQueues", func(input []byte) (map[string]*deployspb.DeploymentWorkflowArgs_TaskQueueFamilyInfo, error) {
		return d.DeploymentLocalState.TaskQueueFamilies, nil
	})
	if err != nil {
		d.logger.Error("Failed while setting up query handler")
		return err
	}

	// Fetch signal channels
	updateDeploymentSignalChannel := workflow.GetSignalChannel(d.ctx, UpdateDeploymentSignalName)
	updateBuildIDSignalChannel := workflow.GetSignalChannel(d.ctx, UpdateDeploymentBuildIDSignalName)
	forceCANSignalChannel := workflow.GetSignalChannel(d.ctx, ForceCANSignalName)
	forceCAN := false

	selector := workflow.NewSelector(d.ctx)
	selector.AddReceive(updateDeploymentSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// fetch the input from the signal
		var signalInput *deployspb.UpdateDeploymentSignalInput
		updateDeploymentSignalChannel.Receive(d.ctx, &signalInput)

		if d.DeploymentLocalState.TaskQueueFamilies == nil {
			d.DeploymentLocalState.TaskQueueFamilies = make(map[string]*deployspb.DeploymentWorkflowArgs_TaskQueueFamilyInfo)
			d.DeploymentLocalState.TaskQueueFamilies[signalInput.Name] = &deployspb.DeploymentWorkflowArgs_TaskQueueFamilyInfo{}
		}
		// add the task queue to the local state
		d.DeploymentLocalState.TaskQueueFamilies[signalInput.Name].TaskQueues =
			append(d.DeploymentLocalState.TaskQueueFamilies[signalInput.Name].TaskQueues, signalInput.TaskQueueInfo)

		// Call activity which starts "DeploymentName" workflow
		activityInput := StartDeploymentNameWorkflowActivityInput{
			NamespaceName:  d.DeploymentLocalState.NamespaceName,
			NamespaceID:    d.DeploymentLocalState.NamespaceId,
			DeploymentName: d.DeploymentLocalState.DeploymentName,
		}
		activityCtx := workflow.WithActivityOptions(d.ctx, defaultActivityOptions)
		workflow.ExecuteActivity(activityCtx, d.a.StartDeploymentNameWorkflow, activityInput)
	})
	selector.AddReceive(updateBuildIDSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
	})
	selector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
		forceCAN = true
	})

	// async draining before CAN
	for (!workflow.GetInfo(d.ctx).GetContinueAsNewSuggested() && !forceCAN) || selector.HasPending() {
		selector.Select(d.ctx)
	}

	/*

		 	Posting this as a reminder to limit the number of signals coming through since we use CAN:

			Workflows cannot have infinitely-sized history and when the event count grows too large, `ContinueAsNew` can be returned
			to start a new one atomically. However, in order not to lose any data, signals must be drained and any other futures
			that need to be reacted to must be completed first. This means there must be a period where there are no signals to
			drain and no futures to wait on. If signals come in faster than processed or futures wait so long there is no idle
			period, `ContinueAsNew` will not happen in a timely manner and history will grow.

	*/

	d.logger.Debug("Deployment doing continue-as-new")
	workflowArgs := &deployspb.DeploymentWorkflowArgs{
		NamespaceName:     d.DeploymentLocalState.NamespaceName,
		NamespaceId:       d.DeploymentLocalState.NamespaceId,
		TaskQueueFamilies: d.DeploymentLocalState.TaskQueueFamilies,
	}
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, workflowArgs)

}
