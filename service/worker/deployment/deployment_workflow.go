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
	"errors"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	TaskQueue struct {
		Name          string
		TaskQueueType enumspb.TaskQueueType
	}
	// DeploymentTaskQueue holds relevant information for a task-queue present in a Deployment
	DeploymentTaskQueue struct {
		FirstPollerTimeStamp *timestamppb.Timestamp
		TaskQueue            *TaskQueue
	}

	// DeploymentWorkflowArgs represents the arguments passed for a DeploymentWorkflow
	DeploymentWorkflowArgs struct {
		NamespaceName string
		NamespaceID   string
		TaskQueues    []*DeploymentTaskQueue // required for CAN
	}

	DeploymentLocalState struct {
		NamespaceName  string
		NamespaceID    string
		DeploymentName string
		BuildID        string
		TaskQueues     []*DeploymentTaskQueue // All the task queues associated with this buildID/deployment
	}

	// DeploymentWorkflowRunner holds the local state for a deployment workflow
	DeploymentWorkflowRunner struct {
		ctx                  workflow.Context
		a                    *DeploymentActivities
		logger               sdklog.Logger
		metrics              sdkclient.MetricsHandler
		deploymentlocalState *DeploymentLocalState
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

	DeploymentWorkflowIDPrefix = "temporal-sys-deployment:"
)

// parseDeploymentWorkflowID parses the workflowID, to extract DeploymentName and BuildID,
// for the execution of a Deployment workflow.
func parseDeploymentWorkflowID(workflowID string) (string, string, error) {
	// Split by ":"
	parts := strings.Split(workflowID, ":")
	if len(parts) != 2 {
		return "", "", errors.New("invalid format for workflowID")
	}

	deploymentBuildIDWorkflowID := strings.Split(parts[1], "-")
	if len(deploymentBuildIDWorkflowID) != 2 {
		return "", "", errors.New("invalid format for workflowID")
	}

	deploymentName := deploymentBuildIDWorkflowID[0]
	buildID := deploymentBuildIDWorkflowID[1]
	return deploymentName, buildID, nil
}

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs DeploymentWorkflowArgs) error {
	// Extract buildID and deploymentName from workflowID
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID

	deploymentName, buildID, err := parseDeploymentWorkflowID(workflowID)
	if err != nil {
		return err
	}

	deploymentWorkflowRunner := &DeploymentWorkflowRunner{
		deploymentlocalState: &DeploymentLocalState{
			NamespaceName:  deploymentWorkflowArgs.NamespaceName,
			NamespaceID:    deploymentWorkflowArgs.NamespaceID,
			DeploymentName: deploymentName,
			BuildID:        buildID,
		},
		ctx:     ctx,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentWorkflowArgs.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentWorkflowArgs.NamespaceName}),
	}
	return deploymentWorkflowRunner.run()
}

func (d *DeploymentWorkflowRunner) run() error {

	// Set up Query Handlers here:

	// Fetch signal channels
	updateDeploymentSignalChannel := workflow.GetSignalChannel(d.ctx, UpdateDeploymentSignalName)
	updateBuildIDSignalChannel := workflow.GetSignalChannel(d.ctx, UpdateDeploymentBuildIDSignalName)

	selector := workflow.NewSelector(d.ctx)
	selector.AddReceive(updateDeploymentSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// fetch the input from the signal
		var inputDeploymentTaskQueue *DeploymentTaskQueue
		updateDeploymentSignalChannel.Receive(d.ctx, &inputDeploymentTaskQueue)

		// add the task queue to the local state
		d.deploymentlocalState.TaskQueues = append(d.deploymentlocalState.TaskQueues, inputDeploymentTaskQueue)

		// Call activity which starts "DeploymentName" workflow

	})
	selector.AddReceive(updateBuildIDSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
	})

	// async draining before CAN
	for !workflow.GetInfo(d.ctx).GetContinueAsNewSuggested() || selector.HasPending() {
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
	workflowArgs := DeploymentWorkflowArgs{
		NamespaceName: d.deploymentlocalState.NamespaceName,
		NamespaceID:   d.deploymentlocalState.NamespaceID,
		TaskQueues:    d.deploymentlocalState.TaskQueues,
	}
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, workflowArgs)

}
