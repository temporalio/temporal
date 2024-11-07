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

package versioning

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type (
	TaskQueue struct {
		Name          string
		TaskQueueType enumspb.TaskQueueType
	}
	Deployment struct {
		Namespace      string
		DeploymentName string
		TaskQueue      []*TaskQueue // All the task queues associated with this buildID/deployment
		BuildID        string
	}
	DeploymentName struct {
		Namespace      string
		Name           string
		CurrentBuildID string // denotes the current "default" build-ID of a deploymentName
	}

	// DeploymentWorkflowRunner holds the local state while running a deployment workflow
	DeploymentWorkflowRunner struct {
		deployment Deployment
		ctx        workflow.Context
		a          *activities
		logger     sdklog.Logger
		metrics    sdkclient.MetricsHandler
	}
	// DeploymentWorkflowRunner holds the local state while running a deployment name workflow
	DeploymentNameWorkflowRunner struct {
		deploymentName DeploymentName
		ctx            workflow.Context
		a              *activities
		logger         sdklog.Logger
		metrics        sdkclient.MetricsHandler
	}
	// DeploymentBuildIDArgs holds the arguments used while calling activities associated with a deployment
	DeploymentBuildIDArgs struct {
		deployment Deployment
		buildID    string // used for verifying/updating the buildID of all the task queues in a deployment
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
	UpdateDeploymentSignalChannelName = "update_deployment"
	updateBuildIDSignalChannelName    = "update_task_queue_build_id"
)

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs Deployment) error {
	deploymentWorkflowRunner := &DeploymentWorkflowRunner{
		deployment: deploymentWorkflowArgs,
		ctx:        ctx,
		logger:     sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentWorkflowArgs.Namespace),
		metrics:    workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentWorkflowArgs.Namespace}),
	}
	// TQ should be set to nil here since signal handler updates it
	deploymentWorkflowRunner.deployment.TaskQueue = nil
	return deploymentWorkflowRunner.run()
}

func (d *DeploymentWorkflowRunner) run() error {

	// Set up Query Handlers here:

	// Fetch signal channels
	updateDeploymentSignalChannel := workflow.GetSignalChannel(d.ctx, UpdateDeploymentSignalChannelName)
	updateBuildIDSignalChannel := workflow.GetSignalChannel(d.ctx, updateBuildIDSignalChannelName)

	selector := workflow.NewSelector(d.ctx)
	selector.AddReceive(updateDeploymentSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal

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
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, d.deployment)

}

// TODO Shivam - Define workflow for DeploymentName
func DeploymentNameWorkflow(ctx workflow.Context, deploymentNameArgs DeploymentName) error {
	deploymentWorkflowNameRunner := &DeploymentNameWorkflowRunner{
		deploymentName: deploymentNameArgs,
		ctx:            ctx,
		logger:         sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentNameArgs.Namespace),
		metrics:        workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentNameArgs.Namespace}),
	}
	// TQ should be set to nil here since signal handler updates it
	return deploymentWorkflowNameRunner.run()
}

func (d *DeploymentNameWorkflowRunner) run() error {
	/* TODO Shivam:

	Implement this workflow to be an infinitely long running workflow

	Query handlers to return current default buildID of the deployment name

	Signal handler(s) to:
	signal DeploymentWorkflow and update the buildID of all task-queues in the deployment name.
	update default buildID of the deployment name.

	*/
	return nil
}
