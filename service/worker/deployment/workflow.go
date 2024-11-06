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

const continueAsNewThreshold = 1000

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

	// Query Handler
	err := workflow.SetQueryHandler(d.ctx, "list-task-queues", func(input []byte) ([]*TaskQueue, error) {
		return d.deployment.TaskQueue, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for list-task-queue with the error: " + err.Error())
		return err
	}

	updateDeploymentSignalChannel := workflow.GetSignalChannel(d.ctx, "update_deployment")
	updateBuildIDSignalChannel := workflow.GetSignalChannel(d.ctx, "update_task_queue_build_id")
	var signalCount int

	selector := workflow.NewSelector(d.ctx)
	selector.AddReceive(updateDeploymentSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		signalCount++
		var signalTaskQueue TaskQueue
		updateDeploymentSignalChannel.Receive(d.ctx, &signalTaskQueue)
		d.logger.Info("Received Signal to update Deployment!", "signal", signalTaskQueue)

		// Process Signal
		d.deployment.TaskQueue = append(d.deployment.TaskQueue, &signalTaskQueue)

		// TODO Shivam: Start an entity workflow for DeploymentName:

	})
	selector.AddReceive(updateBuildIDSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		signalCount++
		// Process Signal

		var new_buildID string
		updateBuildIDSignalChannel.Receive(d.ctx, &new_buildID)

		ctx := workflow.WithActivityOptions(d.ctx, defaultActivityOptions)
		var updatedTaskQueues []*TaskQueue
		err := workflow.ExecuteActivity(ctx, d.a.VerifyTaskQueueDefaultBuildID, d.deployment, d.deployment.BuildID).Get(d.ctx, &updatedTaskQueues)
		if err != nil {
			d.logger.Error("Task Queue default buildID verification activity failed with error : %s", err.Error())
			return
		}
		// update taskQueue list for the deployment as some task queues might have moved to a different buildID
		d.deployment.TaskQueue = updatedTaskQueues

		// Updating buildID of the task queues in consideration
		var res bool
		err = workflow.ExecuteActivity(ctx, d.a.UpdateTaskQueueDefaultBuildID, d.deployment, new_buildID).Get(d.ctx, &res)
		if err != nil {
			d.logger.Error("Task Queue update buildID activity failed with error : %s", err.Error())
			return
		}

	})

	// async draining before CAN
	for signalCount < continueAsNewThreshold || selector.HasPending() {
		selector.Select(d.ctx)
	}

	/*

		 	Posting this as a reminder to limit the number of signals coming through since we use CAN:

			Workflows cannot have infinitely-sized history and when the event count grows too large, `ContinueAsNew` can be returned
			to start a new one atomically. However, in order not to lose any data, signals must be drained and any other futures
			that need to be reacted to must be completed first. This means there must be a period where there are no signals to
			drain and no futures to wait on. If signals come in faster than processed or futures wait so long there is no idle
			period, `ContinueAsNew` will not happen in a timely manner and history will grow.

			Since this sample is a long-running workflow, once the request count reaches a certain size, we perform a
			`ContinueAsNew`. To not lose any data, we only send this if there are no in-flight signal requests or executing
			activities. An executing activity can mean it is busy retrying. Care must be taken designing these systems where they do
			not receive requests so frequent that they can never have a idle period to return a `ContinueAsNew`. Signals are usually
			fast to receive, so they are less of a problem. Waiting on activities (as a response to the request or as a response
			callback activity) can be a tougher problem. Since we rely on the response of the activity, activity must complete
			including all retries. Retry policies of these activities should be set balancing the resiliency needs with the need to
			have a period of idleness at some point.


	*/

	d.logger.Debug("Deployment doing continue-as-new")
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, d.deployment)

}
