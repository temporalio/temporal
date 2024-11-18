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
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	deployspb "go.temporal.io/server/api/deployment/v1"
)

type (
	// DeploymentWorkflowRunner holds the local state for a deployment workflow
	DeploymentWorkflowRunner struct {
		*deployspb.DeploymentWorkflowArgs
		ctx     workflow.Context
		a       *DeploymentActivities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler
		lock    workflow.Mutex
	}
)

const (
	// Updates
	RegisterWorkerInDeployment = "register-task-queue-worker"

	// Signals
	UpdateDeploymentBuildIDSignalName = "update-deployment-build-id"
	ForceCANSignalName                = "force-continue-as-new"

	DeploymentWorkflowIDPrefix      = "temporal-sys-deployment"
	DeploymentWorkflowIDDelimeter   = "|"
	DeploymentWorkflowIDInitialSize = (2 * len(DeploymentWorkflowIDDelimeter)) + len(DeploymentWorkflowIDPrefix)
)

type AwaitSignals struct {
	SignalsCompleted bool
}

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs *deployspb.DeploymentWorkflowArgs) error {
	deploymentWorkflowRunner := &DeploymentWorkflowRunner{
		DeploymentWorkflowArgs: deploymentWorkflowArgs,
		ctx:                    ctx,
		a:                      nil,
		logger:                 sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentWorkflowArgs.NamespaceName),
		metrics:                workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentWorkflowArgs.NamespaceName}),
		lock:                   workflow.NewMutex(ctx),
	}
	return deploymentWorkflowRunner.run()
}

func (a *AwaitSignals) ListenToSignals(ctx workflow.Context) {
	// Fetch signal channels
	updateBuildIDSignalChannel := workflow.GetSignalChannel(ctx, UpdateDeploymentBuildIDSignalName)
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	forceCAN := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(updateBuildIDSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
	})
	selector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
		forceCAN = true
	})

	for (!workflow.GetInfo(ctx).GetContinueAsNewSuggested() && !forceCAN) || selector.HasPending() {
		selector.Select(ctx)
	}

	// Done processing signals before CAN
	a.SignalsCompleted = true
}

func (d *DeploymentWorkflowRunner) run() error {
	var a AwaitSignals
	var pendingUpdates int

	// Set up Query Handlers here:
	err := workflow.SetQueryHandler(d.ctx, "deploymentTaskQueues", func(input []byte) (map[string]*deployspb.DeploymentLocalState_TaskQueueFamilyInfo, error) {
		return d.DeploymentLocalState.TaskQueueFamilies, nil
	})
	if err != nil {
		d.logger.Error("Failed while setting up query handler")
		return err
	}

	// Listen to signals in a different go-routine to make business logic clearer
	workflow.Go(d.ctx, a.ListenToSignals)

	// Setting an update handler for updating deployment task-queues
	if err := workflow.SetUpdateHandler(
		d.ctx,
		RegisterWorkerInDeployment,
		func(ctx workflow.Context, updateInput *deployspb.RegisterWorkerInDeploymentArgs) error {
			// need a lock here
			pendingUpdates++
			defer func() {
				pendingUpdates--
			}()

			// if no TaskQueueFamilies have been registered for the deployment
			if d.DeploymentLocalState.TaskQueueFamilies == nil {
				d.DeploymentLocalState.TaskQueueFamilies = make(map[string]*deployspb.DeploymentLocalState_TaskQueueFamilyInfo)
			}
			// if no TaskQueues have been registered for the TaskQueueName
			if d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues == nil {
				d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues = make(map[int32]*deployspb.DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo)
			}

			// idempotency check
			if _, ok := d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues[int32(updateInput.TaskQueueType)]; ok {
				return nil
			}

			// Add the task queue to the local state
			newTaskQueueWorkerInfo := &deployspb.DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo{
				TaskQueueType:   updateInput.TaskQueueType,
				FirstPollerTime: updateInput.FirstPollerTime,
			}
			d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues[int32(updateInput.TaskQueueType)] = newTaskQueueWorkerInfo

			// Call activity which starts "DeploymentName" workflow

			return nil
		},
		// TODO Shivam - have a validator which backsoff updates if we are scheduled to have a CAN
	); err != nil {
		return err
	}

	// Wait on any pending signals and updates.
	err = workflow.Await(d.ctx, func() bool { return pendingUpdates == 0 && a.SignalsCompleted })
	if err != nil {
		return err
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
		NamespaceName:        d.NamespaceName,
		NamespaceId:          d.NamespaceId,
		DeploymentLocalState: d.DeploymentLocalState,
	}
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, workflowArgs)

}
