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

	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
)

type (
	// DeploymentWorkflowRunner holds the local state for a deployment workflow
	DeploymentWorkflowRunner struct {
		*deploymentspb.DeploymentWorkflowArgs
		ctx              workflow.Context
		a                *DeploymentActivities
		logger           sdklog.Logger
		metrics          sdkclient.MetricsHandler
		lock             workflow.Mutex
		signalsCompleted bool
	}
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
			MaximumInterval: 60 * time.Second,
		},
	}
)

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs *deploymentspb.DeploymentWorkflowArgs) error {
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

func (d *DeploymentWorkflowRunner) listenToSignals(ctx workflow.Context) {
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
	d.signalsCompleted = true
}

func (d *DeploymentWorkflowRunner) run() error {
	var pendingUpdates int

	// Set up Query Handlers here:
	if err := workflow.SetQueryHandler(d.ctx, QueryDescribeDeployment, d.handleDescribeQuery); err != nil {
		d.logger.Error("Failed while setting up query handler")
		return err
	}

	// Setting an update handler for updating deployment task-queues
	if err := workflow.SetUpdateHandler(
		d.ctx,
		RegisterWorkerInDeployment,
		func(ctx workflow.Context, updateInput *deploymentspb.RegisterWorkerInDeploymentArgs) error {
			err := workflow.Await(ctx, func() bool { return d.DeploymentLocalState.StartedSeriesWorkflow })
			if err != nil {
				return err
			}

			err = d.lock.Lock(ctx)
			if err != nil {
				d.logger.Error("Could not acquire deployment workflow lock")
				return err
			}
			pendingUpdates++
			defer func() {
				pendingUpdates--
				d.lock.Unlock()
			}()

			// check if already present
			if _, ok := d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].GetTaskQueues()[int32(updateInput.TaskQueueType)]; ok {
				return nil
			}

			// if no TaskQueueFamilies have been registered for the deployment
			if d.DeploymentLocalState.TaskQueueFamilies == nil {
				d.DeploymentLocalState.TaskQueueFamilies = make(map[string]*deploymentspb.DeploymentLocalState_TaskQueueFamilyData)
			}
			if d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName] == nil {
				d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName] = &deploymentspb.DeploymentLocalState_TaskQueueFamilyData{}
			}
			// if no TaskQueues have been registered for the TaskQueueName
			if d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues == nil {
				d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues = make(map[int32]*deploymentspb.TaskQueueData)
			}

			// Add the task queue to the local state
			data := &deploymentspb.TaskQueueData{
				FirstPollerTime: updateInput.FirstPollerTime,
			}
			d.DeploymentLocalState.TaskQueueFamilies[updateInput.TaskQueueName].TaskQueues[int32(updateInput.TaskQueueType)] = data

			// denormalize LastBecameCurrentTime into per-tq data
			syncData := common.CloneProto(data)
			syncData.LastBecameCurrentTime = d.DeploymentLocalState.LastBecameCurrentTime

			activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
			err = workflow.ExecuteActivity(activityCtx, d.a.SyncUserData, &deploymentspb.SyncUserDataRequest{
				Deployment:    d.DeploymentLocalState.WorkerDeployment,
				TaskQueueName: updateInput.TaskQueueName,
				TaskQueueType: updateInput.TaskQueueType,
				Data:          syncData,
			}).Get(ctx, nil)
			return err
		},
		// TODO Shivam - have a validator which backsoff updates if we are scheduled to have a CAN
	); err != nil {
		return err
	}

	// First ensure series workflow is running
	if !d.DeploymentLocalState.StartedSeriesWorkflow {
		activityCtx := workflow.WithActivityOptions(d.ctx, defaultActivityOptions)
		err := workflow.ExecuteActivity(activityCtx, d.a.StartDeploymentSeriesWorkflow, &deploymentspb.StartDeploymentSeriesRequest{
			SeriesName: d.DeploymentLocalState.WorkerDeployment.SeriesName,
		}).Get(d.ctx, nil)
		if err != nil {
			return err
		}
		d.DeploymentLocalState.StartedSeriesWorkflow = true
	}

	// Listen to signals in a different go-routine to make business logic clearer
	workflow.Go(d.ctx, d.listenToSignals)

	// Wait on any pending signals and updates.
	err := workflow.Await(d.ctx, func() bool { return pendingUpdates == 0 && d.signalsCompleted })
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
	return workflow.NewContinueAsNewError(d.ctx, DeploymentWorkflow, d.DeploymentWorkflowArgs)

}

func (d *DeploymentWorkflowRunner) handleDescribeQuery() (*deploymentspb.QueryDescribeDeploymentResponse, error) {
	return &deploymentspb.QueryDescribeDeploymentResponse{
		DeploymentLocalState: d.DeploymentLocalState,
	}, nil
}

/*
// updateMemo should be called whenever the workflow updates it's local state: "is_current_deployment"
func (d *DeploymentWorkflowRunner) updateMemo() {
}
*/
