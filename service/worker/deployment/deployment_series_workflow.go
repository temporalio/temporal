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
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
)

type (
	// DeploymentWorkflowRunner holds the local state while running a deployment-series workflow
	DeploymentSeriesWorkflowRunner struct {
		*deploymentspb.DeploymentSeriesWorkflowArgs
		a              *DeploymentSeriesActivities
		logger         sdklog.Logger
		metrics        sdkclient.MetricsHandler
		lock           workflow.Mutex
		pendingUpdates int
	}
)

func DeploymentSeriesWorkflow(ctx workflow.Context, deploymentSeriesArgs *deploymentspb.DeploymentSeriesWorkflowArgs) error {
	deploymentWorkflowNameRunner := &DeploymentSeriesWorkflowRunner{
		DeploymentSeriesWorkflowArgs: deploymentSeriesArgs,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentSeriesArgs.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentSeriesArgs.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}
	return deploymentWorkflowNameRunner.run(ctx)
}

func (d *DeploymentSeriesWorkflowRunner) run(ctx workflow.Context) error {
	var pendingUpdates int

	err := workflow.SetQueryHandler(ctx, QueryCurrentDeployment, func() (string, error) {
		return d.State.CurrentBuildId, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for DeploymentSeries workflow with error: " + err.Error())
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SetCurrentDeployment,
		d.handleSetCurrent,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSetCurrent,
		},
	); err != nil {
		return err
	}

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(ctx, func() bool { return workflow.GetInfo(ctx).GetContinueAsNewSuggested() && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(ctx, DeploymentSeriesWorkflow, d.DeploymentSeriesWorkflowArgs)
}

func (d *DeploymentSeriesWorkflowRunner) validateSetCurrent(args *deploymentspb.SetCurrentDeploymentArgs) error {
	return nil
}

func (d *DeploymentSeriesWorkflowRunner) handleSetCurrent(ctx workflow.Context, args *deploymentspb.SetCurrentDeploymentArgs) (*deploymentspb.SetCurrentDeploymentResponse, error) {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return nil, serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

	// TODO: See if a request_id is needed for idempotency

	// FIXME: Should lock the series while making the change

	// FIXME: Should update the current deployment in the series entity wf and well as updating the status of the target deployment.

	// FIXME: Also update the status of the previous current deployment to NO_STATUS

	// Update local state
	d.State.CurrentBuildId = args.BuildId
	// FIXME: d.updateMemo()

	return nil, nil
}
