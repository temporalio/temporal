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
	// DeploymentWorkflowRunner holds the local state while running a deployment-series workflow
	DeploymentSeriesWorkflowRunner struct {
		*deployspb.DeploymentSeriesWorkflowArgs
		ctx     workflow.Context
		a       *DeploymentSeriesActivities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler
	}
)

const (
	// Updates
	UpdateDeploymentSeriesDefaultBuildIDName = "update-deployment-name-default-buildID"
)

func DeploymentSeriesWorkflow(ctx workflow.Context, deploymentSeriesArgs *deployspb.DeploymentSeriesWorkflowArgs) error {
	deploymentWorkflowNameRunner := &DeploymentSeriesWorkflowRunner{
		DeploymentSeriesWorkflowArgs: deploymentSeriesArgs,
		ctx:                          ctx,
		a:                            nil,
		logger:                       sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentSeriesArgs.NamespaceName),
		metrics:                      workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentSeriesArgs.NamespaceName}),
	}
	return deploymentWorkflowNameRunner.run()
}

func (d *DeploymentSeriesWorkflowRunner) run() error {
	var pendingUpdates int

	err := workflow.SetQueryHandler(d.ctx, QueryCurrentDeployment, func(input []byte) (string, error) {
		return d.DefaultBuildId, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for DeploymentSeries workflow with error: " + err.Error())
		return err
	}

	// TODO Shivam (later) -  Updatehandler for updating default-buildID of a deployment.
	// This shall be responsible for:
	//  - an update operation on a deployment which shall update all the task-queue's default buildID and the local state of the current deployment

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(d.ctx, func() bool { return workflow.GetInfo(d.ctx).GetContinueAsNewSuggested() && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(d.ctx, DeploymentSeriesWorkflow, d.DeploymentSeriesWorkflowArgs)
}
