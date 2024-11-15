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
	"github.com/docker/docker/daemon/logger"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	deployspb "go.temporal.io/server/api/deployment/v1"
)

type (
	// DeploymentWorkflowRunner holds the local state while running a deployment name workflow
	DeploymentNameWorkflowRunner struct {
		*deployspb.DeploymentNameWorkflowArgs
		ctx                         workflow.Context
		a                           *DeploymentNameActivities
		logger                      sdklog.Logger
		metrics                     sdkclient.MetricsHandler
		requestsBeforeContinueAsNew int
	}
)

const (
	// Updates
	UpdateDeploymentNameDefaultBuildIDName = "update-deployment-name-default-buildID"

	DeploymentNameWorkflowIDPrefix = "temporal-sys-deployment-name"
	RequestsBeforeContinueAsNew    = 500
)

// TODO Shivam - Define workflow for DeploymentName
func DeploymentNameWorkflow(ctx workflow.Context, deploymentNameArgs *deployspb.DeploymentNameWorkflowArgs) error {
	deploymentWorkflowNameRunner := &DeploymentNameWorkflowRunner{
		DeploymentNameWorkflowArgs:  deploymentNameArgs,
		ctx:                         ctx,
		a:                           nil,
		logger:                      sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentNameArgs.NamespaceName),
		metrics:                     workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentNameArgs.NamespaceName}),
		requestsBeforeContinueAsNew: RequestsBeforeContinueAsNew,
	}
	return deploymentWorkflowNameRunner.run()
}

func (d *DeploymentNameWorkflowRunner) run() error {
	var requestCount int
	var pendingUpdates int

	/* TODO Shivam:

	Implement this workflow to be an infinitely long running workflow

	Update handler(s) to:
		- signal/update/have an activity DeploymentWorkflow and update the buildID of all task-queues in the deployment name && update default buildID of the deployment name.

	*/
	err := workflow.SetQueryHandler(d.ctx, "DefaultBuildID", func(input []byte) (string, error) {
		return d.DefaultBuildId, nil
	})
	if err != nil {
		logger.Info("SetQueryHandler failed for DeploymentName workflow with error: " + err.Error())
		return err
	}

	// Updatehandler for updating default-buildID

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(d.ctx, func() bool { return requestCount >= d.requestsBeforeContinueAsNew && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(d.ctx, DeploymentNameWorkflow, d.DeploymentNameWorkflowArgs)
}
