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
)

type (
	DeploymentNameWorkflowArgs struct {
		NamespaceName  string
		NamespaceID    string
		DefaultBuildID string
	}

	// DeploymentWorkflowRunner holds the local state while running a deployment name workflow
	DeploymentNameWorkflowRunner struct {
		ctx     workflow.Context
		a       *DeploymentNameActivities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler
		// local state denoting the current "default" build-ID of a deploymentName (can be nil)
		DefaultBuildID string
	}
)

const (
	UpdateDeploymentNameBuildIDSignalName = "update-deployment-name-buildID"

	DeploymentNameWorkflowIDPrefix = "temporal-sys-deployment-name:"
)

// TODO Shivam - Define workflow for DeploymentName
func DeploymentNameWorkflow(ctx workflow.Context, deploymentNameArgs DeploymentNameWorkflowArgs) error {
	deploymentWorkflowNameRunner := &DeploymentNameWorkflowRunner{
		ctx:            ctx,
		logger:         sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentNameArgs.NamespaceName),
		metrics:        workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentNameArgs.NamespaceName}),
		DefaultBuildID: "", // TODO Shivam - extract buildID from the workflowID
	}
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
