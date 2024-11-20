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
	"context"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	deployspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
)

type (
	DeploymentActivities struct {
		activityDeps
		namespaceName namespace.Name
		namespaceID   namespace.ID
	}

	DeploymentSeriesWorkflowActivityInput struct {
		SeriesName string
	}
)

// StartDeploymentSeriesWorkflow activity starts a DeploymentSeries workflow
func (a *DeploymentActivities) StartDeploymentSeriesWorkflow(ctx context.Context, input DeploymentSeriesWorkflowActivityInput) error {
	logger := activity.GetLogger(ctx)
	logger.Info("activity to start DeploymentSeries workflow started")

	sdkClient := a.ClientFactory.NewClient(sdkclient.Options{
		Namespace:     a.namespaceName.String(),
		DataConverter: sdk.PreferProtoDataConverter,
	})

	workflowID := GenerateDeploymentSeriesWorkflowID(input.SeriesName)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: primitives.PerNSWorkerTaskQueue,
		Memo: map[string]interface{}{
			BuildIDMemoKey: "",
		},
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}

	// Build workflow args
	deploymentSeriesWorkflowArgs := &deployspb.DeploymentSeriesWorkflowArgs{
		NamespaceName: a.namespaceName.String(),
		NamespaceId:   a.namespaceID.String(),
	}

	// Calling the workflow with the args
	_, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, DeploymentSeriesWorkflow, deploymentSeriesWorkflowArgs)
	if err != nil {
		return err
	}

	return nil
}
