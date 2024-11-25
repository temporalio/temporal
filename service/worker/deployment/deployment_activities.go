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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
)

type (
	DeploymentActivities struct {
		namespaceName  namespace.Name
		namespaceID    namespace.ID
		sdkClient      sdkclient.Client
		matchingClient resource.MatchingClient
	}
)

// StartDeploymentSeriesWorkflow activity starts a DeploymentSeries workflow

func (a *DeploymentActivities) StartDeploymentSeriesWorkflow(ctx context.Context, input *deploymentspb.StartDeploymentSeriesRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting deployment series workflow", "seriesName", input.SeriesName)

	workflowID := GenerateDeploymentSeriesWorkflowID(input.SeriesName)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: primitives.PerNSWorkerTaskQueue,
		Memo: map[string]interface{}{
			DeploymentSeriesBuildIDMemoField: "",
		},
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}

	// Calling the workflow with the args
	_, err := a.sdkClient.ExecuteWorkflow(ctx, workflowOptions, DeploymentSeriesWorkflowType, &deploymentspb.DeploymentSeriesWorkflowArgs{
		NamespaceName: a.namespaceName.String(),
		NamespaceId:   a.namespaceID.String(),
	})
	if err != nil {
		logger.Error("starting deployment series workflow failed", "seriesName", input.SeriesName, "error", err)
	}
	return err
}

func (a *DeploymentActivities) SyncUserData(ctx context.Context, input *deploymentspb.SyncUserDataRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("syncing task queue userdata for deployment", "taskQueue", input.TaskQueueName, "type", input.TaskQueueType)

	_, err := a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:   a.namespaceID.String(),
		TaskQueue:     input.TaskQueueName,
		TaskQueueType: input.TaskQueueType,
		Deployment:    input.Deployment,
		Data:          input.Data,
	})
	if err != nil {
		logger.Error("syncing task queue userdata", "taskQueue", input.TaskQueueName, "type", input.TaskQueueType, "error", err)
	}

	// TODO: it might be nice if we check propagation status and not return from here until
	// it's propagated to all partitions

	return err
}
