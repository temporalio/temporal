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
	"cmp"
	"context"

	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
)

type (
	DeploymentActivities struct {
		namespace        *namespace.Namespace
		deploymentClient DeploymentStoreClient
		matchingClient   resource.MatchingClient
	}
)

func (a *DeploymentActivities) StartDeploymentSeriesWorkflow(ctx context.Context, input *deploymentspb.StartDeploymentSeriesRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting deployment series workflow", "seriesName", input.SeriesName)
	identity := "deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.deploymentClient.StartDeploymentSeries(ctx, a.namespace, input.SeriesName, identity, input.RequestId)
}

func (a *DeploymentActivities) SyncUserData(ctx context.Context, input *deploymentspb.SyncUserDataRequest) error {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)
	for _, sync := range input.Sync {
		go func() {
			logger.Info("syncing task queue userdata for deployment", "taskQueue", sync.Name, "type", sync.Type)
			_, err := a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   a.namespace.ID().String(),
				TaskQueue:     sync.Name,
				TaskQueueType: sync.Type,
				Deployment:    input.Deployment,
				Data:          sync.Data,
			})
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", sync.Name, "type", sync.Type, "error", err)
			}
			errs <- err
		}()
	}

	var err error
	for range input.Sync {
		err = cmp.Or(err, <-errs)
	}

	// TODO: it might be nice if we check propagation status and not return from here until
	// it's propagated to all partitions

	return err
}
