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

package workerdeployment

import (
	"cmp"
	"context"
	"sync"

	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
)

type (
	VersionActivities struct {
		namespace        *namespace.Namespace
		deploymentClient Client
		matchingClient   resource.MatchingClient
	}
)

func (a *VersionActivities) StartWorkerDeploymentWorkflow(
	ctx context.Context,
	input *deploymentspb.StartWorkerDeploymentRequest,
) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting deployment workflow", "deploymentName", input.DeploymentName)
	identity := "deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.deploymentClient.Start(ctx, a.namespace, input.DeploymentName, identity, input.RequestId)
}

func (a *VersionActivities) SyncWorkerDeploymentUserData(
	ctx context.Context,
	input *deploymentspb.SyncWorkerDeploymentUserDataRequest,
) (*deploymentspb.SyncWorkerDeploymentUserDataResponse, error) {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, e := range input.Sync {
		go func(syncData *deploymentspb.SyncWorkerDeploymentUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment", "taskQueue", syncData.Name, "type", syncData.Type)
			res, err := a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   a.namespace.ID().String(),
				TaskQueue:     syncData.Name,
				TaskQueueType: syncData.Type,
				// Deployment:    input.Deployment,
				// Data:          syncData.Data,
			})
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.Name, "type", syncData.Type, "error", err)
			} else {
				lock.Lock()
				maxVersionByName[syncData.Name] = max(maxVersionByName[syncData.Name], res.Version)
				lock.Unlock()
			}
			errs <- err
		}(e)
	}

	var err error
	for range input.Sync {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncWorkerDeploymentUserDataResponse{TaskQueueMaxVersions: maxVersionByName}, nil
}

func (a *VersionActivities) CheckWorkerDeploymentUserDataPropagation(ctx context.Context, input *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	for n, v := range input.TaskQueueMaxVersions {
		go func(name string, version int64) {
			logger.Info("waiting for userdata propagation", "taskQueue", name, "version", version)
			_, err := a.matchingClient.CheckTaskQueueUserDataPropagation(ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			})
			if err != nil {
				logger.Error("waiting for userdata", "taskQueue", name, "type", version, "error", err)
			}
			errs <- err
		}(n, v)
	}

	var err error
	for range input.TaskQueueMaxVersions {
		err = cmp.Or(err, <-errs)
	}
	return err
}
