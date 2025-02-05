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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/resource"
	"sync"

	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	Activities struct {
		namespace        *namespace.Namespace
		deploymentClient Client
		matchingClient   resource.MatchingClient
	}
)

func (a *Activities) SyncWorkerDeploymentVersion(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	res, err := a.deploymentClient.SyncVersionWorkflowFromWorkerDeployment(
		ctx,
		a.namespace,
		args.DeploymentName,
		args.Version,
		args.UpdateArgs,
		identity,
		args.RequestId,
	)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncVersionStateActivityResult{
		VersionState: res.VersionState,
	}, nil
}

func (a *Activities) SyncUnversionedRamp(
	ctx context.Context,
	input *deploymentspb.SyncUnversionedRampActivityArgs,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)
	// Get all the task queues in the current version and put them into SyncUserData format
	currVersionInfo, err := a.deploymentClient.DescribeVersion(ctx, a.namespace, input.CurrentVersion)
	if err != nil {
		return nil, err
	}
	data := &deploymentspb.DeploymentVersionData{
		Version:           nil,
		RoutingUpdateTime: input.UpdateArgs.RoutingUpdateTime,
		RampingSinceTime:  input.UpdateArgs.RampingSinceTime,
		RampPercentage:    input.UpdateArgs.RampPercentage,
	}
	var taskQueueSyncs []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData
	for _, tqInfo := range currVersionInfo.GetTaskQueueInfos() {
		taskQueueSyncs = append(taskQueueSyncs, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name: tqInfo.GetName(),
			Type: tqInfo.GetType(),
			Data: data,
		})
	}

	// For each task queue, sync the unversioned ramp data
	errs := make(chan error)
	var lock sync.Mutex
	maxVersionByTQName := make(map[string]int64)
	for _, e := range taskQueueSyncs {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing unversioned ramp to task queue userdata", "taskQueue", syncData.Name, "type", syncData.Type)
			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error
			res, err = a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   a.namespace.ID().String(),
				TaskQueue:     syncData.Name,
				TaskQueueType: syncData.Type,
				Operation:     &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{UpdateVersionData: syncData.Data},
			})
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.Name, "type", syncData.Type, "error", err)
			} else {
				lock.Lock()
				maxVersionByTQName[syncData.Name] = max(maxVersionByTQName[syncData.Name], res.Version)
				lock.Unlock()
			}
			errs <- err
		}(e)
	}
	for range taskQueueSyncs {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncDeploymentVersionUserDataResponse{TaskQueueMaxVersions: maxVersionByTQName}, nil
}

func (a *Activities) CheckUnversionedRampUserDataPropagation(ctx context.Context, input *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	for n, v := range input.TaskQueueMaxVersions {
		go func(name string, version int64) {
			logger.Info("waiting for unversioned ramp userdata propagation", "taskQueue", name, "version", version)
			_, err := a.matchingClient.CheckTaskQueueUserDataPropagation(ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			})
			if err != nil {
				logger.Error("waiting for unversioned ramp userdata propagation", "taskQueue", name, "type", version, "error", err)
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

func (a *Activities) IsVersionMissingTaskQueues(ctx context.Context, args *deploymentspb.IsVersionMissingTaskQueuesArgs) (*deploymentspb.IsVersionMissingTaskQueuesResult, error) {
	res, err := a.deploymentClient.IsVersionMissingTaskQueues(
		ctx,
		a.namespace,
		args.PrevCurrentVersion,
		args.NewCurrentVersion,
	)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.IsVersionMissingTaskQueuesResult{
		IsMissingTaskQueues: res,
	}, nil
}

func (a *Activities) DeleteWorkerDeploymentVersion(ctx context.Context, args *deploymentspb.DeleteVersionActivityArgs) error {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	err := a.deploymentClient.DeleteVersionFromWorkerDeployment(
		ctx,
		a.namespace,
		args.DeploymentName,
		args.Version,
		identity,
		args.RequestId,
	)
	if err != nil {
		return err
	}
	return nil
}
