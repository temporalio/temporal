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
	"fmt"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/worker_versioning"
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
	logger.Info("starting worker-deployment workflow", "deploymentName", input.DeploymentName)
	identity := "deployment-version workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.deploymentClient.StartWorkerDeployment(ctx, a.namespace, input.DeploymentName, identity, input.RequestId)
}

func (a *VersionActivities) SyncDeploymentVersionUserData(
	ctx context.Context,
	input *deploymentspb.SyncDeploymentVersionUserDataRequest,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, e := range input.Sync {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment version", "taskQueue", syncData.Name, "type", syncData.Type)

			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error

			if input.ForgetVersion {
				res, err = a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
					NamespaceId:   a.namespace.ID().String(),
					TaskQueue:     syncData.Name,
					TaskQueueType: syncData.Type,
					Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
						ForgetVersion: input.Version,
					},
				})
			} else {

				res, err = a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
					NamespaceId:   a.namespace.ID().String(),
					TaskQueue:     syncData.Name,
					TaskQueueType: syncData.Type,
					Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
						UpdateVersionData: syncData.Data,
					},
				})
			}

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
	return &deploymentspb.SyncDeploymentVersionUserDataResponse{TaskQueueMaxVersions: maxVersionByName}, nil
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

// CheckIfTaskQueuesHavePollers returns true if any of the given task queues has any pollers
func (a *VersionActivities) CheckIfTaskQueuesHavePollers(ctx context.Context, args *deploymentspb.CheckTaskQueuesHaveNoPollersActivityArgs) (bool, error) {
	for _, tq := range args.TaskQueues {

		res, err := a.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: a.namespace.ID().String(),
			DescRequest: &workflowservice.DescribeTaskQueueRequest{
				Namespace:      a.namespace.Name().String(),
				TaskQueue:      tq,
				ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:       &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{worker_versioning.WorkerDeploymentVersionToString(args.WorkerDeploymentVersion)}},
				ReportPollers:  true,
				TaskQueueType:  enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			},
		})
		if err != nil {
			return false, fmt.Errorf("error describing task queue with name %s: %s", tq.GetName(), err)
		}
		typesInfo := res.GetDescResponse().GetVersionsInfo()[worker_versioning.WorkerDeploymentVersionToString(args.WorkerDeploymentVersion)].GetTypesInfo()
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetPollers()) > 0 {
			return true, nil
		}
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].GetPollers()) > 0 {
			return true, nil
		}
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)].GetPollers()) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (a *VersionActivities) AddVersionToWorkerDeployment(ctx context.Context, input *deploymentspb.AddVersionToWorkerDeploymentRequest) (*deploymentspb.AddVersionToWorkerDeploymentResponse, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("adding version to worker-deployment", "deploymentName", input.DeploymentName, "version", input.Version)
	identity := "deployment-version workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.deploymentClient.AddVersionToWorkerDeployment(ctx, a.namespace, input.DeploymentName, input.Version, identity, input.RequestId)
}
