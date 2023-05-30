// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package frontend

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/xwb1989/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
)

// Helper for deduping GetWorkerBuildIdCompatibility matching requests.
type versionSetFetcher struct {
	lock           sync.Mutex
	matchingClient matchingservice.MatchingServiceClient
	futures        map[string]future.Future[*matchingservice.GetWorkerBuildIdCompatibilityResponse]
}

func newVersionSetFetcher(matchingClient matchingservice.MatchingServiceClient) *versionSetFetcher {
	return &versionSetFetcher{
		matchingClient: matchingClient,
		futures:        make(map[string]future.Future[*matchingservice.GetWorkerBuildIdCompatibilityResponse]),
	}
}

func (f *versionSetFetcher) getFuture(ctx context.Context, ns *namespace.Namespace, taskQueue string) future.Future[*matchingservice.GetWorkerBuildIdCompatibilityResponse] {
	f.lock.Lock()
	defer f.lock.Unlock()
	_, found := f.futures[taskQueue]
	if !found {
		fut := future.NewFuture[*matchingservice.GetWorkerBuildIdCompatibilityResponse]()
		f.futures[taskQueue] = fut
		go func() {
			value, err := f.matchingClient.GetWorkerBuildIdCompatibility(ctx, &matchingservice.GetWorkerBuildIdCompatibilityRequest{
				NamespaceId: ns.ID().String(),
				Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
					Namespace: ns.Name().String(),
					TaskQueue: taskQueue,
				},
			})
			fut.Set(value, err)
		}()
	}
	return f.futures[taskQueue]
}

func (f *versionSetFetcher) fetchTaskQueueVersions(ctx context.Context, ns *namespace.Namespace, taskQueue string) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	return f.getFuture(ctx, ns, taskQueue).Get(ctx)
}

// Implementation of the GetWorkerTaskReachability API. Expects an already validated request.
func (wh *WorkflowHandler) getWorkerTaskReachabilityValidated(
	ctx context.Context,
	ns *namespace.Namespace,
	request *workflowservice.GetWorkerTaskReachabilityRequest,
) (*workflowservice.GetWorkerTaskReachabilityResponse, error) {
	vsf := newVersionSetFetcher(wh.matchingClient)

	reachability, err := util.MapConcurrent(request.GetBuildIds(), func(buildId string) (*taskqueuepb.BuildIdReachability, error) {
		return wh.getBuildIdReachability(ctx, buildIdReachabilityRequest{
			namespace:         ns,
			buildId:           buildId,
			taskQueues:        request.GetTaskQueues(),
			versionSetFetcher: vsf,
			reachabilityType:  request.Reachability,
		})
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerTaskReachabilityResponse{BuildIdReachability: reachability}, nil
}

type buildIdReachabilityRequest struct {
	namespace         *namespace.Namespace
	buildId           string
	taskQueues        []string
	versionSetFetcher *versionSetFetcher
	reachabilityType  enumspb.TaskReachability
}

func (wh *WorkflowHandler) getBuildIdReachability(
	ctx context.Context,
	request buildIdReachabilityRequest,
) (*taskqueuepb.BuildIdReachability, error) {
	taskQueues := request.taskQueues
	if len(taskQueues) == 0 {
		// Namespace scope, fetch mapping from DB.
		response, err := wh.matchingClient.GetBuildIdTaskQueueMapping(ctx, &matchingservice.GetBuildIdTaskQueueMappingRequest{
			NamespaceId: request.namespace.ID().String(),
			BuildId:     request.buildId,
		})
		if err != nil {
			return nil, err
		}
		taskQueues = response.TaskQueues
	}

	numTaskQueuesToQuery := util.Min(len(taskQueues), wh.config.ReachabilityTaskQueueScanLimit())
	taskQueuesToQuery, taskQueuesToSkip := taskQueues[:numTaskQueuesToQuery], taskQueues[numTaskQueuesToQuery:]

	taskQueueReachability, err := util.MapConcurrent(taskQueuesToQuery, func(taskQueue string) (*taskqueuepb.TaskQueueReachability, error) {
		compatibilityResponse, err := request.versionSetFetcher.fetchTaskQueueVersions(ctx, request.namespace, taskQueue)
		if err != nil {
			return nil, err
		}
		return wh.getTaskQueueReachability(ctx, taskQueueReachabilityRequest{
			buildId:          request.buildId,
			namespace:        request.namespace,
			taskQueue:        taskQueue,
			versionSets:      compatibilityResponse.GetResponse().GetMajorVersionSets(),
			reachabilityType: request.reachabilityType,
		})
	})
	if err != nil {
		return nil, err
	}

	if len(taskQueuesToSkip) > 0 {
		skippedTasksReachability := make([]*taskqueuepb.TaskQueueReachability, len(taskQueuesToSkip))
		for i, taskQueue := range taskQueuesToSkip {
			skippedTasksReachability[i] = &taskqueuepb.TaskQueueReachability{TaskQueue: taskQueue, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}}
		}
		taskQueueReachability = append(taskQueueReachability, skippedTasksReachability...)
	}
	return &taskqueuepb.BuildIdReachability{BuildId: request.buildId, TaskQueueReachability: taskQueueReachability}, nil
}

type taskQueueReachabilityRequest struct {
	buildId          string
	taskQueue        string
	namespace        *namespace.Namespace
	versionSets      []*taskqueuepb.CompatibleVersionSet
	reachabilityType enumspb.TaskReachability
}

// Get the reachability of a single build ID in a single task queue scope.
func (wh *WorkflowHandler) getTaskQueueReachability(ctx context.Context, request taskQueueReachabilityRequest) (*taskqueuepb.TaskQueueReachability, error) {
	taskQueueReachability := taskqueuepb.TaskQueueReachability{TaskQueue: request.taskQueue, Reachability: []enumspb.TaskReachability{}}

	var isDefaultInQueue bool
	var buildIdsFilter string

	if request.buildId == "" {
		// Query for the unversioned worker
		isDefaultInQueue = len(request.versionSets) == 0
		// Query workflows that have completed tasks marked with a sentinel "unversioned" search attribute.
		buildIdsFilter = fmt.Sprintf(`%s = "%s"`, searchattribute.BuildIds, common.UnversionedSearchAttribute)
	} else {
		// Query for a versioned worker
		setIdx, buildIdIdx := common.FindBuildId(request.versionSets, request.buildId)
		if setIdx == -1 {
			// build id not in set - unreachable
			return &taskQueueReachability, nil
		}
		set := request.versionSets[setIdx]
		isDefaultInSet := buildIdIdx == len(set.BuildIds)-1

		if !isDefaultInSet {
			// unreachable
			return &taskQueueReachability, nil
		}

		isDefaultInQueue = setIdx == len(request.versionSets)-1
		escapedBuildIds := make([]string, len(set.GetBuildIds()))
		for i, buildId := range set.GetBuildIds() {
			escapedBuildIds[i] = sqlparser.String(sqlparser.NewStrVal([]byte(common.VersionedBuildIdSearchAttribute(buildId))))
		}
		buildIdsFilter = fmt.Sprintf("%s IN (%s)", searchattribute.BuildIds, strings.Join(escapedBuildIds, ","))
	}

	if isDefaultInQueue {
		taskQueueReachability.Reachability = append(
			taskQueueReachability.Reachability,
			enumspb.TASK_REACHABILITY_NEW_WORKFLOWS,
		)
		// Take into account started workflows that have not yet been processed by any worker.
		if request.reachabilityType != enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS {
			buildIdsFilter = fmt.Sprintf("(%s IS NULL OR %s)", searchattribute.BuildIds, buildIdsFilter)
		}
	}

	reachability, err := wh.queryVisibilityForExisitingWorkflowsReachability(ctx, request.namespace, request.taskQueue, buildIdsFilter, request.reachabilityType)
	if err != nil {
		return nil, err
	}
	taskQueueReachability.Reachability = append(taskQueueReachability.Reachability, reachability...)
	return &taskQueueReachability, nil
}

func (wh *WorkflowHandler) queryVisibilityForExisitingWorkflowsReachability(
	ctx context.Context,
	ns *namespace.Namespace,
	taskQueue,
	buildIdsFilter string,
	reachabilityType enumspb.TaskReachability,
) ([]enumspb.TaskReachability, error) {
	statusFilter := ""
	switch reachabilityType {
	case enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS:
		statusFilter = fmt.Sprintf(` AND %s = "Running"`, searchattribute.ExecutionStatus)
	case enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS:
		statusFilter = fmt.Sprintf(` AND %s != "Running"`, searchattribute.ExecutionStatus)
	case enumspb.TASK_REACHABILITY_UNSPECIFIED:
		reachabilityType = enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS
		statusFilter = ""
	case enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS:
		statusFilter = ""
	case enumspb.TASK_REACHABILITY_NEW_WORKFLOWS:
		return nil, nil
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unsupported reachability type: %v", reachabilityType))
	}

	req := manager.CountWorkflowExecutionsRequest{
		NamespaceID: ns.ID(),
		Namespace:   ns.Name(),
		Query:       fmt.Sprintf("%s = %q AND %s%s", searchattribute.TaskQueue, taskQueue, buildIdsFilter, statusFilter),
	}

	// TODO(bergundy): is count more efficient than select with page size of 1?
	countResponse, err := wh.visibilityMrg.CountWorkflowExecutions(ctx, &req)
	if err != nil {
		return nil, err
	} else if countResponse.Count == 0 {
		return nil, nil
	}
	return []enumspb.TaskReachability{reachabilityType}, nil
}
