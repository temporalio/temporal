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

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
)

// Little helper to concurrently map a function over input and fail fast on error.
func mapConcurrent[IN any, OUT any](input []IN, mapper func(IN) (OUT, error)) ([]OUT, error) {
	errorsCh := make(chan error, len(input))
	results := make([]OUT, len(input))

	for i, in := range input {
		i := i
		in := in
		go func() {
			var err error
			results[i], err = mapper(in)
			errorsCh <- err
		}()
	}
	for range input {
		if err := <-errorsCh; err != nil {
			return nil, err
		}
	}
	return results, nil
}

// Implementation of the GetWorkerTaskReachability API. Expects an already validated request.
func (wh *WorkflowHandler) getWorkerTaskReachabilityValidated(
	ctx context.Context,
	ns *namespace.Namespace,
	request *workflowservice.GetWorkerTaskReachabilityRequest,
) (*workflowservice.GetWorkerTaskReachabilityResponse, error) {

	var taskQueues []string
	if request.GetScope().GetTaskQueue() != "" {
		taskQueues = []string{request.GetScope().GetTaskQueue()}
	} else {
		// Namespace scope implicitly assumed unless task queue is requested
		response, err := wh.matchingClient.GetBuildIdTaskQueueMapping(ctx, &matchingservice.GetBuildIdTaskQueueMappingRequest{
			NamespaceId: ns.ID().String(),
			BuildId:     request.GetBuildId(),
		})
		if err != nil {
			return nil, err
		}
		taskQueues = response.TaskQueues
	}

	numTaskQueuesToQuery := util.Min(len(taskQueues), wh.config.ReachabilityTaskQueueScanLimit())
	taskQueuesToQuery, taskQueuesToSkip := taskQueues[:numTaskQueuesToQuery], taskQueues[numTaskQueuesToQuery:]

	taskQueueReachability, err := mapConcurrent(taskQueuesToQuery, func(taskQueue string) (*taskqueuepb.TaskQueueReachability, error) {
		compatibilityResponse, err := wh.matchingClient.GetWorkerBuildIdCompatibility(ctx, &matchingservice.GetWorkerBuildIdCompatibilityRequest{
			NamespaceId: ns.ID().String(),
			Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
				Namespace: ns.Name().String(),
				TaskQueue: taskQueue,
			},
		})
		if err != nil {
			return nil, err
		}
		return wh.getTaskQueueReachability(ctx, taskQueueReachabilityRequest{
			buildId:     request.GetBuildId(),
			namespace:   ns,
			taskQueue:   taskQueue,
			versionSets: compatibilityResponse.Response.GetMajorVersionSets(),
		})
	})
	if err != nil {
		return nil, err
	}
	for _, taskQueue := range taskQueuesToSkip {
		taskQueueReachability = append(taskQueueReachability, &taskqueuepb.TaskQueueReachability{TaskQueue: taskQueue, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}})
	}
	if err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerTaskReachabilityResponse{TaskQueueReachability: taskQueueReachability}, nil
}

type taskQueueReachabilityRequest struct {
	buildId     string
	taskQueue   string
	namespace   *namespace.Namespace
	versionSets []*taskqueuepb.CompatibleVersionSet
}

// Get the reachability of a single build ID in a single task queue scope.
func (wh *WorkflowHandler) getTaskQueueReachability(ctx context.Context, request taskQueueReachabilityRequest) (*taskqueuepb.TaskQueueReachability, error) {
	taskQueueReachability := taskqueuepb.TaskQueueReachability{TaskQueue: request.taskQueue, Reachability: []enumspb.TaskReachability{}}

	var isDefaultInQueue bool
	var buildIdsFilter string

	if request.buildId == "" {
		// Query for the unversioned worker
		isDefaultInQueue = len(request.versionSets) == 0
		buildIdsFilter = "BuildIds IS NULL"
	} else {
		// Query for a versioned worker
		setIdx := -1
		buildIdIdx := -1
		if len(request.versionSets) > 0 {
			for sidx, set := range request.versionSets {
				for bidx, id := range set.BuildIds {
					if request.buildId == id {
						setIdx = sidx
						buildIdIdx = bidx
						break
					}
				}
			}
		}

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
			escapedBuildIds[i] = fmt.Sprintf("%q", buildId)
		}
		buildIdsFilter = fmt.Sprintf("BuildIds IN (%s)", strings.Join(escapedBuildIds, ","))
		// TODO: To properly answer if there are open workflows that may be routed to a build id, we'll need to see if
		// any of them have not *yet* been assigned a build id, e.g. they've been started but have not had any completed
		// workflow tasks.
		// Since this is an API used to check which build ids can be retired, we'll take the stricter approach and
		// assume that `BuildIds = NULL` does not mean old unversioned workflows.
		if isDefaultInQueue {
			buildIdsFilter = fmt.Sprintf("(%s OR BuildIds IS NULL)", buildIdsFilter)
		}
	}

	if isDefaultInQueue {
		taskQueueReachability.Reachability = append(
			taskQueueReachability.Reachability,
			enumspb.TASK_REACHABILITY_NEW_WORKFLOWS,
		)
	}

	reachability, err := wh.queryVisibilityForExisitingWorkflowsReachability(ctx, request.namespace, request.taskQueue, buildIdsFilter)
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
) ([]enumspb.TaskReachability, error) {
	type reachabilityQuery struct {
		str              string
		taskReachability enumspb.TaskReachability
	}

	queries := []reachabilityQuery{
		{
			str:              fmt.Sprintf("TaskQueue = %q AND %s AND ExecutionStatus = \"Running\"", taskQueue, buildIdsFilter),
			taskReachability: enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS,
		},
		{
			str:              fmt.Sprintf("TaskQueue = %q AND %s AND ExecutionStatus != \"Running\"", taskQueue, buildIdsFilter),
			taskReachability: enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS,
		},
	}
	reachabilityResponses, err := mapConcurrent(queries, func(query reachabilityQuery) (enumspb.TaskReachability, error) {
		req := manager.CountWorkflowExecutionsRequest{
			NamespaceID: ns.ID(),
			Namespace:   ns.Name(),
			Query:       query.str,
		}

		// TODO: is count more efficient than select with page size of 1?
		countResponse, err := wh.visibilityMrg.CountWorkflowExecutions(ctx, &req)
		if err != nil {
			return enumspb.TASK_REACHABILITY_UNSPECIFIED, err
		} else if countResponse.Count == 0 {
			return enumspb.TASK_REACHABILITY_UNSPECIFIED, nil
		}
		return query.taskReachability, nil
	})
	if err != nil {
		return nil, err
	}

	var reachability []enumspb.TaskReachability
	for _, result := range reachabilityResponses {
		if result != enumspb.TASK_REACHABILITY_UNSPECIFIED {
			reachability = append(reachability, result)
		}
	}
	return reachability, nil
}
