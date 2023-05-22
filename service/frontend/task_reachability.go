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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
)

// Helper for deduping GetWorkerBuildIdCompatibility matching requests.
type versionSetFetcher struct {
	sync.Mutex
	matchingClient  matchingservice.MatchingServiceClient
	notificationChs map[string]chan struct{}
	responses       map[string]versionSetFetcherResponse
}

type versionSetFetcherResponse struct {
	value *matchingservice.GetWorkerBuildIdCompatibilityResponse
	err   error
}

func newVersionSetFetcher(matchingClient matchingservice.MatchingServiceClient) *versionSetFetcher {
	return &versionSetFetcher{
		matchingClient:  matchingClient,
		notificationChs: make(map[string]chan struct{}),
		responses:       make(map[string]versionSetFetcherResponse),
	}
}

func (f *versionSetFetcher) getStoredResponse(taskQueue string) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	f.Lock()
	defer f.Unlock()
	if response, found := f.responses[taskQueue]; found {
		return response.value, response.err
	}
	return nil, nil
}

func (f *versionSetFetcher) fetchTaskQueueVersions(ctx context.Context, ns *namespace.Namespace, taskQueue string) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	response, err := f.getStoredResponse(taskQueue)
	if err != nil || response != nil {
		return response, err
	}

	f.Lock()
	var waitCh chan struct{}
	if ch, found := f.notificationChs[taskQueue]; found {
		waitCh = ch
	} else {
		waitCh = make(chan struct{})
		f.notificationChs[taskQueue] = waitCh

		go func() {
			value, err := f.matchingClient.GetWorkerBuildIdCompatibility(ctx, &matchingservice.GetWorkerBuildIdCompatibilityRequest{
				NamespaceId: ns.ID().String(),
				Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
					Namespace: ns.Name().String(),
					TaskQueue: taskQueue,
				},
			})
			f.Lock()
			defer f.Unlock()
			f.responses[taskQueue] = versionSetFetcherResponse{value: value, err: err}
			close(waitCh)
		}()
	}
	f.Unlock()

	<-waitCh
	return f.getStoredResponse(taskQueue)
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
		if isDefaultInQueue {
			buildIdsFilter = "BuildIds IS NULL"
		} else {
			allKnownBuildIdsEscaped := []string{}
			for _, set := range request.versionSets {
				for _, buildId := range set.BuildIds {
					allKnownBuildIdsEscaped = append(allKnownBuildIdsEscaped, fmt.Sprintf("%q", buildId))
				}
			}
			// This is a versioned task queue, the unversioned worker should only process workflows that have tasks that
			// were completed by workers that are not defined in the versioning data.
			// This condition assumes that all of these workflows were processed by SDKs that have already been using
			// the BuildId field in WorkflowTaskCompleted events without opting in to versioning, which should
			// eventually cover all SDKs.
			// TODO: This doesn't cover a build id that was previously not used for versioning and later got defined in
			// the versioning data. Maybe it'd be better to mark every workflow that opts in to versioning.
			buildIdsFilter = fmt.Sprintf("BuildIds IS NOT NULL AND BuilIds NOT IN (%s)", strings.Join(allKnownBuildIdsEscaped, ","))
		}
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
		// Workflows that have just started and hane not yet been processed by workers are marked as:
		// Latest - BuildIds IS NULL.
		// Compatible - BuildIds = ["source" build id] where source is either a taken from a parent or a previous run in
		// a chain.
		buildIdsFilter = fmt.Sprintf("BuildIds IN (%s)", strings.Join(escapedBuildIds, ","))
		if isDefaultInQueue && request.reachabilityType != enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS {
			buildIdsFilter = fmt.Sprintf("(%s OR BuildIds IS NULL)", buildIdsFilter)
		}
	}

	if isDefaultInQueue {
		taskQueueReachability.Reachability = append(
			taskQueueReachability.Reachability,
			enumspb.TASK_REACHABILITY_NEW_WORKFLOWS,
		)
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
		statusFilter = " AND ExecutionStatus = \"Running\""
	case enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS:
		statusFilter = " AND ExecutionStatus != \"Running\""
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
		Query:       fmt.Sprintf("TaskQueue = %q AND %s%s", taskQueue, buildIdsFilter, statusFilter),
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
