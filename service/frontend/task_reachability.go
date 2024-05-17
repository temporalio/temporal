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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
)

// Helper for deduping GetWorkerBuildIdCompatibility matching requests.
type versionSetFetcher struct {
	lock           sync.Mutex
	matchingClient matchingservice.MatchingServiceClient
	futures        map[string]future.Future[*persistencespb.VersioningData]
}

func newVersionSetFetcher(matchingClient matchingservice.MatchingServiceClient) *versionSetFetcher {
	return &versionSetFetcher{
		matchingClient: matchingClient,
		futures:        make(map[string]future.Future[*persistencespb.VersioningData]),
	}
}

func (f *versionSetFetcher) getFuture(ctx context.Context, ns *namespace.Namespace, taskQueue string) future.Future[*persistencespb.VersioningData] {
	f.lock.Lock()
	defer f.lock.Unlock()
	_, found := f.futures[taskQueue]
	if !found {
		fut := future.NewFuture[*persistencespb.VersioningData]()
		f.futures[taskQueue] = fut
		go func() {
			value, err := f.matchingClient.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
				NamespaceId:   ns.ID().String(),
				TaskQueue:     taskQueue,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			})
			fut.Set(value.GetUserData().GetData().GetVersioningData(), err)
		}()
	}
	return f.futures[taskQueue]
}

func (f *versionSetFetcher) fetchTaskQueueVersions(ctx context.Context, ns *namespace.Namespace, taskQueue string) (*persistencespb.VersioningData, error) {
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

	numTaskQueuesToQuery := min(len(taskQueues), wh.config.ReachabilityTaskQueueScanLimit())
	taskQueuesToQuery, taskQueuesToSkip := taskQueues[:numTaskQueuesToQuery], taskQueues[numTaskQueuesToQuery:]

	taskQueueReachability, err := util.MapConcurrent(taskQueuesToQuery, func(taskQueue string) (*taskqueuepb.TaskQueueReachability, error) {
		versioningData, err := request.versionSetFetcher.fetchTaskQueueVersions(ctx, request.namespace, taskQueue)
		if err != nil {
			return nil, err
		}
		return wh.getTaskQueueReachability(ctx, taskQueueReachabilityRequest{
			buildId:          request.buildId,
			namespace:        request.namespace,
			taskQueue:        taskQueue,
			versioningData:   versioningData,
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
	versioningData   *persistencespb.VersioningData
	reachabilityType enumspb.TaskReachability
}

// Get the reachability of a single build ID in a single task queue scope.
func (wh *WorkflowHandler) getTaskQueueReachability(ctx context.Context, request taskQueueReachabilityRequest) (*taskqueuepb.TaskQueueReachability, error) {
	taskQueueReachability := taskqueuepb.TaskQueueReachability{TaskQueue: request.taskQueue, Reachability: []enumspb.TaskReachability{}}

	var isDefaultInQueue bool
	var reachableByNewWorkflows bool
	var buildIdsFilter string
	versionSets := request.versioningData.GetVersionSets()

	if request.buildId == "" { // Query for the unversioned worker
		isDefaultInQueue = len(versionSets) == 0
		if isDefaultInQueue {
			reachableByNewWorkflows = true
		} else {
			// If the queue became versioned just recently, consider the unversioned build ID reachable.
			queueBecameVersionedAt := util.FoldSlice(versionSets, &hlc.Clock{WallClock: math.MaxInt64}, func(c *hlc.Clock, set *persistencespb.CompatibleVersionSet) *hlc.Clock {
				return hlc.Min(c, set.BecameDefaultTimestamp)
			})
			reachableByNewWorkflows = time.Since(hlc.UTC(queueBecameVersionedAt)) < wh.config.ReachabilityQuerySetDurationSinceDefault()
		}

		// Query workflows that have completed tasks marked with a sentinel "unversioned" search attribute.
		buildIdsFilter = fmt.Sprintf(`%s = "%s"`, searchattribute.BuildIds, worker_versioning.UnversionedSearchAttribute)
	} else { // Query for a versioned worker
		setIdx, buildIdIdx := worker_versioning.FindBuildId(request.versioningData, request.buildId)
		if setIdx == -1 {
			// build ID not in set - unreachable
			return &taskQueueReachability, nil
		}
		set := versionSets[setIdx]
		if set.BuildIds[buildIdIdx].State == persistencespb.STATE_DELETED {
			// build ID not in set anymore - unreachable
			return &taskQueueReachability, nil
		}
		isDefaultInSet := buildIdIdx == len(set.BuildIds)-1

		if !isDefaultInSet {
			// unreachable
			return &taskQueueReachability, nil
		}

		isDefaultInQueue = setIdx == len(versionSets)-1

		// Allow some propagation delay of the versioning data.
		reachableByNewWorkflows = isDefaultInQueue || time.Since(hlc.UTC(set.BecameDefaultTimestamp)) < wh.config.ReachabilityQuerySetDurationSinceDefault()

		var escapedBuildIds []string
		for _, buildId := range set.GetBuildIds() {
			if buildId.State == persistencespb.STATE_ACTIVE {
				escapedBuildIds = append(escapedBuildIds, sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.VersionedBuildIdSearchAttribute(buildId.Id)))))
			}
		}
		buildIdsFilter = fmt.Sprintf("%s IN (%s)", searchattribute.BuildIds, strings.Join(escapedBuildIds, ","))
	}

	if reachableByNewWorkflows {
		taskQueueReachability.Reachability = append(
			taskQueueReachability.Reachability,
			enumspb.TASK_REACHABILITY_NEW_WORKFLOWS,
		)
	}
	if isDefaultInQueue {
		// Take into account started workflows that have not yet been processed by any worker.
		if request.reachabilityType != enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS {
			buildIdsFilter = fmt.Sprintf("(%s IS NULL OR %s)", searchattribute.BuildIds, buildIdsFilter)
		}
	}

	reachability, err := wh.queryVisibilityForExistingWorkflowsReachability(ctx, request.namespace, request.taskQueue, buildIdsFilter, request.reachabilityType)
	if err != nil {
		return nil, err
	}
	taskQueueReachability.Reachability = append(taskQueueReachability.Reachability, reachability...)
	return &taskQueueReachability, nil
}

func (wh *WorkflowHandler) queryVisibilityForExistingWorkflowsReachability(
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

	escapedTaskQueue := sqlparser.String(sqlparser.NewStrVal([]byte(taskQueue)))

	req := manager.CountWorkflowExecutionsRequest{
		NamespaceID: ns.ID(),
		Namespace:   ns.Name(),
		Query:       fmt.Sprintf("%s = %s AND %s%s", searchattribute.TaskQueue, escapedTaskQueue, buildIdsFilter, statusFilter),
	}

	// TODO(bergundy): is count more efficient than select with page size of 1?
	countResponse, err := wh.visibilityMgr.CountWorkflowExecutions(ctx, &req)
	if err != nil {
		return nil, err
	} else if countResponse.Count == 0 {
		return nil, nil
	}
	return []enumspb.TaskReachability{reachabilityType}, nil
}
