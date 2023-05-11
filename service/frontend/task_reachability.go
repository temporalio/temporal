package frontend

import (
	"context"
	"fmt"
	"strings"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

// Little helper to concurrently map a function over input and fail fast on error.
func raceMap[IN any, OUT any](input []IN, mapper func(IN) (OUT, error)) ([]OUT, error) {
	var group sync.WaitGroup
	group.Add(len(input))
	errorsCh := make(chan error, len(input))
	doneCh := make(chan OUT, len(input))
	results := make([]OUT, 0, len(input))

	defer func() {
		go func() {
			group.Wait()
			close(doneCh)
			close(errorsCh)
		}()
	}()

	for _, in := range input {
		in := in
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := mapper(in)
			if err != nil {
				errorsCh <- err
				return
			}
			doneCh <- result
		}()
	}
	for {
		select {
		case err := <-errorsCh:
			return nil, err
		case result := <-doneCh:
			results = append(results, result)
			if len(results) == len(input) {
				return results, nil
			}
		}
	}
}

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
	var buildIds []string

	if request.GetTaskQueue() != "" {
		var err error
		buildIds, err = wh.getAllTaskQueuePollerBuildIds(ctx, ns, request.GetTaskQueue())
		if err != nil {
			return nil, err
		}
		if len(buildIds) == 0 {
			return &workflowservice.GetWorkerTaskReachabilityResponse{}, nil
		}
	} else {
		buildIds = []string{request.GetBuildId()}
	}

	versionFetcher := newVersionSetFetcher(wh.matchingClient)

	buildIdReachability, err := raceMap(buildIds, func(buildId string) (*taskqueuepb.BuildIdReachability, error) {
		var taskQueues []string
		if request.GetScope().GetTaskQueue() != "" {
			taskQueues = []string{request.GetScope().GetTaskQueue()}
		} else {
			// Namespace scope implicitly assumed unless task queue is requested
			response, err := wh.matchingClient.GetBuildIdTaskQueueMapping(ctx, &matchingservice.GetBuildIdTaskQueueMappingRequest{
				NamespaceId: ns.ID().String(),
				BuildId:     buildId,
			})
			if err != nil {
				return nil, err
			}
			taskQueues = response.TaskQueues
		}

		return wh.getBuildIdReachability(ctx, buildIdReachabilityRequest{
			versionFetcher: versionFetcher,
			namespace:      ns,
			buildId:        buildId,
			taskQueues:     taskQueues,
		})
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerTaskReachabilityResponse{BuildIdReachability: buildIdReachability}, nil
}

// Extract the set of build ids used to identify all task queue pollers.
func (wh *WorkflowHandler) getAllTaskQueuePollerBuildIds(ctx context.Context, ns *namespace.Namespace, taskQueueName string) ([]string, error) {
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueTypes := []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY, enumspb.TASK_QUEUE_TYPE_WORKFLOW}

	describeResponses, err := raceMap(taskQueueTypes, func(taskQueueType enumspb.TaskQueueType) (*matchingservice.DescribeTaskQueueResponse, error) {
		return wh.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: ns.ID().String(),
			DescRequest: &workflowservice.DescribeTaskQueueRequest{
				Namespace:     ns.Name().String(),
				TaskQueue:     taskQueue,
				TaskQueueType: taskQueueType,
			},
		})
	})
	if err != nil {
		return nil, err
	}

	seenBuildIds := make(map[string]struct{}, 0)
	for _, response := range describeResponses {
		for _, poller := range response.Pollers {
			seenBuildIds[poller.GetWorkerVersionCapabilities().GetBuildId()] = struct{}{}
		}
	}
	buildIds := make([]string, 0, len(seenBuildIds))
	for buildId := range seenBuildIds {
		buildIds = append(buildIds, buildId)
	}
	return buildIds, nil
}

type buildIdReachabilityRequest struct {
	versionFetcher *versionSetFetcher
	namespace      *namespace.Namespace
	buildId        string
	taskQueues     []string
}

// Get the reachability of a single build ID in namespace or task queue scope.
// Expects a pre-popluated list of associated task queues.
func (wh *WorkflowHandler) getBuildIdReachability(
	ctx context.Context,
	request buildIdReachabilityRequest,
) (*taskqueuepb.BuildIdReachability, error) {
	buildIdReachability := taskqueuepb.BuildIdReachability{BuildId: request.buildId}
	if len(request.taskQueues) == 0 {
		return &buildIdReachability, nil
	}
	taskQueueReachability, err := raceMap(request.taskQueues, func(taskQueue string) (*taskqueuepb.TaskQueueReachability, error) {
		compatibilityResponse, err := request.versionFetcher.fetchTaskQueueVersions(ctx, request.namespace, taskQueue)
		if err != nil {
			return nil, err
		}
		return wh.getTaskQueueReachability(ctx, taskQueueReachabilityRequest{
			buildId:     request.buildId,
			namespace:   request.namespace,
			taskQueue:   taskQueue,
			versionSets: compatibilityResponse.Response.GetMajorVersionSets(),
		})
	})
	if err != nil {
		return nil, err
	}
	buildIdReachability.TaskQueueReachability = taskQueueReachability
	return &buildIdReachability, nil
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
	reachabilityResponses, err := raceMap(queries, func(query reachabilityQuery) (enumspb.TaskReachability, error) {
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
