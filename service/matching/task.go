package matching

import (
	"context"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	// genericTaskInfo contains the info for an activity or workflow task
	genericTaskInfo struct {
		*persistencespb.AllocatedTaskInfo
		completionFunc func(*internalTask, taskResponse)
	}
	// queryTaskInfo contains the info for a query task
	queryTaskInfo struct {
		taskID  string
		request *matchingservice.QueryWorkflowRequest
	}
	// nexusTaskInfo contains the info for a nexus task
	nexusTaskInfo struct {
		taskID            string
		deadline          time.Time
		operationDeadline time.Time
		request           *matchingservice.DispatchNexusTaskRequest
	}
	// startedTaskInfo contains info for any task received from
	// another matching host. This type of task is already marked as started
	startedTaskInfo struct {
		workflowTaskInfo *matchingservice.PollWorkflowTaskQueueResponse
		activityTaskInfo *matchingservice.PollActivityTaskQueueResponse
		nexusTaskInfo    *matchingservice.PollNexusTaskQueueResponse
	}
	// internalTask represents an activity, workflow, query or started (received from another host).
	// this struct is more like a union and only one of [ query, event, forwarded ] is
	// non-nil for any given task
	// TODO(pri): after deprecating classic matcher, we can consolidate backlogCountHint, recycleToken,
	// and removeFromMatcher into a single *physicalTaskQueueManager field.
	internalTask struct {
		event            *genericTaskInfo // non-nil for activity or workflow task that's locally generated
		query            *queryTaskInfo   // non-nil for a query task that's locally sync matched
		nexus            *nexusTaskInfo   // non-nil for a nexus task that's locally sync matched
		started          *startedTaskInfo // non-nil for a task received from a parent partition which is already started
		namespace        namespace.Name
		source           enumsspb.TaskSource
		responseC        chan taskResponse // non-nil only where there is a caller waiting for response (sync match)
		backlogCountHint func() int64
		// forwardInfo contains information about forward source partition and versioning decisions made by it
		// a parent partition receiving forwarded tasks makes no versioning decisions and only follows what the source
		// partition instructed.
		forwardInfo *taskqueuespb.TaskForwardInfo
		// redirectInfo is only set when redirect rule is applied on the task. for forwarded tasks, this is populated
		// based on forwardInfo.
		redirectInfo *taskqueuespb.BuildIdRedirectInfo
		// pollerScalingDecision is assigned when the queue has advice to give to the poller about whether
		// it should adjust its poller count
		pollerScalingDecision *taskqueuepb.PollerScalingDecision
		recycleToken          func(*internalTask)
		removeFromMatcher     atomic.Pointer[func()]

		targetWorkerDeploymentVersion *deploymentspb.WorkerDeploymentVersion

		// These fields are for use by matcherData:
		waitableMatchResult
		forwardCtx context.Context // non-nil for sync match task only
		// effectivePriority is initialized from an explicit task priority if present, or the
		// default for the task queue. It can also be the special pollForwarderPriority (higher
		// than normal priorities) to indicate the poll forwarder. In some other cases (e.g.
		// migration) it may be adjusted from the explicit task priority.
		effectivePriority priorityKey
		// taskDispatchRevisionNumber represents the revision number used by the task and is max(taskDirectiveRevisionNumber, routingConfigRevisionNumber) for the task.
		taskDispatchRevisionNumber int64
	}

	// taskResponse is used to report the result of either a match with a local poller,
	// or forwarding a task, query, or nexus task.
	taskResponse struct {
		// If forwarded is true, then forwardRes and forwardErr have the result of forwarding.
		// If it's false, then startErr has the result of RecordTaskStarted.
		forwarded  bool
		forwardRes any // note this may be a non-nil "any" containing a nil pointer
		forwardErr error
		startErr   error
	}
)

var (
	// sentinel values for task.removeFromMatcher
	removeFuncNotAddedYet = func() {}
	removeFuncEvicted     = func() {}
)

func (res taskResponse) err() error {
	if res.forwarded {
		return res.forwardErr
	}
	return res.startErr
}

func newInternalTaskForSyncMatch(
	info *persistencespb.TaskInfo,
	forwardInfo *taskqueuespb.TaskForwardInfo,
	taskDispatchRevisionNumber int64,
	targetVersion *deploymentspb.WorkerDeploymentVersion,
) *internalTask {
	var redirectInfo *taskqueuespb.BuildIdRedirectInfo
	// if this task is not forwarded, source can only be history
	source := enumsspb.TASK_SOURCE_HISTORY
	if forwardInfo != nil {
		// if task is forwarded, it may be history or backlog. setting based on forward info
		source = forwardInfo.TaskSource
		redirectInfo = forwardInfo.GetRedirectInfo()
	}
	return &internalTask{
		taskDispatchRevisionNumber: taskDispatchRevisionNumber,
		event: &genericTaskInfo{
			AllocatedTaskInfo: &persistencespb.AllocatedTaskInfo{
				Data:   info,
				TaskId: syncMatchTaskId,
			},
		},
		forwardInfo:       forwardInfo,
		source:            source,
		redirectInfo:      redirectInfo,
		responseC:         make(chan taskResponse, 1),
		effectivePriority: priorityKey(info.GetPriority().GetPriorityKey()),

		targetWorkerDeploymentVersion: targetVersion,
	}
}

func newInternalTaskFromBacklog(
	info *persistencespb.AllocatedTaskInfo,
	completionFunc func(*internalTask, taskResponse),
) *internalTask {
	return &internalTask{
		event: &genericTaskInfo{
			AllocatedTaskInfo: info,
			completionFunc:    completionFunc,
		},
		source:            enumsspb.TASK_SOURCE_DB_BACKLOG,
		effectivePriority: priorityKey(info.GetData().GetPriority().GetPriorityKey()),
	}
}

func newInternalQueryTask(
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) *internalTask {
	return &internalTask{
		query: &queryTaskInfo{
			taskID:  taskID,
			request: request,
		},
		forwardInfo:       request.GetForwardInfo(),
		responseC:         make(chan taskResponse, 1),
		source:            enumsspb.TASK_SOURCE_HISTORY,
		effectivePriority: priorityKey(request.GetPriority().GetPriorityKey()),
	}
}

func newInternalNexusTask(
	taskID string,
	deadline time.Time,
	operationDeadline time.Time,
	request *matchingservice.DispatchNexusTaskRequest,
) *internalTask {
	return &internalTask{
		nexus: &nexusTaskInfo{
			taskID:            taskID,
			deadline:          deadline,
			operationDeadline: operationDeadline,
			request:           request,
		},
		forwardInfo: request.GetForwardInfo(),
		responseC:   make(chan taskResponse, 1),
		source:      enumsspb.TASK_SOURCE_HISTORY,
	}
}

func newInternalStartedTask(info *startedTaskInfo) *internalTask {
	return &internalTask{started: info}
}

func newPollForwarderTask() *internalTask {
	return &internalTask{effectivePriority: pollForwarderPriority}
}

func (task *internalTask) isPollForwarder() bool {
	return task.effectivePriority == pollForwarderPriority
}

// isQuery returns true if the underlying task is a query task
func (task *internalTask) isQuery() bool {
	return task.query != nil
}

// isNexus returns true if the underlying task is a nexus task
func (task *internalTask) isNexus() bool {
	return task.nexus != nil
}

// isStarted is true when this task is already marked as started
func (task *internalTask) isStarted() bool {
	return task.started != nil
}

// isForwarded returns true if the underlying task is forwarded by a remote matching host
// forwarded tasks are already marked as started in history
func (task *internalTask) isForwarded() bool {
	return task.forwardInfo != nil
}

func (task *internalTask) isSyncMatchTask() bool {
	return task.responseC != nil
}

func (task *internalTask) workflowExecution() *commonpb.WorkflowExecution {
	switch {
	case task.event != nil:
		return &commonpb.WorkflowExecution{WorkflowId: task.event.Data.GetWorkflowId(), RunId: task.event.Data.GetRunId()}
	case task.query != nil:
		return task.query.request.GetQueryRequest().GetExecution()
	case task.started != nil && task.started.workflowTaskInfo != nil:
		return task.started.workflowTaskInfo.WorkflowExecution
	case task.started != nil && task.started.activityTaskInfo != nil:
		return task.started.activityTaskInfo.WorkflowExecution
	}
	return &commonpb.WorkflowExecution{}
}

// pollWorkflowTaskQueueResponse returns the poll response for a workflow task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollWorkflowTaskQueueResponse() *matchingservice.PollWorkflowTaskQueueResponse {
	if task.isStarted() {
		return task.started.workflowTaskInfo
	}
	return nil
}

// pollActivityTaskQueueResponse returns the poll response for an activity task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollActivityTaskQueueResponse() *matchingservice.PollActivityTaskQueueResponse {
	if task.isStarted() {
		return task.started.activityTaskInfo
	}
	return nil
}

// pollNexusTaskQueueResponse returns the poll response for a nexus task that is ready for dispatching. This method
// should only be called when isStarted() is true
func (task *internalTask) pollNexusTaskQueueResponse() *matchingservice.PollNexusTaskQueueResponse {
	if task.isStarted() {
		if task.started.nexusTaskInfo.Response != nil {
			task.started.nexusTaskInfo.Response.PollerScalingDecision = task.pollerScalingDecision
		}
		return task.started.nexusTaskInfo
	}
	return nil
}

// getResponse waits for a response on the task's response channel.
func (task *internalTask) getResponse() (taskResponse, bool) {
	if task.responseC == nil {
		return taskResponse{}, false
	}
	return <-task.responseC, true
}

func (task *internalTask) getPriority() *commonpb.Priority {
	if task.event != nil {
		return task.event.AllocatedTaskInfo.GetData().GetPriority()
	} else if task.query != nil {
		return task.query.request.GetPriority()
	}
	// nexus tasks don't have priorities for now
	return nil
}

func (task *internalTask) fairLevel() fairLevel {
	return fairLevelFromAllocatedTask(task.event.AllocatedTaskInfo)
}

// resetMatcherState must be called before adding or re-adding a backlog task to priMatcher.
func (task *internalTask) resetMatcherState() {
	task.removeFromMatcher.Store(&removeFuncNotAddedYet)
}

// setRemoveFunc sets the function to remove the task from the matcher.
// It returns true if the task is still valid and the function was set,
// false if the task was evicted already and should not be added.
func (task *internalTask) setRemoveFunc(remove func()) bool {
	return task.removeFromMatcher.CompareAndSwap(&removeFuncNotAddedYet, &remove)
}

// setEvicted marks the task as evicted. If it was added to a matcher it will be removed.
func (task *internalTask) setEvicted() {
	remove := task.removeFromMatcher.Swap(&removeFuncEvicted)
	(*remove)()
}

// finish marks a task as finished. Must be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
//
// If the task took a rate limit token and didn't "use" it by actually dispatching the task,
// finish will be called with wasValid=false and task.recycleToken=clockedRateLimiter.RecycleToken,
// so finish will call the rate limiter's RecycleToken to give the unused token back to any process
// that is waiting on the token, if one exists.
func (task *internalTask) finish(err error, wasValid bool) {
	res := taskResponse{startErr: err}
	task.finishInternal(res, wasValid)
}

// finishForward must be called after forwarding a task.
func (task *internalTask) finishForward(forwardRes any, forwardErr error, wasValid bool) {
	res := taskResponse{forwarded: true, forwardRes: forwardRes, forwardErr: forwardErr}
	task.finishInternal(res, wasValid)
}

func (task *internalTask) finishInternal(res taskResponse, wasValid bool) {
	if !wasValid && task.recycleToken != nil {
		task.recycleToken(task)
	}

	switch {
	case task.responseC != nil:
		task.responseC <- res
	case task.event.completionFunc != nil:
		// TODO: this probably should not be done synchronously in PollWorkflow/ActivityTaskQueue
		task.event.completionFunc(task, res)
	}
}
