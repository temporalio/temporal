package matching

import (
	executionpb "go.temporal.io/temporal-proto/execution"

	commongenpb "github.com/temporalio/temporal/.gen/proto/common"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	// genericTaskInfo contains the info for an activity or decision task
	genericTaskInfo struct {
		*persistenceblobs.AllocatedTaskInfo
		completionFunc func(*persistenceblobs.AllocatedTaskInfo, error)
	}
	// queryTaskInfo contains the info for a query task
	queryTaskInfo struct {
		taskID  string
		request *matchingservice.QueryWorkflowRequest
	}
	// startedTaskInfo contains info for any task received from
	// another matching host. This type of task is already marked as started
	startedTaskInfo struct {
		decisionTaskInfo *matchingservice.PollForDecisionTaskResponse
		activityTaskInfo *matchingservice.PollForActivityTaskResponse
	}
	// internalTask represents an activity, decision, query or started (received from another host).
	// this struct is more like a union and only one of [ query, event, forwarded ] is
	// non-nil for any given task
	internalTask struct {
		event            *genericTaskInfo // non-nil for activity or decision task that's locally generated
		query            *queryTaskInfo   // non-nil for a query task that's locally sync matched
		started          *startedTaskInfo // non-nil for a task received from a parent partition which is already started
		namespace        string
		source           commongenpb.TaskSource
		forwardedFrom    string     // name of the child partition this task is forwarded from (empty if not forwarded)
		responseC        chan error // non-nil only where there is a caller waiting for response (sync-match)
		backlogCountHint int64
	}
)

func newInternalTask(
	info *persistenceblobs.AllocatedTaskInfo,
	completionFunc func(*persistenceblobs.AllocatedTaskInfo, error),
	source commongenpb.TaskSource,
	forwardedFrom string,
	forSyncMatch bool,
) *internalTask {
	task := &internalTask{
		event: &genericTaskInfo{
			AllocatedTaskInfo: info,
			completionFunc:    completionFunc,
		},
		source:        source,
		forwardedFrom: forwardedFrom,
	}
	if forSyncMatch {
		task.responseC = make(chan error, 1)
	}
	return task
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
		forwardedFrom: request.GetForwardedFrom(),
		responseC:     make(chan error, 1),
	}
}

func newInternalStartedTask(info *startedTaskInfo) *internalTask {
	return &internalTask{started: info}
}

// isQuery returns true if the underlying task is a query task
func (task *internalTask) isQuery() bool {
	return task.query != nil
}

// isStarted is true when this task is already marked as started
func (task *internalTask) isStarted() bool {
	return task.started != nil
}

// isForwarded returns true if the underlying task is forwarded by a remote matching host
// forwarded tasks are already marked as started in history
func (task *internalTask) isForwarded() bool {
	return task.forwardedFrom != ""
}

func (task *internalTask) workflowExecution() *executionpb.WorkflowExecution {
	switch {
	case task.event != nil:
		return &executionpb.WorkflowExecution{WorkflowId: task.event.Data.GetWorkflowId(), RunId: primitives.UUIDString(task.event.Data.GetRunId())}
	case task.query != nil:
		return task.query.request.GetQueryRequest().GetExecution()
	case task.started != nil && task.started.decisionTaskInfo != nil:
		return task.started.decisionTaskInfo.WorkflowExecution
	case task.started != nil && task.started.activityTaskInfo != nil:
		return task.started.activityTaskInfo.WorkflowExecution
	}
	return &executionpb.WorkflowExecution{}
}

// pollForDecisionResponse returns the poll response for a decision task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollForDecisionResponse() *matchingservice.PollForDecisionTaskResponse {
	if task.isStarted() {
		return task.started.decisionTaskInfo
	}
	return nil
}

// pollForActivityResponse returns the poll response for an activity task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollForActivityResponse() *matchingservice.PollForActivityTaskResponse {
	if task.isStarted() {
		return task.started.activityTaskInfo
	}
	return nil
}

// finish marks a task as finished. Should be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
func (task *internalTask) finish(err error) {
	switch {
	case task.responseC != nil:
		task.responseC <- err
	case task.event.completionFunc != nil:
		task.event.completionFunc(task.event.AllocatedTaskInfo, err)
	}
}
