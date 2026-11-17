package recordchildworkflowcompleted

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// maxResetRedirectCount is the number of times we follow the reset run ID to forward the request to the new parent.
// This redirection happens only when a workflow is reset and in most cases it's 1 or 2 hops.
// maxResetRedirectCount prevents us from following long chain of resets (or some circular loop in redirects).
const maxResetRedirectCount = 100

// errRecordedParentRunMissing marks the persistence error for the parent run named by the request.
// Fallback matches this marker. Other NotFound errors returned while recording the completion do not.
var errRecordedParentRunMissing = errors.New("recorded parent run is missing")

// errFallbackChildNotLinked means the current run is not the parent still waiting on this child.
// Invoke restores the recorded run ID and returns the original missing-run error.
var errFallbackChildNotLinked = errors.New("current run is not linked to the completing child")

type recordedParentRunMissingError struct {
	err error
}

func (e *recordedParentRunMissingError) Error() string {
	return e.err.Error()
}

func (e *recordedParentRunMissingError) Unwrap() error {
	return e.err
}

func (e *recordedParentRunMissingError) Is(target error) bool {
	return target == errRecordedParentRunMissing
}

// This API records the child completion event in the parent's history. It does the following.
// - Rejects the request if the parent was closed for any reason other than reset.
// - If the parent was closed due to reset, it forwards the request to the new parent following the resetRunID link.
// - If loading the recorded parent run reports that this run is missing, it delivers the completion to the current run of the same workflow ID when that run has a pending child with the same initiated event, child workflow ID, and non-empty first run ID.
// - It ensures that the child sending the completion request was initialized by this parent before accepting the request.
func Invoke(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	_, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()), request.GetParentExecution().WorkflowId)
	if err != nil {
		return nil, err
	}

	// If the parent is reset, we need to follow a possible chain of resets to deliver the completion event to the correct parent.
	// A recorded run that retention has deleted has no ResetRunId to follow. The current run is tried once when that
	// load reports this run is missing and the current run's pending child has the same non-empty first run ID.
	recordedRunID := request.GetParentExecution().GetRunId()
	redirectCount := 0
	resolvedCurrent := false
	fallbackDelivery := false
	var missingParent error
	for {
		resetRunID, recordErr := recordChildWorkflowCompleted(ctx, request, shardContext, workflowConsistencyChecker, fallbackDelivery)
		fallbackDelivery = false
		if errors.Is(recordErr, errFallbackChildNotLinked) {
			request.ParentExecution.RunId = recordedRunID
			if missingParent == nil {
				return nil, consts.ErrChildExecutionNotFound
			}
			return nil, recordedParentRunMissingCause(missingParent)
		}
		if errors.Is(recordErr, consts.ErrWorkflowCompleted) {
			// if the parent was reset, forward the request to the new run pointed by resetRunID
			// Note: An alternative solution is to load the current run here ane compare the originalRunIDs of the current run and the closed parent.
			// If they match, then deliver it to the current run. We should consider this optimization if we notice that reset chain is longer than 1-2 hops.
			if resetRunID != "" {
				if redirectCount >= maxResetRedirectCount {
					return nil, consts.ErrResetRedirectLimitReached
				}
				redirectCount++
				request.ParentExecution.RunId = resetRunID
				continue
			}
		}
		if recordErr != nil && !resolvedCurrent && recordedRunID != "" && errors.Is(recordErr, errRecordedParentRunMissing) {
			// An empty first run ID cannot distinguish a reset successor from a later execution that reused the workflow ID.
			if request.GetChildFirstExecutionRunId() == "" {
				return nil, recordedParentRunMissingCause(recordErr)
			}
			missingParent = recordErr
			currentRunID, currentErr := workflowConsistencyChecker.GetCurrentWorkflowRunID(
				ctx,
				request.GetNamespaceId(),
				request.GetParentExecution().GetWorkflowId(),
				locks.PriorityHigh,
			)
			if currentErr != nil {
				return nil, currentErr
			}
			if currentRunID != "" && currentRunID != request.GetParentExecution().GetRunId() {
				resolvedCurrent = true
				request.ParentExecution.RunId = currentRunID
				fallbackDelivery = true
				continue
			}
		}
		if recordErr != nil {
			return nil, recordedParentRunMissingCause(recordErr)
		}
		return &historyservice.RecordChildExecutionCompletedResponse{}, nil
	}
}

// recordedParentRunMissingCause returns the persistence error wrapped at the parent load boundary.
func recordedParentRunMissingCause(err error) error {
	missing, ok := errors.AsType[*recordedParentRunMissingError](err)
	if !ok {
		return err
	}
	return missing.err
}

// recordChildWorkflowCompleted records the child completed event in the parent history if the parent is still running.
// It returns consts.ErrWorkflowCompleted if the parent is already completed. Additionally a reset run ID is returned if the parent was completed due to a reset operation.
func recordChildWorkflowCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	fallbackDelivery bool,
) (resetRunID string, retError error) {
	parentInitiatedID := request.ParentInitiatedId
	parentInitiatedVersion := request.ParentInitiatedVersion
	workflowKey := definition.NewWorkflowKey(
		request.NamespaceId,
		request.GetParentExecution().WorkflowId,
		request.GetParentExecution().RunId,
	)
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		request.Clock,
		func(mutableState historyi.MutableState) bool {
			if !mutableState.IsWorkflowExecutionRunning() {
				// current branch already closed, we won't perform any operation, pass the check
				return true
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil {
				// can't find initiated event, potential stale mutable, fail the predicate check
				return false
			}
			if !onCurrentBranch {
				// found on different branch, since we don't record completion on a different branch, pass the check
				return true
			}

			_, childInitEventFound := mutableState.GetChildExecutionInfo(parentInitiatedID)
			return childInitEventFound
		},
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		// Mark only the missing-execution error from this load. NotFound from the update below stays unmarked.
		if persistence.IsWorkflowExecutionNotFound(err, workflowKey.WorkflowID, workflowKey.RunID) {
			return "", &recordedParentRunMissingError{err: err}
		}
		return "", err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	retError = api.UpdateWorkflowWithNew(
		shardContext,
		ctx,
		workflowLease,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				// A closed current run was selected by workflow ID. Do not follow its reset link.
				if fallbackDelivery {
					return nil, errFallbackChildNotLinked
				}
				resetRunID = mutableState.GetExecutionInfo().ResetRunId
				return nil, consts.ErrWorkflowCompleted
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil || !onCurrentBranch {
				if fallbackDelivery {
					return nil, errFallbackChildNotLinked
				}
				return nil, consts.ErrChildExecutionNotFound
			}

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(parentInitiatedID)
			if !isRunning {
				if fallbackDelivery {
					return nil, errFallbackChildNotLinked
				}
				return nil, consts.ErrChildExecutionNotFound
			}

			// StartedRunId is written by the child-started event and is the child's first run ID.
			// Recording a missing started event would copy the request's first run ID onto this child
			// before that comparison, so the fallback check happens first.
			if fallbackDelivery && !childFirstRunLinked(ci, request) {
				return nil, errFallbackChildNotLinked
			}

			// note we already checked if startedEventID is empty (in consistency predicate)
			// and reloaded mutable state, so if startedEventID is still missing, we need to
			// record a started event before recording completion event.
			if err := recordStartedEventIfMissing(ctx, mutableState, request, ci); err != nil {
				return nil, err
			}

			childExecution := request.GetChildExecution()
			if ci.GetStartedWorkflowId() != childExecution.GetWorkflowId() {
				// this can only happen when we don't have the initiated version
				return nil, consts.ErrChildExecutionNotFound
			}

			if request.GetChildFirstExecutionRunId() != "" && ci.GetStartedRunId() != request.GetChildFirstExecutionRunId() {
				// this can happen when parent starts another child run in different branch
				return nil, consts.ErrChildExecutionNotFound
			}

			completionEvent := request.CompletionEvent
			switch completionEvent.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				attributes := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				attributes := completionEvent.GetWorkflowExecutionFailedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionFailedEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				attributes := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(parentInitiatedID, childExecution)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
				attributes := completionEvent.GetWorkflowExecutionTimedOutEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(parentInitiatedID, childExecution, attributes)
			}
			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
	)
	return resetRunID, retError
}

// childFirstRunLinked reports that this pending child is the one the completion is for.
// The initiated event ID already selected ci. StartedRunId is empty until the child starts,
// and an empty request first-run ID is not a match.
func childFirstRunLinked(ci *persistencespb.ChildExecutionInfo, request *historyservice.RecordChildExecutionCompletedRequest) bool {
	firstRunID := request.GetChildFirstExecutionRunId()
	startedRunID := ci.GetStartedRunId()
	return firstRunID != "" &&
		startedRunID != "" &&
		startedRunID == firstRunID &&
		ci.GetStartedWorkflowId() == request.GetChildExecution().GetWorkflowId()
}

func recordStartedEventIfMissing(
	ctx context.Context,
	mutableState historyi.MutableState,
	request *historyservice.RecordChildExecutionCompletedRequest,
	ci *persistencespb.ChildExecutionInfo,
) error {
	parentInitiatedID := request.ParentInitiatedId
	if ci.StartedEventId == common.EmptyEventID {
		initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, parentInitiatedID)
		if err != nil {
			return consts.ErrChildExecutionNotFound
		}
		initiatedAttr := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		execution := &commonpb.WorkflowExecution{
			WorkflowId: request.GetChildExecution().GetWorkflowId(),
			RunId:      request.GetChildExecution().GetRunId(),
		}
		if request.GetChildFirstExecutionRunId() != "" {
			execution.RunId = request.GetChildFirstExecutionRunId()
		}
		// note values used here should not matter because the child info will be deleted
		// when the response is recorded, so it should be fine e.g. that ci.Clock is nil
		_, err = mutableState.AddChildWorkflowExecutionStartedEvent(
			execution,
			initiatedAttr.WorkflowType,
			initiatedEvent.EventId,
			initiatedAttr.Header,
			ci.Clock,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
