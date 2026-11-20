package history

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

func (t *transferQueueStandbyTaskExecutor) checkChildCompletionRecorded(
	ctx context.Context, task *tasks.CloseExecutionTask, info *closeExecutionVerificationInfo, attempt int,
) (any, error) {
	request := info.request
	request.ResendParent = t.getCurrentTime().After(task.GetVisibilityTime().Add(t.config.MaxLocalParentWorkflowVerificationDuration()))
	if request.ResendParent {
		entry, err := t.registry.GetNamespaceByID(namespace.ID(request.NamespaceId))
		switch err.(type) {
		case nil:
			request.ResendParent = t.config.EnableTransitionHistory(entry.Name().String())
		case *serviceerror.NamespaceNotFound:
			return nil, nil
		default:
			return nil, err
		}
	}
	emitChildCompletionVerificationStarted(t.shardContext, task, request.NamespaceId,
		request.ParentExecution.WorkflowId, request.ParentExecution.RunId, request.ParentInitiatedId,
		request.ParentInitiatedVersion, info.childWorkflowState, request.ResendParent, attempt)
	_, err := t.historyRawClient.VerifyChildExecutionCompletionRecorded(ctx, request)
	emitChildCompletionVerificationResult(t.shardContext, task, request.NamespaceId,
		request.ParentExecution.WorkflowId, request.ParentExecution.RunId, request.ParentInitiatedId,
		request.ParentInitiatedVersion, info.childWorkflowState, request.ResendParent, attempt, err)
	switch err.(type) {
	case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
		return nil, nil
	case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
		key := definition.NewWorkflowKey(request.NamespaceId, request.ParentExecution.WorkflowId, request.ParentExecution.RunId)
		return &verifyCompletionRecordedPostActionInfo{parentWorkflowKey: &key}, nil
	default:
		return nil, &verificationErr{msg: recordChildCompletionVerificationFailedMsg, err: err}
	}
}

func (t *transferQueueStandbyTaskExecutor) checkChildStarted(
	ctx context.Context, task *tasks.StartChildExecutionTask, info *startChildExecutionVerificationInfo,
) (any, error) {
	request := info.request
	if request.NamespaceId == "" {
		entry, err := t.registry.GetNamespace(info.namespaceName)
		if err != nil {
			return nil, err
		}
		request.NamespaceId = entry.ID().String()
	}
	key := definition.NewWorkflowKey(request.NamespaceId, request.WorkflowExecution.WorkflowId, request.WorkflowExecution.RunId)
	request.ResendChild = t.getCurrentTime().After(task.GetVisibilityTime().Add(t.config.StandbyTaskMissingEventsResendDelay(task.GetType())))
	if request.ResendChild {
		entry, err := t.registry.GetNamespaceByID(namespace.ID(request.NamespaceId))
		switch err.(type) {
		case nil:
			request.ResendChild = t.config.EnableTransitionHistory(entry.Name().String())
		case *serviceerror.NamespaceNotFound:
			return nil, nil
		default:
			return nil, err
		}
	}
	_, err := t.historyRawClient.VerifyFirstWorkflowTaskScheduled(ctx, request)
	switch err.(type) {
	case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
		return nil, nil
	case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
		return &startChildExecutionPostActionInfo{childWorkflowKey: &key}, nil
	default:
		return nil, &verificationErr{msg: verifyFirstWorkflowTaskScheduledFailedMsg, err: err}
	}
}
