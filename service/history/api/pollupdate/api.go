package pollupdate

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/update"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	shardContext historyi.ShardContext,
	ctxLookup api.WorkflowConsistencyChecker,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	waitStage := req.GetRequest().GetWaitPolicy().GetLifecycleStage()
	updateRef := req.GetRequest().GetUpdateRef()
	wfexec := updateRef.GetWorkflowExecution()
	wfKey, upd, err := func() (*definition.WorkflowKey, *update.Update, error) {
		workflowLease, err := ctxLookup.GetWorkflowLease(
			ctx,
			nil,
			definition.NewWorkflowKey(
				req.GetNamespaceId(),
				wfexec.GetWorkflowId(),
				wfexec.GetRunId(),
			),
			locks.PriorityHigh,
		)
		if err != nil {
			return nil, nil, err
		}
		release := workflowLease.GetReleaseFn()
		defer release(nil)
		wfCtx := workflowLease.GetContext()
		upd := wfCtx.UpdateRegistry(ctx).Find(ctx, updateRef.UpdateId)
		wfKey := wfCtx.GetWorkflowKey()
		return &wfKey, upd, nil
	}()
	if err != nil {
		return nil, err
	}
	if upd == nil {
		return nil, serviceerror.NewNotFoundf("update %q not found", updateRef.GetUpdateId())
	}

	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	softTimeout := shardContext.GetConfig().LongPollExpirationInterval(ns.Name().String())
	// If the long-poll times out due to softTimeout
	// then return a non-error empty response with actual reached stage.
	status, err := upd.WaitLifecycleStage(ctx, waitStage, softTimeout)
	if err != nil {
		return nil, err
	}

	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
			Outcome: status.Outcome,
			Stage:   status.Stage,
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      wfKey.RunID,
				},
				UpdateId: updateRef.UpdateId,
			},
		},
	}, nil
}
