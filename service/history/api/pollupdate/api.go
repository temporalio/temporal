package pollupdate

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/workflow"
)

// updateObserver is an implementation of workflow.Observer that observes the
// outcome of workflow execution update.
type updateObserver struct {
	mut sync.Mutex
	id  string
	fut *future.Proxy[*updatepb.Outcome]
}

type WorkflowCtxLookup func(
	context.Context,
	*clockspb.VectorClock,
	api.MutableStateConsistencyPredicate,
	definition.WorkflowKey,
) (api.WorkflowContext, error)

// Invoke waits for the outcome of a workflow execution update. It may block for
// as long as is specified by the context.Context argument.
func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	findWorkflow WorkflowCtxLookup,
	wfobservers *workflow.ObserverSet,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	updateRef := req.GetRequest().GetUpdateRef()
	obsKey := workflow.ObserverKey{
		WFKey: definition.NewWorkflowKey(
			req.GetRequest().GetNamespace(),
			updateRef.GetWorkflowExecution().GetWorkflowId(),
			updateRef.GetWorkflowExecution().GetRunId(),
		),
		ObservableID: updateRef.GetUpdateId(),
	}

	wfCtxLookup := func() (workflow.Context, func(error), error) {
		weCtx, err := findWorkflow(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			obsKey.WFKey,
		)
		if err != nil {
			return nil, nil, err
		}
		return weCtx.GetContext(), weCtx.GetReleaseFn(), nil
	}

	observerCtor := func() (workflow.Observer, error) {
		return &updateObserver{
			id:  updateRef.GetUpdateId(),
			fut: future.NewProxy[*updatepb.Outcome](future.NewFuture[*updatepb.Outcome]()),
		}, nil
	}

	obs, release, err := wfobservers.FindOrCreate(ctx, obsKey, wfCtxLookup, observerCtor)
	if err != nil {
		return nil, err
	}
	defer release()
	upObserver, ok := obs.(*updateObserver)
	if !ok {
		return nil, serviceerror.NewInternal("wrong observer type found, perhaps due to ID reuse")
	}
	outcome, err := upObserver.AwaitOutcome(ctx)
	if err != nil {
		return nil, err
	}
	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{Outcome: outcome},
	}, nil
}

func (uo *updateObserver) Connect(ctx context.Context, wfctx workflow.Context) error {
	reg := wfctx.UpdateRegistry()
	if fut, ok := reg.Outcome(uo.id); ok {
		// update still in-flight, attach to the future in update registry
		uo.fut.Rebind(fut)
		return nil
	}
	// update is either complete or does not exist - look up in mutable state.
	ms, err := wfctx.LoadMutableState(ctx)
	if err != nil {
		return err
	}
	outcome, err := ms.GetUpdateOutcome(ctx, uo.id)
	if err != nil {
		uo.fut.Rebind(future.NewReadyFuture[*updatepb.Outcome](nil, err))
		return nil
	}

	// found the outcome in MutableState/History - now tunnel it out through
	// this observer's future - subsequent calls to AwaitOutcome will be able to
	// return immediately. Note that the Rebind call is threadsafe.
	uo.fut.Rebind(future.NewReadyFuture(outcome, nil))
	return nil
}

func (uo *updateObserver) AwaitOutcome(ctx context.Context) (*updatepb.Outcome, error) {
	return uo.fut.Get(ctx)
}
