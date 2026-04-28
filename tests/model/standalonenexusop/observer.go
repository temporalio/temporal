package standalonenexusop

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/grpc"
)

const (
	methodStart     = workflowservice.WorkflowService_StartNexusOperationExecution_FullMethodName
	methodTerminate = workflowservice.WorkflowService_TerminateNexusOperationExecution_FullMethodName
	methodCancel    = workflowservice.WorkflowService_RequestCancelNexusOperationExecution_FullMethodName
	methodDelete    = workflowservice.WorkflowService_DeleteNexusOperationExecution_FullMethodName
)

// observer updates World.Operations from observed RPC traffic.
type observer struct {
	store *umpire.EntityStore[*Entity]
	u     *umpire.Umpire
}

func newObserver(deps Deps) *observer {
	return &observer{store: deps.Store, u: deps.Umpire}
}

func (o *observer) ObservedMethods() []string {
	return []string{methodStart, methodTerminate, methodCancel, methodDelete}
}

func (o *observer) Strategy() umpire.Strategy {
	return umpire.Strategy{
		Name: "standalone-nexus-op-observer",
		Server: func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			resp, err := handler(ctx, req)
			if err != nil {
				return resp, err
			}
			switch info.FullMethod {
			case methodStart:
				o.handleStart(req, resp)
			case methodTerminate:
				o.handleTerminate(req)
			case methodCancel:
				o.handleCancel(req)
			case methodDelete:
				o.handleDelete(req)
			}
			return resp, err
		},
	}
}

func (o *observer) handleStart(req, resp any) {
	rq, ok := req.(*workflowservice.StartNexusOperationExecutionRequest)
	if !ok {
		return
	}
	rs, ok := resp.(*workflowservice.StartNexusOperationExecutionResponse)
	if !ok || !rs.GetStarted() {
		return
	}
	key := rq.GetNamespace() + "/" + rq.GetOperationId()
	if _, exists := o.store.Get(key); exists {
		return
	}
	o.store.Add(&Entity{
		Namespace:   rq.GetNamespace(),
		OperationID: rq.GetOperationId(),
		RequestID:   rq.GetRequestId(),
		RunID:       rs.GetRunId(),
		Status:      StatusRunning,
	})
	o.u.Record(&umpire.EntityCreated{
		Type:     "nexus-operation",
		EntityID: key,
		Parents: map[string]string{
			"namespace": rq.GetNamespace(),
			"endpoint":  rq.GetEndpoint(),
		},
	})
}

func (o *observer) handleTerminate(req any) {
	r, ok := req.(*workflowservice.TerminateNexusOperationExecutionRequest)
	if !ok {
		return
	}
	op, ok := o.store.Get(r.GetNamespace() + "/" + r.GetOperationId())
	if !ok {
		return
	}
	umpire.RecordTransition(o.u, op.OperationID, &op.Status, StatusTerminated)
}

func (o *observer) handleCancel(req any) {
	r, ok := req.(*workflowservice.RequestCancelNexusOperationExecutionRequest)
	if !ok {
		return
	}
	op, ok := o.store.Get(r.GetNamespace() + "/" + r.GetOperationId())
	if !ok {
		return
	}
	op.CancelRequested = true
}

func (o *observer) handleDelete(req any) {
	r, ok := req.(*workflowservice.DeleteNexusOperationExecutionRequest)
	if !ok {
		return
	}
	key := r.GetNamespace() + "/" + r.GetOperationId()
	if _, exists := o.store.Get(key); !exists {
		return
	}
	o.store.Remove(key)
	o.u.Record(&umpire.EntityDeleted{Type: "nexus-operation", EntityID: key})
}
