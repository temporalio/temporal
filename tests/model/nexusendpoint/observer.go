package nexusendpoint

import (
	"context"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/grpc"
)

const (
	methodCreate = operatorservice.OperatorService_CreateNexusEndpoint_FullMethodName
	methodDelete = operatorservice.OperatorService_DeleteNexusEndpoint_FullMethodName
)

// observer updates World.Endpoints from observed RPC traffic.
type observer struct {
	store *umpire.EntityStore[*Entity]
	u     *umpire.Umpire
}

func newObserver(deps Deps) *observer {
	return &observer{store: deps.Store, u: deps.Umpire}
}

func (o *observer) ObservedMethods() []string {
	return []string{methodCreate, methodDelete}
}

func (o *observer) Strategy() umpire.Strategy {
	return umpire.Strategy{
		Name: "nexus-endpoint-observer",
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
			case methodCreate:
				o.handleCreate(resp)
			case methodDelete:
				o.handleDelete(req)
			}
			return resp, err
		},
	}
}

func (o *observer) handleCreate(resp any) {
	r, ok := resp.(*operatorservice.CreateNexusEndpointResponse)
	if !ok {
		return
	}
	ep := r.GetEndpoint()
	spec := ep.GetSpec()
	target := spec.GetTarget().GetWorker()
	name := spec.GetName()
	if _, exists := o.store.Get(name); exists {
		return
	}
	o.store.Add(&Entity{
		Name:      name,
		ID:        ep.GetId(),
		Version:   ep.GetVersion(),
		Namespace: target.GetNamespace(),
		TaskQueue: target.GetTaskQueue(),
		Status:    StatusCreated,
	})
	o.u.Record(&umpire.EntityCreated{
		Type:     "nexus-endpoint",
		EntityID: name,
		Parents: map[string]string{
			"namespace": target.GetNamespace(),
			"taskqueue": target.GetTaskQueue(),
		},
	})
}

func (o *observer) handleDelete(req any) {
	r, ok := req.(*operatorservice.DeleteNexusEndpointRequest)
	if !ok {
		return
	}
	for _, ep := range o.store.All() {
		if ep.ID != r.GetId() {
			continue
		}
		umpire.RecordTransition(o.u, ep.Name, &ep.Status, StatusDeleted)
		o.u.Record(&umpire.EntityDeleted{Type: "nexus-endpoint", EntityID: ep.Name})
		return
	}
}
