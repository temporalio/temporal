package namespace

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/grpc"
)

// methodRegister is the gRPC full-method name for the only namespace
// lifecycle RPC the observer reacts to.
const methodRegister = workflowservice.WorkflowService_RegisterNamespace_FullMethodName

// observer updates the World's Namespaces store from observed RPC traffic.
type observer struct {
	store *umpire.EntityStore[*Entity]
	u     *umpire.Umpire
}

func newObserver(deps Deps) *observer {
	return &observer{store: deps.Store, u: deps.Umpire}
}

// ObservedMethods returns the gRPC full-method names this observer handles.
func (o *observer) ObservedMethods() []string {
	return []string{methodRegister}
}

// Strategy returns the umpire.Strategy that runs the observer on the
// server side.
func (o *observer) Strategy() umpire.Strategy {
	return umpire.Strategy{
		Name: "namespace-observer",
		Server: func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			resp, err := handler(ctx, req)
			if err == nil && info.FullMethod == methodRegister {
				o.handleRegister(req)
			}
			return resp, err
		},
	}
}

func (o *observer) handleRegister(req any) {
	r, ok := req.(*workflowservice.RegisterNamespaceRequest)
	if !ok {
		return
	}
	if _, exists := o.store.Get(r.GetNamespace()); exists {
		return
	}
	o.store.Add(&Entity{Name: r.GetNamespace(), Status: StatusRegistered})
	o.u.Record(&umpire.EntityCreated{Type: "namespace", EntityID: r.GetNamespace()})
}
