// Package action holds the Temporal-specific Nexus-operation actions and their realizers,
// plus the runtime glue (RealizeContext, StateOracle, a programmable mock handler) over a live
// test env. The generic action schema and Drive loop live in common/testing/umpire. See
// UMPIRE_ACTIONS.md and PLAN.md.
package action

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/workflowservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Ctx is the Temporal-side umpire.RealizeContext: the live env, the resolved mock endpoint, the
// programmable handler, and the running Var→identity bindings. Realizers type-assert the
// umpire.RealizeContext they are handed back to *Ctx.
type Ctx struct {
	Env      *testcore.TestEnv
	Endpoint string
	Handler  *ResponsePolicy
	Iter     int

	mu   sync.Mutex
	bind map[string]string
}

// NewCtx builds a RealizeContext for one drive.
func NewCtx(env *testcore.TestEnv, endpoint string, h *ResponsePolicy, iter int) *Ctx {
	return &Ctx{Env: env, Endpoint: endpoint, Handler: h, Iter: iter, bind: map[string]string{}}
}

func (c *Ctx) Binding(v string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.bind[v]
	return id, ok
}

func (c *Ctx) Bind(v, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bind[v] = id
}

// bindFresh binds every Fresh effect var of a to id.
func bindFresh(rc umpire.RealizeContext, a umpire.Action, id string) {
	for _, e := range a.Effects {
		if e.Ref.Fresh {
			rc.Bind(e.Ref.Var, id)
		}
	}
}

// Oracle implements umpire.StateOracle over a live env's Monitor model. Phase 1 handles
// NexusOperation, keyed by its captured WorkflowID (the operation's execution id for a
// standalone op, the caller workflow id for an embedded one).
type Oracle struct{ Env *testcore.TestEnv }

func (o Oracle) Current(t umpire.EntityType, id string) (string, bool) {
	if op := o.find(t, id); op != nil {
		return op.FSM.Current(), true
	}
	return "", false
}

// Visited implements umpire.VisitedOracle for reconciliation.
func (o Oracle) Visited(t umpire.EntityType, id string) ([]umpire.Edge, bool) {
	if op := o.find(t, id); op != nil {
		return op.FSM.VisitedEdges(), true
	}
	return nil, false
}

func (o Oracle) find(t umpire.EntityType, id string) *model.NexusOperation {
	nsRoot := umpire.NewEntityID(model.NamespaceType, o.Env.NamespaceID().String())
	for _, e := range o.Env.GetMonitor().ModelState().QueryEntities(t, 0, &nsRoot) {
		if op, ok := e.Entity.(*model.NexusOperation); ok && op.WorkflowID == id {
			return op
		}
	}
	return nil
}

// Resolver implements umpire.EffectResolver over the default entity lifecycles.
type Resolver struct{}

func (Resolver) Destination(t umpire.EntityType, event string) (string, bool) {
	lc, ok := planner.DefaultModels().Lifecycle(string(t))
	if !ok {
		return "", false
	}
	return lc.Destination(event)
}

// ResponsePolicy is a programmable Nexus mock handler: a HandlerResponse action installs the
// start result, and it records the first callback URL/token so a CompletionCallback action can
// complete the operation.
type ResponsePolicy struct {
	mu       sync.Mutex
	onStart  nexus.HandlerStartOperationResult[any]
	startErr error
	captured chan callback
}

type callback struct{ url, token string }

// NewResponsePolicy returns a policy with no configured response yet (an action installs one).
func NewResponsePolicy() *ResponsePolicy {
	return &ResponsePolicy{captured: make(chan callback, 1)}
}

// Handler adapts the policy to a nexustest.Handler for env.createRandomExternalNexusServer.
func (p *ResponsePolicy) Handler() nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case p.captured <- callback{opts.CallbackURL, opts.CallbackHeader.Get(commonnexus.CallbackTokenHeader)}:
			default: // already captured (a retry); keep the first
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.onStart, p.startErr
		},
		OnCancelOperation: func(_ context.Context, _, _, _ string, _ nexus.CancelOperationOptions) error { return nil },
	}
}

func (p *ResponsePolicy) setStart(r nexus.HandlerStartOperationResult[any], err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStart, p.startErr = r, err
}

// ---- Realizers ----

// rpcStartStandalone realizes StartNexusOperationExecution: creates a standalone operation
// (its own execution) and binds it — unspecified→scheduled.
type rpcStartStandalone struct{}

func (rpcStartStandalone) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (rpcStartStandalone) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	opID := fmt.Sprintf("umpire-action-op-%d", c.Iter)
	if _, err := c.Env.FrontendClient().StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              c.Env.Namespace().String(),
		OperationId:            opID,
		Endpoint:               c.Endpoint,
		Service:                "service",
		Operation:              "operation",
		RequestId:              opID,
		ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
	}); err != nil {
		return err
	}
	bindFresh(rc, a, opID) // op identity == its execution id (== WorkflowID in telemetry)
	return nil
}

// handlerAsync realizes handler:AsyncAck: installs the mock handler to acknowledge the start
// asynchronously — scheduled→started. Reactive: the work is in Install.
type handlerAsync struct{}

func (handlerAsync) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "umpire-action-token"}, nil)
	return nil
}

func (handlerAsync) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// completion realizes callback:Complete(...): delivers an async completion to the captured
// callback — started→{succeeded,failed,canceled} (opErr nil = success).
type completion struct{ opErr *nexus.OperationError }

func (completion) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (co completion) Fire(ctx context.Context, rc umpire.RealizeContext, _ umpire.Action) error {
	c := rc.(*Ctx)
	var cb callback
	select {
	case cb = <-c.Handler.captured:
	case <-ctx.Done():
		return ctx.Err()
	}
	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{Serializer: commonnexus.PayloadSerializer})
	opts := nexusrpc.CompleteOperationOptions{Header: nexus.Header{commonnexus.CallbackTokenHeader: cb.token}}
	if co.opErr != nil {
		opts.Error = co.opErr
	} else {
		opts.Result = payload.EncodeString("umpire-action-result")
	}
	return client.CompleteOperation(ctx, cb.url, opts)
}

// ---- Declared actions (Phase 1: the standalone completion path) ----

func nexusOp(v string, fresh bool) umpire.Ref {
	return umpire.Ref{Type: model.NexusOperationType, Var: v, Fresh: fresh}
}

var (
	StartStandalone = umpire.Action{
		Name: "StartNexusOperationExecution", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Effects:   []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Faultable: []string{"StartNexusOperationExecution"},
		Realize:   rpcStartStandalone{},
	}
	HandlerAsyncAck = umpire.Action{
		Name: "handler:AsyncAck", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusStart}},
		Realize:  handlerAsync{},
	}
	CallbackSucceed = umpire.Action{
		Name: "callback:Complete(ok)", Kind: umpire.CompletionCallback,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusStarted}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusSucceed}},
		Realize:  completion{},
	}
)

// StandaloneCompletion is the Phase-1 plan: create a standalone operation, acknowledge the
// start asynchronously, then complete it — unspecified→scheduled→started→succeeded.
var StandaloneCompletion = []umpire.Action{StartStandalone, HandlerAsyncAck, CallbackSucceed}
