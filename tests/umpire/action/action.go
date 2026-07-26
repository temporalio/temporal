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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
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
	RunID    string // the started operation's run id, captured by the start action for terminate

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
	block    bool // hold the start attempt (keeps the operation scheduled) until ctx is done
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
		OnStartOperation: func(hctx context.Context, _, _ string, _ *nexus.LazyValue, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case p.captured <- callback{opts.CallbackURL, opts.CallbackHeader.Get(commonnexus.CallbackTokenHeader)}:
			default: // already captured (a retry); keep the first
			}
			p.mu.Lock()
			r, err, block := p.onStart, p.startErr, p.block
			p.mu.Unlock()
			if block {
				<-hctx.Done() // hold the attempt so the operation stays scheduled
				return nil, hctx.Err()
			}
			return r, err
		},
		OnCancelOperation: func(_ context.Context, _, _, _ string, _ nexus.CancelOperationOptions) error { return nil },
	}
}

func (p *ResponsePolicy) setStart(r nexus.HandlerStartOperationResult[any], err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStart, p.startErr = r, err
}

func (p *ResponsePolicy) setBlock() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.block = true
}

// ---- Realizers ----

// rpcStartStandalone realizes StartNexusOperationExecution: creates a standalone operation
// (its own execution) and binds it — unspecified→scheduled.
type rpcStartStandalone struct{}

func (rpcStartStandalone) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (rpcStartStandalone) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	opID := fmt.Sprintf("umpire-action-op-%d", c.Iter)
	resp, err := c.Env.FrontendClient().StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              c.Env.Namespace().String(),
		OperationId:            opID,
		Endpoint:               c.Endpoint,
		Service:                "service",
		Operation:              "operation",
		RequestId:              opID,
		ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
	})
	if err != nil {
		return err
	}
	c.RunID = resp.GetRunId()
	bindFresh(rc, a, opID) // op identity == its execution id (== WorkflowID in telemetry)
	return nil
}

// cmdSchedule realizes cmd:ScheduleNexusOperation via a caller workflow that issues
// ExecuteOperation — embedded scheduling (the operation is a child of the workflow). Non-
// blocking: it starts the workflow (which then blocks on the operation) and binds the operation
// under its caller workflow id — the id its chasm.transition telemetry carries — so later
// actions (handler, callback) drive its outcome. This is the worker-behavior layer: the SDK
// worker turns "produce a ScheduleNexusOperation command" into a real WFT command.
type cmdSchedule struct{}

func (cmdSchedule) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (cmdSchedule) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	wfID := fmt.Sprintf("umpire-action-caller-%d", c.Iter)
	endpoint := c.Endpoint
	caller := func(wctx workflow.Context) error {
		return workflow.NewNexusClient(endpoint, "service").
			ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{}).Get(wctx, nil)
	}
	c.Env.SdkWorker().RegisterWorkflow(caller)
	if _, err := c.Env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: c.Env.WorkerTaskQueue(),
	}, caller); err != nil {
		return err
	}
	bindFresh(rc, a, wfID)
	return nil
}

// handlerBlock holds the start attempt so the operation stays scheduled (no effect of its
// own; a companion action, e.g. terminate, acts while it is held). Reactive.
type handlerBlock struct{}

func (handlerBlock) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setBlock()
	return nil
}
func (handlerBlock) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerRetryable fails the start attempt retryably, sending the operation into backoff —
// scheduled→backing_off. Reactive.
type handlerRetryable struct{}

func (handlerRetryable) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire action: injected retryable failure"))
	return nil
}
func (handlerRetryable) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// rpcTerminate realizes TerminateNexusOperationExecution on the bound standalone operation.
type rpcTerminate struct{}

func (rpcTerminate) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (rpcTerminate) Fire(ctx context.Context, rc umpire.RealizeContext, _ umpire.Action) error {
	c := rc.(*Ctx)
	opID, _ := rc.Binding("op")
	_, err := c.Env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   c.Env.Namespace().String(),
		OperationId: opID,
		RunId:       c.RunID,
		Reason:      "umpire action: reach terminated",
	})
	return err
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
	CallbackSucceed = CompleteWith(nil, model.NexusSucceed)

	// ScheduleEmbedded creates the operation inside a caller workflow (embedded hosting).
	ScheduleEmbedded = umpire.Action{
		Name: "cmd:ScheduleNexusOperation", Kind: umpire.WorkerCommand, Hosting: umpire.Embedded,
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Realize: cmdSchedule{},
	}
)

// CompleteWith delivers an async completion (opErr nil = success) to a started operation,
// firing `event` (succeed/fail/cancel). Used by both hostings.
func CompleteWith(opErr *nexus.OperationError, event string) umpire.Action {
	return umpire.Action{
		Name: "callback:Complete", Kind: umpire.CompletionCallback,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusStarted}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: event}},
		Realize:  completion{opErr: opErr},
	}
}

// EmbeddedSucceed / EmbeddedFail / EmbeddedCancel are the embedded async-completion plans:
// schedule the operation via a caller workflow, async-ack the start, then complete it —
// started --> {succeeded, failed, canceled}.
func EmbeddedSucceed() []umpire.Action {
	return []umpire.Action{ScheduleEmbedded, HandlerAsyncAck, CompleteWith(nil, model.NexusSucceed)}
}

func EmbeddedFail() []umpire.Action {
	return []umpire.Action{ScheduleEmbedded, HandlerAsyncAck, CompleteWith(nexus.NewOperationFailedError("umpire action: injected async failure"), model.NexusFail)}
}

func EmbeddedCancel() []umpire.Action {
	return []umpire.Action{ScheduleEmbedded, HandlerAsyncAck, CompleteWith(nexus.NewOperationCanceledError("umpire action: injected async cancellation"), model.NexusCancel)}
}

// StandaloneCompletion is the Phase-1 plan: create a standalone operation, acknowledge the
// start asynchronously, then complete it — unspecified→scheduled→started→succeeded.
var StandaloneCompletion = []umpire.Action{StartStandalone, HandlerAsyncAck, CallbackSucceed}

// HandlerBlock holds the start attempt so the operation stays scheduled.
var HandlerBlock = umpire.Action{
	Name: "handler:Block", Kind: umpire.HandlerResponse,
	Realize: handlerBlock{},
}

// HandlerRetryable sends the operation into backoff: scheduled→backing_off.
var HandlerRetryable = umpire.Action{
	Name: "handler:RetryableError", Kind: umpire.HandlerResponse,
	Effects: []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusAttemptFailed}},
	Realize: handlerRetryable{},
}

// TerminateFrom is the terminate action gated on the operation being in `state` — the
// precondition is what pins which edge (state --terminate--> terminated) the plan exercises.
// It is Standalone-only (an embedded operation has no terminate RPC).
func TerminateFrom(state string) umpire.Action {
	return umpire.Action{
		Name: "TerminateNexusOperationExecution", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Requires:  []umpire.Pre{{Ref: nexusOp("op", false), State: state}},
		Effects:   []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusTerminate}},
		Faultable: []string{"TerminateNexusOperationExecution"},
		Realize:   rpcTerminate{},
	}
}

// StandaloneTerminate is the plan that reaches state --terminate--> terminated: start a
// standalone operation, drive it into `state` (block/retry/async), then terminate it.
func StandaloneTerminate(state string) []umpire.Action {
	drive := HandlerBlock // scheduled: hold the attempt
	switch state {
	case model.NexusBackingOff:
		drive = HandlerRetryable
	case model.NexusStarted:
		drive = HandlerAsyncAck
	}
	return []umpire.Action{StartStandalone, drive, TerminateFrom(state)}
}
