// Package action holds the Temporal-specific Nexus-operation actions and their realizers,
// plus the runtime glue (RealizeContext, StateOracle, a programmable mock handler) over a live
// test env. The generic action schema and Drive loop live in common/testing/umpire. See UMPIRE.md.
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
	"go.temporal.io/server/common/testing/testhooks"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire1/model"
	"go.temporal.io/server/tests/umpire1/planner"
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

	mu       sync.Mutex
	bind     map[string]string
	rejects  map[string]error // action name -> captured synchronous rejection (nil = fired ok)
	cleanups []func()         // e.g. fault unregistration, run on Cleanup
}

// addCleanup registers a function to run when the drive finishes (see Cleanup).
func (c *Ctx) addCleanup(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups = append(c.cleanups, f)
}

// Cleanup runs the registered cleanups (unregistering faults, etc.). Call it after Drive.
func (c *Ctx) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.cleanups {
		f()
	}
	c.cleanups = nil
}

// NewCtx builds a RealizeContext for one drive. It seeds the Monitor's namespace name→id map: a
// synchronous rejection carries only the name, but the driver knows both, so this lets the observer
// route the rejection fact into the id-scoped model (see UMPIRE.md, Monitor.SetNamespaceID).
func NewCtx(env *testcore.TestEnv, endpoint string, h *ResponsePolicy, iter int) *Ctx {
	env.GetMonitor().SetNamespaceID(env.Namespace().String(), env.NamespaceID().String())
	return &Ctx{Env: env, Endpoint: endpoint, Handler: h, Iter: iter, bind: map[string]string{}, rejects: map[string]error{}}
}

// ObserveReject implements umpire.RejectSink: it captures the synchronous outcome of an action
// declared to be rejected (a non-nil err is the rejection; nil means the RPC was accepted).
// RejectionDrift judges these against the generic rejection contract.
func (c *Ctx) ObserveReject(action string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rejects[action] = err
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

// bindFresh binds every Fresh effect var of a to id — except LinkedFrom refs, which are bound by
// observation (to a predecessor's successor), not to the realizer's id.
func bindFresh(rc umpire.RealizeContext, a umpire.Action, id string) {
	for _, e := range a.Effects {
		if e.Ref.Fresh && e.Ref.LinkedFrom == "" {
			rc.Bind(e.Ref.Var, id)
		}
	}
}

// Oracle implements umpire.StateOracle / VisitedOracle over a live env's Monitor model. It is
// entity-agnostic: it finds the entity of type t whose identity matches id (see entityID) and reads
// its Lifecycle, so it serves NexusOperation and Workflow (and any future Lifecycled entity) alike.
type Oracle struct{ Env *testcore.TestEnv }

func (o Oracle) Current(t umpire.EntityType, id string) (string, bool) {
	if lc := o.find(t, id); lc != nil {
		return lc.Current(), true
	}
	return "", false
}

// Visited implements umpire.VisitedOracle for reconciliation.
func (o Oracle) Visited(t umpire.EntityType, id string) ([]umpire.Edge, bool) {
	if lc := o.find(t, id); lc != nil {
		return lc.VisitedEdges(), true
	}
	return nil, false
}

func (o Oracle) find(t umpire.EntityType, id string) *umpire.Lifecycle {
	nsRoot := umpire.NewEntityID(model.NamespaceType, o.Env.NamespaceID().String())
	for _, e := range o.Env.GetMonitor().ModelState().QueryEntities(t, 0, &nsRoot) {
		if entityID(e.Entity) != id {
			continue
		}
		if lced, ok := e.Entity.(umpire.Lifecycled); ok {
			return lced.Lifecycle()
		}
	}
	return nil
}

// entityID returns the identity an action binds an entity to — its captured WorkflowID (the
// operation's execution id for a standalone op, the caller workflow id for an embedded one; the
// workflow id for a Workflow). Per-type because the identity field is not a shared method.
func entityID(e umpire.Entity) string {
	switch x := e.(type) {
	case *model.NexusOperation:
		return x.WorkflowID
	case *model.Workflow:
		return x.WorkflowID
	case *model.WorkflowRun:
		return x.RunID
	}
	return ""
}

// Successor implements umpire.LineageOracle: the run the given run produced (continue-as-new /
// reset / retry), found by its observed predecessor link. Lets Drive bind a LinkedFrom ref by
// observation — the driver never needs the server-minted successor RunID (see UMPIRE.md).
func (o Oracle) Successor(t umpire.EntityType, predecessorID string) (string, bool) {
	nsRoot := umpire.NewEntityID(model.NamespaceType, o.Env.NamespaceID().String())
	for _, e := range o.Env.GetMonitor().ModelState().QueryEntities(t, 0, &nsRoot) {
		if r, ok := e.Entity.(*model.WorkflowRun); ok && r.PreviousRunID == predecessorID {
			return r.RunID, true
		}
	}
	return "", false
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

// handlerSyncOk returns a synchronous success from the handler — scheduled→succeeded. Reactive.
type handlerSyncOk struct{}

func (handlerSyncOk) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(&nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil)
	return nil
}
func (handlerSyncOk) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerOpFailed fails the operation from the handler — scheduled→failed. Reactive.
type handlerOpFailed struct{}

func (handlerOpFailed) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewOperationFailedError("umpire action: injected operation failure"))
	return nil
}
func (handlerOpFailed) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerOpCanceled reports the operation canceled from the handler — scheduled→canceled.
// Reactive.
type handlerOpCanceled struct{}

func (handlerOpCanceled) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewOperationCanceledError("umpire action: injected cancellation"))
	return nil
}
func (handlerOpCanceled) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return nil
}

// timerForceTimeout installs the NexusOperationForceTimeout hook so the operation times out
// from `from` (scheduled or backing_off) deterministically, no real timer wait. Reactive.
type timerForceTimeout struct{ from string }

func (t timerForceTimeout) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Env.InjectHook(testhooks.NewHook(testhooks.NexusOperationForceTimeout, t.from))
	return nil
}
func (timerForceTimeout) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return nil
}

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
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Entry:   []string{"StartNexusOperationExecution"},
		// The internal calls a standalone start triggers: the CHASM operation task and the outbound
		// Nexus HTTP invocation to the handler (service/operation of the mock endpoint). Learned via
		// LearnFootprint; declared here so ReconcileFootprint catches wire-level drift.
		Footprint: []string{"StartNexusOperation", "HTTP POST /service/operation"},
		Realize:   rpcStartStandalone{},
	}
	HandlerAsyncAck = umpire.Action{
		Name: "handler:AsyncAck", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusStart}},
		Realize:  handlerAsync{},
	}
	CallbackSucceed = CompleteWith(nil, model.NexusSucceed)

	// ScheduleEmbedded creates the operation inside a caller workflow (embedded hosting),
	// realized by the real kitchensink interpreter (see kitchensink.go).
	ScheduleEmbedded = umpire.Action{
		Name: "cmd:ScheduleNexusOperation", Kind: umpire.WorkerCommand, Hosting: umpire.Embedded,
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Realize: kitchensink{},
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
// started --> {succeeded, failed, canceled}. Computed by the planner.
func EmbeddedSucceed() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusSucceed, umpire.Embedded)
}
func EmbeddedFail() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusFail, umpire.Embedded)
}
func EmbeddedCancel() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusCancel, umpire.Embedded)
}

// HandlerSyncOk / HandlerOpFailed settle the operation directly from the start attempt.
var (
	HandlerSyncOk = umpire.Action{
		Name: "handler:SyncOk", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusSucceed}},
		Realize:  handlerSyncOk{},
	}
	HandlerOpFailed = umpire.Action{
		Name: "handler:OpFailed", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusFail}},
		Realize:  handlerOpFailed{},
	}
	HandlerOpCanceled = umpire.Action{
		Name: "handler:OpCanceled", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusCancel}},
		Realize:  handlerOpCanceled{},
	}
)

// TimerForceTimeout fires the schedule-to-close timeout from `from` (a
// testhooks.NexusForceTimeoutFrom* value: scheduled or backing_off) — timed_out.
func TimerForceTimeout(from string) umpire.Action {
	return umpire.Action{
		Name: "timer:ForceTimeout(" + from + ")", Kind: umpire.Timer,
		Effects: []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusTimeout}},
		Realize: timerForceTimeout{from: from},
	}
}

// EmbeddedSyncSuccess / EmbeddedOpFailure / EmbeddedScheduledCancel settle the operation from
// the start attempt: scheduled --> {succeeded, failed, canceled}. Computed by the planner.
func EmbeddedSyncSuccess() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusSucceed, umpire.Embedded)
}
func EmbeddedOpFailure() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusFail, umpire.Embedded)
}
func EmbeddedScheduledCancel() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusCancel, umpire.Embedded)
}

// EmbeddedTimeoutScheduled forces the timeout on the first attempt (scheduled --> timed_out);
// EmbeddedTimeoutBackingOff first fails retryably into backoff, then times out from there
// (backing_off --> timed_out). Computed by the planner.
func EmbeddedTimeoutScheduled() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusTimeout, umpire.Embedded)
}
func EmbeddedTimeoutBackingOff() []umpire.Action {
	return mustPlan(model.NexusBackingOff, model.NexusTimeout, umpire.Embedded)
}

// StandaloneCompletion is the Phase-1 plan: create a standalone operation, acknowledge the
// start asynchronously, then complete it — unspecified→scheduled→started→succeeded.
func StandaloneCompletion() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusSucceed, umpire.Standalone)
}

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
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: state}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusTerminate}},
		Entry:    []string{"TerminateNexusOperationExecution"},
		Realize:  rpcTerminate{},
	}
}

// StandaloneTerminate is the plan that reaches state --terminate--> terminated. The route to
// `state` needs a handler that holds the operation there; the planner's actionFor picks the
// attempt outcome (async→started, retryable→backing_off), but "hold in scheduled" has no
// outcome event, so that one case is completed with HandlerBlock here.
func StandaloneTerminate(state string) []umpire.Action {
	if state == model.NexusScheduled {
		return []umpire.Action{StartStandalone, HandlerBlock, TerminateFrom(state)}
	}
	return mustPlan(state, model.NexusTerminate, umpire.Standalone)
}
