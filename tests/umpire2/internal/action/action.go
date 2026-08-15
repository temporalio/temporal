// Package action holds the Temporal-specific Nexus-operation actions and their realizers,
// plus the runtime glue (RealizeContext, StateOracle, a programmable mock handler) over a live
// test env. The generic action schema and Drive loop live in common/testing/umpire. See UMPIRE.md.
package action

import (
	"context"
	"sync"

	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// Ctx is the Temporal-side umpire.RealizeContext: the live env, the resolved mock endpoint, the
// programmable handler, and the running Var→identity bindings. Realizers type-assert the
// umpire.RealizeContext they are handed back to *Ctx.
type Ctx struct {
	Env      Environment
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
func NewCtx(env Environment, endpoint string, h *ResponsePolicy, iter int) *Ctx {
	monitor := env.GetMonitor()
	monitor.SetNamespaceID(env.Namespace().String(), env.NamespaceID().String())
	if observer, ok := monitor.(factObserver); ok {
		h.setFactObserver(env.NamespaceID().String(), observer)
	}
	return &Ctx{Env: env, Endpoint: endpoint, Handler: h, Iter: iter, bind: map[string]string{}, rejects: map[string]error{}}
}

type factObserver interface {
	ObserveFact(context.Context, umpire.Fact) error
}

// ObserveExecution forwards neutral runtime observations to the owning monitor.
func (c *Ctx) ObserveExecution(ctx context.Context, observed umpire.ExecutionObservation) error {
	observer, ok := c.Env.GetMonitor().(umpire.ExecutionObserver)
	if !ok {
		return nil
	}
	observed.Scope = c.Env.NamespaceID().String()
	return observer.ObserveExecution(ctx, observed)
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
type Oracle struct{ Env Environment }

func (o Oracle) Current(t umpire.EntityType, id string) (string, bool) {
	if entity, ok := o.find(t, id); ok {
		return entity.Current, true
	}
	return "", false
}

// Visited implements umpire.VisitedOracle for reconciliation.
func (o Oracle) Visited(t umpire.EntityType, id string) ([]umpire.Edge, bool) {
	if entity, ok := o.find(t, id); ok {
		return entity.Visited, true
	}
	return nil, false
}

func (o Oracle) find(t umpire.EntityType, id string) (umpire.EntitySnapshot, bool) {
	for _, entity := range o.Env.GetMonitor().Snapshot(o.Env.NamespaceID().String()).EntitiesOfType(t) {
		if entity.ID != id {
			continue
		}
		return entity, true
	}
	return umpire.EntitySnapshot{}, false
}

// Successor implements umpire.LineageOracle: the run the given run produced (continue-as-new /
// reset / retry), found by its observed predecessor link. Lets Drive bind a LinkedFrom ref by
// observation — the driver never needs the server-minted successor RunID (see UMPIRE.md).
func (o Oracle) Successor(t umpire.EntityType, predecessorID string) (string, bool) {
	for _, entity := range o.Env.GetMonitor().Snapshot(o.Env.NamespaceID().String()).EntitiesOfType(t) {
		if entity.PredecessorID == predecessorID {
			return entity.ID, true
		}
	}
	return "", false
}

// Resolver implements umpire.EffectResolver over the default entity lifecycles.
type Resolver struct{}

func (Resolver) Destination(t umpire.EntityType, event string) (string, bool) {
	var lifecycle *umpire.Lifecycle
	switch t {
	case model.NexusOperationType:
		lifecycle = model.NewNexusOperation().Lifecycle()
	case model.WorkflowType:
		lifecycle = model.NewWorkflow().Lifecycle()
	case model.WorkflowRunType:
		lifecycle = model.NewWorkflowRun().Lifecycle()
	default:
		return "", false
	}
	return lifecycle.Destination(event)
}
