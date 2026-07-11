package umpire

import (
	"context"
	"fmt"
	"slices"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
	"go.temporal.io/server/tests/umpire/fact"
	"go.temporal.io/server/tests/umpire/rule"
	"google.golang.org/grpc"
)

// Umpire is the property-based test monitoring system.
// It receives gRPC events and OTEL traces, routes them to entity FSMs, and
// runs pluggable verification rules that detect invariant violations.
//
// Umpire implements sdktrace.SpanProcessor so it can receive spans
// synchronously (no batching delay) and process them inline.
type Umpire struct {
	logger   log.Logger
	registry *umpirefw.Registry
	decoder  *entity.FactDecoder
	rulebook *umpirefw.Rulebook
	factLog  *umpirefw.FactLog
}

// NewUmpire creates a new Umpire with all default rules registered.
func NewUmpire(logger log.Logger) (*Umpire, error) {
	if logger == nil {
		panic("logger is required")
	}

	registry := umpirefw.NewRegistry()
	entity.RegisterDefaultEntities(registry)

	decoder := entity.NewFactDecoder()
	el := umpirefw.NewFactLog()
	rb := umpirefw.NewRulebook()

	// Safety rules — checked on every observation.
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.SpeculativeTaskCreation{} })
	// (state/timestamp consistency is now structural: WorkflowUpdate's *At accessors
	// are derived from the lifecycle's entry times, so they cannot drift.)
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.WorkflowUpdateHistoryOrdering{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.WorkflowUpdateClosure{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.WorkflowUpdateStageMonotone{} })

	// Liveness rules — checked at test teardown.
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowTaskStarvation{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.SpeculativeTaskRollback{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.SpeculativeConversion{} })
	// EntityProgress subsumes the former WorkflowUpdateLossPrevention (stuck admitted)
	// and WorkflowUpdateCompletion (stuck accepted) via the update's MustProgress states.
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.EntityProgress{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowUpdateDeduplication{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowUpdateContinueAsNew{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowUpdateWorkerSkipped{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowUpdateContextClear{} })

	// rule.EntityTransitionLegality is a generic safety rule over any Lifecycled
	// entity (it would subsume WorkflowUpdateStageMonotone). Left UNregistered for
	// now: "illegal transition" over-captures benign races (e.g. a duplicate
	// accepted span), which would false-positive under enforcement. Enable it once
	// event-time ordering makes illegal transitions unambiguous (UMPIRE_PLAN.md).

	if err := rb.InitRules(registry, logger, umpirefw.RuleConfig{}); err != nil {
		return nil, fmt.Errorf("umpire: failed to initialize rules: %w", err)
	}

	safety, liveness := rb.RuleCount()
	logger.Info("umpire initialized",
		tag.NewInt("safetyRules", safety),
		tag.NewInt("livenessRules", liveness),
	)

	u := &Umpire{
		logger:   logger,
		registry: registry,
		decoder:  decoder,
		rulebook: rb,
		factLog:  el,
	}

	return u, nil
}

var _ sdktrace.SpanProcessor = (*Umpire)(nil)

// OnStart is a no-op; we only care about completed spans.
func (u *Umpire) OnStart(_ context.Context, _ sdktrace.ReadWriteSpan) {}

// OnEnd receives completed spans synchronously and routes them to entities.
func (u *Umpire) OnEnd(span sdktrace.ReadOnlySpan) {
	events := u.decoder.ImportSpan(span)
	if len(events) == 0 {
		return
	}
	if err := u.registry.RouteFacts(context.Background(), events); err != nil {
		u.logger.Warn("umpire: failed to route OTEL events", tag.Error(err))
	}
}

// ForceFlush is a no-op; spans are processed synchronously.
func (u *Umpire) ForceFlush(_ context.Context) error {
	return nil
}

// RecordFact converts a gRPC request to an event, adds it to the event log,
// and routes it to entities.
func (u *Umpire) RecordFact(ctx context.Context, request any) {
	ev := u.decoder.ImportRequest(request)
	if ev == nil {
		return
	}
	u.factLog.Add(ev)
	if err := u.registry.RouteFacts(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("umpire: failed to route gRPC event", tag.Error(err))
	}
}

// RecordResponse converts a gRPC response to an event (if any) and routes it.
func (u *Umpire) RecordResponse(ctx context.Context, req, resp any) {
	ev := u.decoder.ImportResponse(req, resp)
	if ev == nil {
		return
	}
	u.factLog.Add(ev)
	if err := u.registry.RouteFacts(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("umpire: failed to route response event", tag.Error(err))
	}
}

// Check runs all rules and returns detected violations. When final is
// true, liveness rules promote pending items to violations.
func (u *Umpire) Check(ctx context.Context, final ...bool) []any {
	f := len(final) > 0 && final[0]
	if f {
		// At teardown, broadcast WorkflowTerminated for all workflows so
		// child entities (WorkflowTask) settle to terminal state.
		u.settleWorkflows(ctx, nil)
	}
	violations := u.rulebook.Check(ctx, f, nil)
	result := make([]any, len(violations))
	for i, v := range violations {
		result[i] = v
	}
	return result
}

// CheckNamespace runs a final check scoped to a single namespace: only entities
// rooted at that namespace are evaluated, and their unresolved liveness
// conditions are promoted to violations. Use it to validate one test's namespace
// at teardown, then PurgeNamespace to drop the collected data.
func (u *Umpire) CheckNamespace(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	u.settleWorkflows(ctx, &root)
	return u.rulebook.Check(ctx, true, &root)
}

// PurgeNamespace removes all entities, facts, and rule state collected for the
// given namespace, so a shared umpire carries nothing between tests.
func (u *Umpire) PurgeNamespace(namespaceID string) {
	root := u.namespaceRoot(namespaceID)
	u.registry.PurgeScope(root)
	u.factLog.PurgeScope(root)
	u.rulebook.PurgeScope(root)
}

func (u *Umpire) namespaceRoot(namespaceID string) umpirefw.EntityID {
	return umpirefw.NewEntityID(entity.NamespaceType, namespaceID)
}

// settleWorkflows broadcasts WorkflowTerminated facts for each unique
// (namespace, workflowID) seen across WorkflowTask entities, allowing them to
// transition to terminal states at test teardown. When scope is non-nil, only
// tasks in that namespace are settled. The terminated facts carry the namespace
// so tasks in other namespaces ignore the broadcast.
func (u *Umpire) settleWorkflows(ctx context.Context, scope *umpirefw.EntityID) {
	seen := make(map[string]bool)
	var facts []umpirefw.Fact
	for _, e := range u.registry.QueryEntities(entity.WorkflowTaskType, 0, scope) {
		if wt, ok := e.Entity.(*entity.WorkflowTask); ok && wt.WorkflowID != "" {
			key := wt.NamespaceID + "/" + wt.WorkflowID
			if !seen[key] {
				seen[key] = true
				facts = append(facts, &fact.WorkflowTerminated{
					WorkflowID:  wt.WorkflowID,
					NamespaceID: wt.NamespaceID,
				})
			}
		}
	}
	if len(facts) > 0 {
		_ = u.registry.RouteFacts(ctx, facts)
	}
}

// Reset clears state between tests.
func (u *Umpire) Reset() {
	// No-op for now; state is stateless across checks.
}

// FactLog returns the event log for querying events in tests.
func (u *Umpire) FactLog() *umpirefw.FactLog {
	return u.factLog
}

// Registry returns the entity registry for querying entities in tests.
func (u *Umpire) Registry() *umpirefw.Registry {
	return u.registry
}

// RuleStats returns per-rule evaluation statistics.
func (u *Umpire) RuleStats() []umpirefw.RuleStats {
	return u.rulebook.Stats()
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (u *Umpire) PassedKeys(ruleName string) []string {
	return u.rulebook.PassedKeys(ruleName)
}

// RequireRulePassed asserts that the given rule evaluated the entity identified
// by entityKey and found no violation. Fails the test if the key is not found
// in the rule's passed keys.
func (u *Umpire) RequireRulePassed(t testing.TB, rule interface{ Name() string }, entityKey string) {
	t.Helper()
	name := rule.Name()
	passed := u.rulebook.PassedKeys(name)
	if !slices.Contains(passed, entityKey) {
		t.Errorf("rule %s did not pass entity %q; passed keys: %v", name, entityKey, passed)
	}
}

// Shutdown cleanly shuts down all Umpire components.
func (u *Umpire) Shutdown(_ context.Context) error {
	u.logger.Info("umpire closed")
	return nil
}

// NewUnaryServerInterceptor returns a gRPC interceptor that records events via u
// and optionally injects faults via inj. Either may be nil.
func NewUnaryServerInterceptor(u *Umpire, inj umpirefw.FaultInjector) grpc.UnaryServerInterceptor {
	return umpirefw.NewUnaryServerInterceptor(u, inj)
}
