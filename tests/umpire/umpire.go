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
	"go.temporal.io/server/tests/umpire/rulebook"
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
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rulebook.SpeculativeTaskCreationRule{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rulebook.WorkflowUpdateStateConsistencyRule{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rulebook.WorkflowUpdateHistoryOrderingRule{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rulebook.WorkflowUpdateClosureRule{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rulebook.WorkflowUpdateStageMonotoneRule{} })

	// Liveness rules — checked at test teardown.
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowTaskStarvationRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.SpeculativeTaskRollbackRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.SpeculativeConversionRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateLossPreventionRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateCompletionRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateDeduplicationRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateDedupProgressRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateContinueAsNewRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateWorkerSkippedRule{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rulebook.WorkflowUpdateContextClearRule{} })

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
		u.settleWorkflows(ctx)
	}
	violations := u.rulebook.Check(ctx, f)
	result := make([]any, len(violations))
	for i, v := range violations {
		result[i] = v
	}
	return result
}

// settleWorkflows broadcasts WorkflowTerminated facts for each unique
// workflowID seen across WorkflowTask entities, allowing them to transition
// to terminal states at test teardown.
func (u *Umpire) settleWorkflows(ctx context.Context) {
	seen := make(map[string]bool)
	var facts []umpirefw.Fact
	for _, e := range u.registry.QueryEntities(entity.WorkflowTaskType, 0) {
		if wt, ok := e.Entity.(*entity.WorkflowTask); ok && wt.WorkflowID != "" {
			if !seen[wt.WorkflowID] {
				seen[wt.WorkflowID] = true
				facts = append(facts, &fact.WorkflowTerminated{
					WorkflowID: wt.WorkflowID,
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
