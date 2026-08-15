package umpire1

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/grpcadapter"
	"go.temporal.io/server/tests/umpire1/model"
	"go.temporal.io/server/tests/umpire1/rule"
	"google.golang.org/grpc"
)

// Monitor is the property-based test monitoring system.
// It receives gRPC events and OTEL traces, routes them to entity FSMs, and
// runs pluggable verification rules that detect invariant violations.
//
// Monitor implements sdktrace.SpanProcessor so it can receive spans
// synchronously (no batching delay) and process them inline.
type Monitor struct {
	logger   log.Logger
	registry *umpirefw.ModelState
	decoder  *model.FactDecoder
	rulebook *umpirefw.RuleRegistry
	factLog  *umpirefw.FactLog

	// nsIDByName resolves a namespace name (all a frontend request carries) to the id the model
	// scopes entities by. A synchronous rejection produces no telemetry, so its fact must be
	// namespace-id-rooted from the request alone; the driver seeds this map (it knows both) before
	// driving. See SetNamespaceID and UMPIRE.md.
	nsMu       sync.RWMutex
	nsIDByName map[string]string
}

// NewMonitor creates a new Monitor with all default rules registered.
func NewMonitor(logger log.Logger) (*Monitor, error) {
	if logger == nil {
		panic("logger is required")
	}

	registry := umpirefw.NewModelState()
	model.RegisterDefaultEntities(registry)

	decoder := model.NewFactDecoder()
	el := umpirefw.NewFactLog()
	rb := umpirefw.NewRuleRegistry()

	// Safety rules — checked on every observation.
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.SpeculativeTaskCreation{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.NexusOperationClosure{} })
	// Illegal-transition conformance is not registered as a rule: it is a built-in
	// framework check (RuleRegistry.Check → checkConformance) that surfaces, for every
	// Lifecycled entity, the illegal transitions Lifecycle.Fire records at fire-time —
	// the model judging its own transitions. Classify tolerates forward jumps over reachable
	// states because an unobserved intermediate is indistinguishable from a skipped transition;
	// events outside that reachable path remain illegal.

	// Liveness rules — checked at test teardown.
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowTaskStarvation{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.EntityProgress{} })

	if err := rb.InitRules(registry, umpirefw.RuleConfig{}); err != nil {
		return nil, fmt.Errorf("monitor: failed to initialize rules: %w", err)
	}

	safety, liveness := rb.RuleCount()
	logger.Info("monitor initialized",
		tag.NewInt("safetyRules", safety),
		tag.NewInt("livenessRules", liveness),
	)

	u := &Monitor{
		logger:     logger,
		registry:   registry,
		decoder:    decoder,
		rulebook:   rb,
		factLog:    el,
		nsIDByName: map[string]string{},
	}

	return u, nil
}

// SetNamespaceID records a namespace name→id mapping so a synchronous rejection (which carries only
// the name) can be routed into the id-scoped model. The driver seeds it before driving; idempotent.
func (u *Monitor) SetNamespaceID(name, id string) {
	if name == "" || id == "" {
		return
	}
	u.nsMu.Lock()
	defer u.nsMu.Unlock()
	u.nsIDByName[name] = id
}

func (u *Monitor) resolveNamespaceID(name string) string {
	u.nsMu.RLock()
	defer u.nsMu.RUnlock()
	return u.nsIDByName[name]
}

var _ sdktrace.SpanProcessor = (*Monitor)(nil)

// OnStart is a no-op; we only care about completed spans.
func (u *Monitor) OnStart(_ context.Context, _ sdktrace.ReadWriteSpan) {}

// OnEnd receives completed spans synchronously and routes them to entities.
func (u *Monitor) OnEnd(span sdktrace.ReadOnlySpan) {
	facts := u.decoder.ImportSpan(span)
	if len(facts) == 0 {
		return
	}
	u.factLog.AddAll(facts)
	if err := u.registry.RouteFacts(context.Background(), facts); err != nil {
		u.logger.Warn("monitor: failed to route OTEL facts", tag.Error(err))
	}
}

// ForceFlush is a no-op; spans are processed synchronously.
func (u *Monitor) ForceFlush(_ context.Context) error {
	return nil
}

// RecordFact converts a gRPC request to an event, adds it to the event log,
// and routes it to entities.
func (u *Monitor) RecordFact(ctx context.Context, request any) {
	ev := u.decoder.ImportRequest(request)
	if ev == nil {
		return
	}
	u.factLog.Add(ev)
	if err := u.registry.RouteFacts(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("monitor: failed to route gRPC event", tag.Error(err))
	}
}

// RecordResponse converts a gRPC response to an event (if any) and routes it.
func (u *Monitor) RecordResponse(ctx context.Context, req, resp any) {
	ev := u.decoder.ImportResponse(req, resp)
	if ev == nil {
		return
	}
	u.factLog.Add(ev)
	if err := u.registry.RouteFacts(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("monitor: failed to route response event", tag.Error(err))
	}
}

// RecordRejection converts a rejected gRPC request (request + error) to a fact and routes it. The
// request carries only the namespace name, so the id the model scopes by is resolved from the map
// the driver seeded (SetNamespaceID); an unknown namespace or an unmodeled rejection is dropped.
func (u *Monitor) RecordRejection(ctx context.Context, req any, err error) {
	named, ok := req.(interface{ GetNamespace() string })
	if !ok {
		return
	}
	nsID := u.resolveNamespaceID(named.GetNamespace())
	if nsID == "" {
		return
	}
	ev := u.decoder.ImportRejection(req, err, nsID)
	if ev == nil {
		return
	}
	u.factLog.Add(ev)
	if routeErr := u.registry.RouteFacts(ctx, []umpirefw.Fact{ev}); routeErr != nil {
		u.logger.Warn("monitor: failed to route rejection event", tag.Error(routeErr))
	}
}

// CheckNamespace runs a final check scoped to a single namespace: only entities
// rooted at that namespace are evaluated, and their unresolved liveness
// conditions are promoted to violations. Use it to validate one test's namespace
// at teardown, then PurgeNamespace to drop the collected data.
func (u *Monitor) CheckNamespace(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	return u.check(ctx, root, true)
}

// CheckNamespaceSafety applies the global rulebook without promoting pending liveness obligations.
func (u *Monitor) CheckNamespaceSafety(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	return u.check(ctx, root, false)
}

func (u *Monitor) check(ctx context.Context, root umpirefw.EntityID, final bool) []umpirefw.Violation {
	violations := u.rulebook.Check(ctx, final, &root)
	for _, violation := range violations {
		tags := []tag.Tag{tag.NewStringTag("rule", violation.Rule)}
		for key, value := range violation.Tags {
			tags = append(tags, tag.NewStringTag(key, value))
		}
		u.logger.Warn("violation: "+violation.Message, tags...)
	}
	return violations
}

// PurgeNamespace removes all entities, facts, and rule state collected for the
// given namespace, so a shared monitor carries nothing between tests.
func (u *Monitor) PurgeNamespace(namespaceID string) {
	root := u.namespaceRoot(namespaceID)
	u.registry.PurgeScope(root)
	u.factLog.PurgeScope(root)
	u.rulebook.PurgeScope(root)
}

func (u *Monitor) namespaceRoot(namespaceID string) umpirefw.EntityID {
	return umpirefw.NewEntityID(model.NamespaceType, namespaceID)
}

// Snapshot returns a defensive semantic view of one namespace.
func (u *Monitor) Snapshot(namespaceID string) umpirefw.Snapshot {
	root := u.namespaceRoot(namespaceID)
	entries := u.registry.QueryAll(0, &root)
	slices.SortFunc(entries, func(left, right umpirefw.EntityEntry) int {
		return cmp.Compare(left.Key, right.Key)
	})
	entities := make([]umpirefw.EntitySnapshot, 0, len(entries))
	for _, entry := range entries {
		leaf := entry.Key[strings.LastIndex(entry.Key, "@")+1:]
		entity := umpirefw.EntitySnapshot{
			Key:  entry.Key,
			Type: entry.Entity.Type(),
			ID:   strings.TrimPrefix(leaf, string(entry.Entity.Type())+":"),
		}
		if lifecycled, ok := entry.Entity.(umpirefw.Lifecycled); ok {
			lifecycle := lifecycled.Lifecycle()
			entity.Current = lifecycle.Current()
			entity.Terminal = lifecycle.IsTerminal()
			entity.Disposition = lifecycle.CurrentDisposition()
			entity.Visited = lifecycle.VisitedEdges()
		}
		switch value := entry.Entity.(type) {
		case *model.NexusOperation:
			if value.WorkflowID != "" {
				entity.ID = value.WorkflowID
			}
		case *model.Workflow:
			if value.WorkflowID != "" {
				entity.ID = value.WorkflowID
			}
		case *model.WorkflowRun:
			if value.RunID != "" {
				entity.ID = value.RunID
			}
			entity.RootID = value.FirstRunID
			entity.PredecessorID = value.PreviousRunID
			entity.Initiator = value.Initiator
		}
		entities = append(entities, entity)
	}
	facts := u.factLog.QueryByID(root)
	factSnapshots := make([]umpirefw.FactSnapshot, len(facts))
	for i, observed := range facts {
		factSnapshots[i] = umpirefw.FactSnapshot{Name: observed.Name()}
	}
	return umpirefw.Snapshot{
		Generation: u.registry.Generation(),
		Entities:   entities,
		Facts:      factSnapshots,
	}
}

// Observed reports whether a protocol-level semantic observation occurred in one namespace.
func (u *Monitor) Observed(string, umpirefw.ObservationQuery) bool {
	return false
}

// ArtifactFacts returns normalized JSON evidence for the facts observed in one namespace.
func (u *Monitor) ArtifactFacts(namespaceID string) ([]json.RawMessage, error) {
	root := u.namespaceRoot(namespaceID)
	facts := u.factLog.QueryByID(root)
	result := make([]json.RawMessage, 0, len(facts))
	for _, observed := range facts {
		payload, err := json.Marshal(observed)
		if err != nil {
			return nil, fmt.Errorf("encode observed fact %s: %w", observed.Name(), err)
		}
		encoded, err := json.Marshal(struct {
			Name    string               `json:"name"`
			Target  *umpirefw.EntityPath `json:"target,omitempty"`
			Payload json.RawMessage      `json:"payload"`
		}{
			Name:    observed.Name(),
			Target:  observed.TargetEntity(),
			Payload: payload,
		})
		if err != nil {
			return nil, fmt.Errorf("encode observed fact artifact %s: %w", observed.Name(), err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

// ObservationSummary returns a compact diagnostic of the namespace evidence.
func (u *Monitor) ObservationSummary(namespaceID string) string {
	snapshot := u.Snapshot(namespaceID)
	return fmt.Sprintf("facts=%v entities=%v", snapshot.FactNames(), snapshot.Entities)
}

// FactLog returns the event log for querying events in tests.
func (u *Monitor) FactLog() *umpirefw.FactLog {
	return u.factLog
}

// ModelState returns the entity registry for querying entities in tests.
func (u *Monitor) ModelState() *umpirefw.ModelState {
	return u.registry
}

// RuleStats returns per-rule evaluation statistics.
func (u *Monitor) RuleStats() []umpirefw.RuleStats {
	return u.rulebook.Stats()
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (u *Monitor) PassedKeys(ruleName string) []string {
	return u.rulebook.PassedKeys(ruleName)
}

// Shutdown cleanly shuts down all Monitor components.
func (u *Monitor) Shutdown(_ context.Context) error {
	u.logger.Info("monitor closed")
	return nil
}

// UnaryServerInterceptor returns a gRPC interceptor that records events via u
// and optionally injects faults via inj. Either may be nil.
func (u *Monitor) UnaryServerInterceptor(inj umpirefw.FaultInjector) grpc.UnaryServerInterceptor {
	return grpcadapter.NewUnaryServerInterceptor(u, inj)
}
