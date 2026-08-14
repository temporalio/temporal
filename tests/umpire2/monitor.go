package umpire2

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/assurance"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
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
	decoder  *model.FactDecoder
	evidence *evidenceIngestor

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
	defaultProtocol, err := protocol.Default()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to compile default protocol: %w", err)
	}
	defaultProtocol.Register(registry)
	relations, err := defaultProtocol.NewRelationStore()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to create relation store: %w", err)
	}

	decoder := model.NewFactDecoder()
	el := umpirefw.NewFactLog()
	rb := umpirefw.NewRuleRegistry()
	catalog, err := assurance.Default()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to compile assurance catalog: %w", err)
	}
	if err := catalog.Register(rb); err != nil {
		return nil, fmt.Errorf("monitor: failed to register assurance catalog: %w", err)
	}
	// Illegal-transition conformance is not registered as a rule: it is a built-in
	// framework check (RuleRegistry.Check → checkConformance) that surfaces, for every
	// Lifecycled entity, the illegal transitions Lifecycle.Fire records at fire-time —
	// the model judging its own transitions. Classify tolerates forward jumps over reachable
	// states because an unobserved intermediate is indistinguishable from a skipped transition;
	// events outside that reachable path remain illegal.

	if err := rb.InitRules(registry, logger, umpirefw.RuleConfig{Relations: relations}, catalog.Names()...); err != nil {
		return nil, fmt.Errorf("monitor: failed to initialize rules: %w", err)
	}

	safety, liveness := rb.RuleCount()
	logger.Info("monitor initialized",
		tag.NewInt("safetyRules", safety),
		tag.NewInt("livenessRules", liveness),
	)

	trace := newExecutionTrace(registry, relations, defaultProtocol.CausalFootprints())
	return &Monitor{
		logger:     logger,
		decoder:    decoder,
		evidence:   newEvidenceIngestor(registry, rb, el, defaultProtocol, relations, trace),
		nsIDByName: map[string]string{},
	}, nil
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
	if err := u.evidence.ingest(context.Background(), facts); err != nil {
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
	if err := u.evidence.ingest(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("monitor: failed to route gRPC event", tag.Error(err))
	}
}

// ObserveFact records an already-normalized non-secret fact from an in-process observation boundary.
func (u *Monitor) ObserveFact(ctx context.Context, observed umpirefw.Fact) error {
	if observed == nil {
		return nil
	}
	return u.evidence.ingest(ctx, []umpirefw.Fact{observed})
}

// RecordResponse converts a gRPC response to an event (if any) and routes it.
func (u *Monitor) RecordResponse(ctx context.Context, req, resp any) {
	var namespaceID string
	if named, ok := req.(interface{ GetNamespace() string }); ok {
		namespaceID = u.resolveNamespaceID(named.GetNamespace())
	}
	facts := u.decoder.ImportResponses(req, resp, namespaceID)
	if len(facts) == 0 {
		return
	}
	if err := u.evidence.ingest(ctx, facts); err != nil {
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
	if routeErr := u.evidence.ingest(ctx, []umpirefw.Fact{ev}); routeErr != nil {
		u.logger.Warn("monitor: failed to route rejection event", tag.Error(routeErr))
	}
}

// ObserveExecution records a neutral runtime action window or verdict.
func (u *Monitor) ObserveExecution(_ context.Context, observed umpirefw.ExecutionObservation) error {
	return u.evidence.observeExecution(observed)
}

// CheckNamespace runs a final check scoped to a single namespace: only entities
// rooted at that namespace are evaluated, and their unresolved liveness
// conditions are promoted to violations. Use it to validate one test's namespace
// at teardown, then PurgeNamespace to drop the collected data.
func (u *Monitor) CheckNamespace(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	return u.evidence.check(ctx, root, true)
}

// CheckNamespaceSafety applies the global rulebook without promoting pending liveness obligations.
func (u *Monitor) CheckNamespaceSafety(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	return u.evidence.check(ctx, root, false)
}

// PurgeNamespace removes all entities, facts, and rule state collected for the
// given namespace, so a shared monitor carries nothing between tests.
func (u *Monitor) PurgeNamespace(namespaceID string) {
	root := u.namespaceRoot(namespaceID)
	u.decoder.PurgeNamespace(namespaceID)
	u.evidence.purgeScope(root)
}

func (u *Monitor) namespaceRoot(namespaceID string) umpirefw.EntityID {
	return umpirefw.NewEntityID(model.NamespaceType, namespaceID)
}

// FactLog returns the event log for querying events in tests.
func (u *Monitor) FactLog() *umpirefw.FactLog {
	return u.evidence.factLog
}

// ModelState returns the entity registry for querying entities in tests.
func (u *Monitor) ModelState() *umpirefw.ModelState {
	return u.evidence.registry
}

// Relations returns the protocol's runtime relation state.
func (u *Monitor) Relations() *umpirefw.RelationStore {
	return u.evidence.relations
}

// SetCoverage installs an optional semantic coverage collector.
func (u *Monitor) SetCoverage(coverage *umpirefw.Coverage) {
	u.evidence.setCoverage(coverage)
}

// SetTraceRecorder installs an optional normalized trace recorder.
func (u *Monitor) SetTraceRecorder(recorder *umpirefw.TraceRecorder) {
	u.evidence.trace.setRecorder(recorder)
}

// RuleStats returns per-rule evaluation statistics.
func (u *Monitor) RuleStats() []umpirefw.RuleStats {
	return u.evidence.rulebook.Stats()
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (u *Monitor) PassedKeys(ruleName string) []string {
	return u.evidence.rulebook.PassedKeys(ruleName)
}

// RequireRulePassed asserts that the given rule evaluated the entity identified
// by entityKey and found no violation. Fails the test if the key is not found
// in the rule's passed keys.
func (u *Monitor) RequireRulePassed(t testing.TB, rule interface{ Name() string }, entityKey string) {
	t.Helper()
	name := rule.Name()
	passed := u.evidence.rulebook.PassedKeys(name)
	if !slices.Contains(passed, entityKey) {
		t.Errorf("rule %s did not pass entity %q; passed keys: %v", name, entityKey, passed)
	}
}

// Shutdown cleanly shuts down all Monitor components.
func (u *Monitor) Shutdown(_ context.Context) error {
	u.logger.Info("monitor closed")
	return nil
}

// NewUnaryServerInterceptor returns a gRPC interceptor that records events via u
// and optionally injects faults via inj. Either may be nil.
func NewUnaryServerInterceptor(u *Monitor, inj umpirefw.FaultInjector) grpc.UnaryServerInterceptor {
	return umpirefw.NewUnaryServerInterceptor(u, inj)
}
