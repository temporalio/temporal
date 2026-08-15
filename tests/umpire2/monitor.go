package umpire2

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/grpcadapter"
	"go.temporal.io/server/tests/umpire2/internal/assurance"
	"go.temporal.io/server/tests/umpire2/internal/model"
	"go.temporal.io/server/tests/umpire2/internal/protocol"
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

	defaultProtocol, err := protocol.Default()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to compile default protocol: %w", err)
	}
	decoder := model.NewFactDecoder()
	catalog, err := assurance.Default()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to compile assurance catalog: %w", err)
	}
	// Illegal-transition conformance is not registered as a rule: it is a built-in
	// framework check (RuleRegistry.Check → checkConformance) that surfaces, for every
	// Lifecycled entity, the illegal transitions Lifecycle.Fire records at fire-time —
	// the model judging its own transitions. Classify tolerates forward jumps over reachable
	// states because an unobserved intermediate is indistinguishable from a skipped transition;
	// events outside that reachable path remain illegal.

	runtime, err := umpirefw.NewRuntime(defaultProtocol.RuntimeDeclaration(catalog.RuntimeRules()))
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to initialize runtime: %w", err)
	}

	var safety, liveness int
	for _, stats := range runtime.RuleStats() {
		if stats.Kind == "safety" {
			safety++
		} else if stats.Kind == "liveness" {
			liveness++
		}
	}
	logger.Info("monitor initialized",
		tag.NewInt("safetyRules", safety),
		tag.NewInt("livenessRules", liveness),
	)

	trace := newExecutionTrace(runtime, defaultProtocol.CausalFootprints())
	return &Monitor{
		logger:     logger,
		decoder:    decoder,
		evidence:   newEvidenceIngestor(runtime, defaultProtocol, trace),
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
	return u.check(ctx, root, true)
}

// CheckNamespaceSafety applies the global rulebook without promoting pending liveness obligations.
func (u *Monitor) CheckNamespaceSafety(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	return u.check(ctx, root, false)
}

func (u *Monitor) check(ctx context.Context, root umpirefw.EntityID, final bool) []umpirefw.Violation {
	violations := u.evidence.check(ctx, root, final)
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
	u.decoder.PurgeNamespace(namespaceID)
	u.evidence.purgeScope(root)
}

func (u *Monitor) namespaceRoot(namespaceID string) umpirefw.EntityID {
	return umpirefw.NewEntityID(model.NamespaceType, namespaceID)
}

// Snapshot returns a defensive semantic view of one namespace.
func (u *Monitor) Snapshot(namespaceID string) umpirefw.Snapshot {
	root := u.namespaceRoot(namespaceID)
	snapshot := u.evidence.runtime.Snapshot(root)
	entries := u.evidence.runtime.View(root).AllEntities(0)
	slices.SortFunc(entries, func(left, right umpirefw.EntityEntry) int {
		return cmp.Compare(left.Key, right.Key)
	})
	entitiesByKey := make(map[string]umpirefw.Entity, len(entries))
	for _, entry := range entries {
		entitiesByKey[entry.Key] = entry.Entity
	}
	for index := range snapshot.Entities {
		entity := &snapshot.Entities[index]
		entry := umpirefw.EntityEntry{Key: entity.Key, Entity: entitiesByKey[entity.Key]}
		entity.ID = snapshotEntityID(namespaceID, entry)
		if run, ok := entry.Entity.(*model.WorkflowRun); ok {
			entity.RootID = run.FirstRunID
			entity.PredecessorID = run.PreviousRunID
			entity.Initiator = run.Initiator
		}
		if operation, ok := entry.Entity.(*model.NexusOperation); ok {
			entity.Attempt = operation.Attempt
		}
	}
	return snapshot
}

func snapshotEntityID(namespaceID string, entry umpirefw.EntityEntry) string {
	switch entity := entry.Entity.(type) {
	case *model.NexusOperation:
		if entity.WorkflowID != "" {
			return entity.WorkflowID
		}
	case *model.Workflow:
		if entity.WorkflowID != "" {
			return entity.WorkflowID
		}
	case *model.WorkflowRun:
		if entity.RunID != "" {
			return entity.RunID
		}
	}
	leaf := entry.Key[strings.LastIndex(entry.Key, "@")+1:]
	id := strings.TrimPrefix(leaf, string(entry.Entity.Type())+":")
	return strings.TrimPrefix(id, namespaceID+"\x00")
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
	return u.evidence.runtime.RuleStats()
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (u *Monitor) PassedKeys(ruleName string) []string {
	return u.evidence.runtime.PassedKeys(ruleName)
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
