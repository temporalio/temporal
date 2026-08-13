package umpire2

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
	"go.temporal.io/server/tests/umpire2/rule"
	"google.golang.org/grpc"
)

// Monitor is the property-based test monitoring system.
// It receives gRPC events and OTEL traces, routes them to entity FSMs, and
// runs pluggable verification rules that detect invariant violations.
//
// Monitor implements sdktrace.SpanProcessor so it can receive spans
// synchronously (no batching delay) and process them inline.
type Monitor struct {
	logger    log.Logger
	registry  *umpirefw.ModelState
	decoder   *model.FactDecoder
	rulebook  *umpirefw.RuleRegistry
	factLog   *umpirefw.FactLog
	protocol  *protocol.Protocol
	relations *umpirefw.RelationStore

	// nsIDByName resolves a namespace name (all a frontend request carries) to the id the model
	// scopes entities by. A synchronous rejection produces no telemetry, so its fact must be
	// namespace-id-rooted from the request alone; the driver seeds this map (it knows both) before
	// driving. See SetNamespaceID and UMPIRE_ERR.md.
	nsMu        sync.RWMutex
	nsIDByName  map[string]string
	coverageMu  sync.RWMutex
	coverage    *umpirefw.Coverage
	traceMu     sync.Mutex
	trace       *umpirefw.TraceRecorder
	traceSeq    atomic.Uint64
	traceSeen   map[string]struct{}
	traceActive map[string]map[string][]string
	traceLast   map[string]string
	footprints  map[string]umpirefw.CausalFootprint
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
	declaredFootprints, err := protocol.DefaultCausalFootprints()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to compile causal footprints: %w", err)
	}
	defaultProtocol.Register(registry)
	relations, err := defaultProtocol.NewRelationStore()
	if err != nil {
		return nil, fmt.Errorf("monitor: failed to create relation store: %w", err)
	}

	decoder := model.NewFactDecoder()
	el := umpirefw.NewFactLog()
	rb := umpirefw.NewRuleRegistry()

	// Safety rules — checked on every observation.
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.SpeculativeTaskCreation{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.NexusOperationClosure{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.NexusActivityLinkConsistency{} })
	rb.RegisterSafety(func() umpirefw.SafetyRule { return &rule.NexusOperationTimeoutSemantics{} })
	// Illegal-transition conformance is not registered as a rule: it is a built-in
	// framework check (RuleRegistry.Check → checkConformance) that surfaces, for every
	// Lifecycled entity, the illegal transitions Lifecycle.Fire records at fire-time —
	// the model judging its own transitions. It is silent for the current converging-DAG
	// lifecycles (Classify treats forward jumps over unobserved states as legal, so they
	// have no possible illegal transitions) and gains teeth with event-time ordering or a
	// branching lifecycle. See UMPIRE_PLAN.md.

	// Liveness rules — checked at test teardown.
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.WorkflowTaskStarvation{} })
	rb.RegisterLiveness(func() umpirefw.LivenessRule { return &rule.EntityProgress{} })

	if err := rb.InitRules(registry, logger, umpirefw.RuleConfig{Relations: relations}); err != nil {
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
		protocol:   defaultProtocol,
		relations:  relations,
		nsIDByName: map[string]string{},
		footprints: make(map[string]umpirefw.CausalFootprint, len(declaredFootprints)),
	}
	for _, declared := range declaredFootprints {
		u.footprints[declared.Footprint.Action] = declared.Footprint
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
	if err := u.routeFacts(context.Background(), facts); err != nil {
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
	if err := u.routeFacts(ctx, []umpirefw.Fact{ev}); err != nil {
		u.logger.Warn("monitor: failed to route gRPC event", tag.Error(err))
	}
}

// ObserveFact records an already-normalized non-secret fact from an in-process observation boundary.
func (u *Monitor) ObserveFact(ctx context.Context, observed umpirefw.Fact) error {
	if observed == nil {
		return nil
	}
	u.factLog.Add(observed)
	return u.routeFacts(ctx, []umpirefw.Fact{observed})
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
	u.factLog.AddAll(facts)
	if err := u.routeFacts(ctx, facts); err != nil {
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
	if routeErr := u.routeFacts(ctx, []umpirefw.Fact{ev}); routeErr != nil {
		u.logger.Warn("monitor: failed to route rejection event", tag.Error(routeErr))
	}
}

func (u *Monitor) routeFacts(ctx context.Context, facts []umpirefw.Fact) error {
	modelErr := u.registry.RouteFacts(ctx, facts)
	relationErrors := u.protocol.ApplyRelations(u.relations, facts)
	for _, relationErr := range relationErrors {
		u.recordRelationConflict(relationErr)
	}
	relationErr := errors.Join(relationErrors...)
	u.recordCoverage(facts)
	traceErr := u.recordTrace(facts)
	return errors.Join(modelErr, relationErr, traceErr)
}

// ObserveExecution records a neutral runtime action window or verdict.
func (u *Monitor) ObserveExecution(_ context.Context, observed umpirefw.ExecutionObservation) error {
	if observed.Kind == umpirefw.ExecutionActionStart && observed.Action != "" {
		u.coverageMu.RLock()
		coverage := u.coverage
		u.coverageMu.RUnlock()
		if coverage != nil {
			coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageAction, ID: observed.Action})
		}
	}

	u.traceMu.Lock()
	defer u.traceMu.Unlock()
	if u.trace == nil {
		return nil
	}
	fields := map[string]string{}
	for key, value := range map[string]string{
		"scope":       observed.Scope,
		"phase":       observed.Phase,
		"outcome":     observed.Outcome,
		"error_class": observed.ErrorClass,
		"checkpoint":  observed.Checkpoint,
	} {
		if value != "" {
			fields[key] = value
		}
	}
	name := observed.Action
	if observed.Kind == umpirefw.ExecutionVerdict {
		name = observed.Checkpoint
		fields["pass"] = strconv.FormatBool(observed.Pass)
		fields["violations"] = strconv.Itoa(observed.Violations)
	}
	if name == "" {
		return fmt.Errorf("execution observation %s has no stable name", observed.Kind)
	}
	keyKind := "action"
	traceKind := umpirefw.TraceAction
	var causes []string
	switch observed.Kind {
	case umpirefw.ExecutionVerdict:
		keyKind = "verdict"
		traceKind = umpirefw.TraceVerdict
		if last := u.traceLast[observed.Scope]; last != "" {
			causes = []string{last}
		}
	case umpirefw.ExecutionActionFinish:
		if byAction := u.traceActive[observed.Scope]; byAction != nil {
			windows := byAction[observed.Action]
			if len(windows) != 0 {
				causes = []string{windows[0]}
			}
		}
	default:
	}
	key := u.nextTraceKey(keyKind)
	if err := u.trace.Record(umpirefw.TraceEvent{Key: key, Kind: traceKind, Name: name, Causes: causes, Fields: fields}); err != nil {
		return err
	}
	switch observed.Kind {
	case umpirefw.ExecutionActionStart:
		if u.traceActive[observed.Scope] == nil {
			u.traceActive[observed.Scope] = map[string][]string{}
		}
		u.traceActive[observed.Scope][observed.Action] = append(u.traceActive[observed.Scope][observed.Action], key)
	case umpirefw.ExecutionActionFinish:
		if byAction := u.traceActive[observed.Scope]; byAction != nil {
			windows := byAction[observed.Action]
			if len(windows) != 0 {
				byAction[observed.Action] = windows[1:]
			}
		}
		u.traceLast[observed.Scope] = key
		if footprint, ok := u.footprints[observed.Action]; ok {
			if err := umpirefw.CompareCausalFootprint(footprint, u.trace.Snapshot()); err != nil {
				return fmt.Errorf("causal footprint %s: %w", observed.Action, err)
			}
		}
	default:
	}
	return nil
}

func (u *Monitor) recordRelationConflict(err error) {
	var relationErr *umpirefw.RelationError
	if !errors.As(err, &relationErr) || relationErr.Scope.Type == "" || relationErr.Scope.ID == "" {
		return
	}
	key := fmt.Sprintf("%s:%s:%s:%s", relationErr.Type, relationErr.Source, relationErr.Target, relationErr.Reason)
	u.rulebook.RecordConformance(relationErr.Scope, key, umpirefw.Violation{
		Rule:    "Conformance",
		Message: fmt.Sprintf("relation %s rejected: %s", relationErr.Type, relationErr.Reason),
		Tags: map[string]string{
			"relation": string(relationErr.Type),
			"source":   relationErr.Source.String(),
			"target":   relationErr.Target.String(),
		},
	})
}

func (u *Monitor) recordCoverage(facts []umpirefw.Fact) {
	u.coverageMu.RLock()
	coverage := u.coverage
	u.coverageMu.RUnlock()
	if coverage == nil {
		return
	}
	roots := map[umpirefw.EntityID]struct{}{}
	for _, observed := range facts {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageFact, ID: observed.Name()})
		if path := observed.TargetEntity(); path != nil {
			roots[path.Root()] = struct{}{}
		}
	}
	for _, edge := range u.relations.Snapshot() {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRelation, ID: string(edge.Type)})
	}
	for root := range roots {
		for _, entry := range u.registry.QueryAll(0, &root) {
			lifecycled, ok := entry.Entity.(umpirefw.Lifecycled)
			if !ok {
				continue
			}
			for _, edge := range lifecycled.Lifecycle().VisitedEdges() {
				coverage.Record(umpirefw.CoveragePoint{
					Kind: umpirefw.CoverageTransition,
					ID:   fmt.Sprintf("%s:%s/%s/%s", entry.Entity.Type(), edge.From, edge.Event, edge.To),
				})
			}
		}
	}
}

func (u *Monitor) recordTrace(facts []umpirefw.Fact) error {
	u.traceMu.Lock()
	defer u.traceMu.Unlock()
	if u.trace == nil {
		return nil
	}
	roots := map[umpirefw.EntityID]struct{}{}
	var errs []error
	for _, observed := range facts {
		fields := map[string]string{}
		var causes []string
		if path := observed.TargetEntity(); path != nil {
			fields["target"] = umpirefw.EntityPathKey(path)
			roots[path.Root()] = struct{}{}
			causes = u.activeTraceCausesLocked(path.Root().ID)
		}
		if err := u.trace.Record(umpirefw.TraceEvent{
			Key:    u.nextTraceKey("fact"),
			Kind:   umpirefw.TraceFact,
			Name:   observed.Name(),
			Causes: causes,
			Fields: fields,
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for _, edge := range u.relations.Snapshot() {
		if _, scoped := roots[edge.Scope]; !scoped {
			continue
		}
		semanticKey := fmt.Sprintf("relation:%s:%s:%s", edge.Type, edge.Source, edge.Target)
		if _, seen := u.traceSeen[semanticKey]; seen {
			continue
		}
		u.traceSeen[semanticKey] = struct{}{}
		if err := u.trace.Record(umpirefw.TraceEvent{
			Key:    u.nextTraceKey("relation"),
			Kind:   umpirefw.TraceRelation,
			Name:   string(edge.Type),
			Causes: u.activeTraceCausesLocked(edge.Scope.ID),
			Fields: map[string]string{
				"source": edge.Source.String(),
				"target": edge.Target.String(),
			},
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for root := range roots {
		for _, entry := range u.registry.QueryAll(0, &root) {
			lifecycled, ok := entry.Entity.(umpirefw.Lifecycled)
			if !ok {
				continue
			}
			for _, edge := range lifecycled.Lifecycle().VisitedEdges() {
				name := fmt.Sprintf("%s:%s/%s/%s", entry.Entity.Type(), edge.From, edge.Event, edge.To)
				semanticKey := "transition:" + entry.Key + ":" + name
				if _, seen := u.traceSeen[semanticKey]; seen {
					continue
				}
				u.traceSeen[semanticKey] = struct{}{}
				if err := u.trace.Record(umpirefw.TraceEvent{
					Key:    u.nextTraceKey("transition"),
					Kind:   umpirefw.TraceTransition,
					Name:   name,
					Causes: u.activeTraceCausesLocked(root.ID),
					Fields: map[string]string{
						"entity": entry.Key,
					},
				}); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func (u *Monitor) activeTraceCausesLocked(scope string) []string {
	byAction := u.traceActive[scope]
	var causes []string
	for _, windows := range byAction {
		causes = append(causes, windows...)
	}
	slices.Sort(causes)
	return slices.Compact(causes)
}

func (u *Monitor) nextTraceKey(kind string) string {
	return fmt.Sprintf("%s:%d", kind, u.traceSeq.Add(1))
}

// CheckNamespace runs a final check scoped to a single namespace: only entities
// rooted at that namespace are evaluated, and their unresolved liveness
// conditions are promoted to violations. Use it to validate one test's namespace
// at teardown, then PurgeNamespace to drop the collected data.
func (u *Monitor) CheckNamespace(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	violations := u.rulebook.Check(ctx, true, &root)
	u.recordRuleCoverage(violations)
	return violations
}

// CheckNamespaceSafety applies the global rulebook without promoting pending liveness obligations.
func (u *Monitor) CheckNamespaceSafety(ctx context.Context, namespaceID string) []umpirefw.Violation {
	root := u.namespaceRoot(namespaceID)
	violations := u.rulebook.Check(ctx, false, &root)
	u.recordRuleCoverage(violations)
	return violations
}

func (u *Monitor) recordRuleCoverage(violations []umpirefw.Violation) {
	u.coverageMu.RLock()
	coverage := u.coverage
	u.coverageMu.RUnlock()
	if coverage == nil {
		return
	}
	for _, stats := range u.rulebook.Stats() {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleEvaluated, ID: stats.Name})
	}
	for _, violation := range violations {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleViolated, ID: violation.Rule})
	}
}

// PurgeNamespace removes all entities, facts, and rule state collected for the
// given namespace, so a shared monitor carries nothing between tests.
func (u *Monitor) PurgeNamespace(namespaceID string) {
	root := u.namespaceRoot(namespaceID)
	u.registry.PurgeScope(root)
	u.factLog.PurgeScope(root)
	u.rulebook.PurgeScope(root)
	u.relations.PurgeScope(root)
	u.traceMu.Lock()
	delete(u.traceActive, namespaceID)
	delete(u.traceLast, namespaceID)
	u.traceMu.Unlock()
}

func (u *Monitor) namespaceRoot(namespaceID string) umpirefw.EntityID {
	return umpirefw.NewEntityID(model.NamespaceType, namespaceID)
}

// FactLog returns the event log for querying events in tests.
func (u *Monitor) FactLog() *umpirefw.FactLog {
	return u.factLog
}

// ModelState returns the entity registry for querying entities in tests.
func (u *Monitor) ModelState() *umpirefw.ModelState {
	return u.registry
}

// Relations returns the protocol's runtime relation state.
func (u *Monitor) Relations() *umpirefw.RelationStore {
	return u.relations
}

// SetCoverage installs an optional semantic coverage collector.
func (u *Monitor) SetCoverage(coverage *umpirefw.Coverage) {
	u.coverageMu.Lock()
	u.coverage = coverage
	u.coverageMu.Unlock()
}

// SetTraceRecorder installs an optional normalized trace recorder.
func (u *Monitor) SetTraceRecorder(recorder *umpirefw.TraceRecorder) {
	u.traceMu.Lock()
	u.trace = recorder
	u.traceSeen = map[string]struct{}{}
	u.traceActive = map[string]map[string][]string{}
	u.traceLast = map[string]string{}
	u.traceSeq.Store(0)
	u.traceMu.Unlock()
}

// RuleStats returns per-rule evaluation statistics.
func (u *Monitor) RuleStats() []umpirefw.RuleStats {
	return u.rulebook.Stats()
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (u *Monitor) PassedKeys(ruleName string) []string {
	return u.rulebook.PassedKeys(ruleName)
}

// RequireRulePassed asserts that the given rule evaluated the entity identified
// by entityKey and found no violation. Fails the test if the key is not found
// in the rule's passed keys.
func (u *Monitor) RequireRulePassed(t testing.TB, rule interface{ Name() string }, entityKey string) {
	t.Helper()
	name := rule.Name()
	passed := u.rulebook.PassedKeys(name)
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
