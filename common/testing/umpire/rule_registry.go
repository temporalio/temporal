package umpire

import (
	"context"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// CheckSafetyRule is a test helper that runs a single safety rule and returns violations.
// It uses sinceGeneration=0 so all entities are visible.
func CheckSafetyRule(ctx context.Context, rule SafetyRule, registry *ModelState, logger log.Logger, config RuleConfig) []Violation {
	st := &ruleState{
		lastReported: make(map[string]time.Time),
		reportTTL:    defaultReportTTL,
	}
	rc := &SafetyContext{
		ruleContext: ruleContext{
			Context:    ctx,
			Now:        time.Now(),
			ModelState: registry,
			Logger:     logger,
			Config:     config,
			state:      st,
			ruleName:   rule.Name(),
		},
	}
	rule.CheckSafety(rc)
	return rc.violations
}

// CheckLivenessRule is a test helper that runs a single liveness rule and returns violations.
// It uses sinceGeneration=0 so all entities are visible, then collects pending items.
func CheckLivenessRule(ctx context.Context, rule LivenessRule, registry *ModelState, logger log.Logger, config RuleConfig) []Violation {
	st := &ruleState{
		lastReported: make(map[string]time.Time),
		pending:      make(map[string]Violation),
		reportTTL:    defaultReportTTL,
	}
	rc := &LivenessContext{
		ruleContext: ruleContext{
			Context:    ctx,
			Now:        time.Now(),
			ModelState: registry,
			Logger:     logger,
			Config:     config,
			state:      st,
			ruleName:   rule.Name(),
		},
	}
	rule.CheckLiveness(rc)
	// Collect pending items as violations (simulates teardown).
	for _, v := range st.pending {
		rc.violations = append(rc.violations, v)
	}
	return rc.violations
}

// Violation represents a detected invariant violation.
type Violation struct {
	Rule    string
	Message string
	Tags    map[string]string
}

// SafetyRule checks invariants that must hold at every observation point.
// Violations are immediate — if the invariant doesn't hold now, it's a bug.
type SafetyRule interface {
	Name() string
	CheckSafety(c *SafetyContext)
}

// LivenessRule checks conditions that must eventually hold.
// At teardown (final check), any unresolved Pending items become violations.
type LivenessRule interface {
	Name() string
	CheckLiveness(c *LivenessContext)
}

// RuleConfig holds configuration overrides for rules.
type RuleConfig struct {
	Relations *RelationStore
}

// Dedup reporting constants.
const (
	reportInterval   = 1 * time.Minute
	defaultReportTTL = 5 * time.Minute
)

// ruleState holds persistent per-rule state across Check calls.
type ruleState struct {
	mu             sync.Mutex
	passedKeys     []string
	lastReported   map[string]time.Time
	reportTTL      time.Duration
	lastGeneration uint64               // generation watermark for dirty-tracking
	pending        map[string]Violation // unresolved liveness conditions
}

// ruleContext holds shared fields for both SafetyContext and LivenessContext.
type ruleContext struct {
	context.Context
	Now             time.Time
	ModelState      *ModelState
	Logger          log.Logger
	Config          RuleConfig
	sinceGeneration uint64    // only query entities changed after this generation
	scope           *EntityID // if set, only query entities rooted at this ancestor (e.g. a namespace)

	state      *ruleState
	ruleName   string
	violations []Violation
}

func (c *ruleContext) logViolation(v Violation) {
	tags := []tag.Tag{tag.NewStringTag("rule", v.Rule)}
	for k, val := range v.Tags {
		tags = append(tags, tag.NewStringTag(k, val))
	}
	c.Logger.Warn(fmt.Sprintf("violation: %s", v.Message), tags...)
}

func (c *ruleContext) recordViolation(key string, v Violation) {
	if lr, reported := c.state.lastReported[key]; reported && c.Now.Sub(lr) < reportInterval {
		return
	}
	c.state.lastReported[key] = c.Now
	v.Rule = c.ruleName
	c.violations = append(c.violations, v)
	c.logViolation(v)
}

// SafetyContext is passed to SafetyRule.CheckSafety.
// It exposes Eval and Pass for immediate invariant checking.
type SafetyContext struct {
	ruleContext
}

// Eval evaluates an entity against an invariant. If ok, records a pass.
// If !ok, records a violation (with dedup).
func (c *SafetyContext) Eval(key string, ok bool, v Violation) {
	if ok {
		c.state.passedKeys = append(c.state.passedKeys, key)
		return
	}
	c.recordViolation(key, v)
}

// Pass records that an entity was evaluated and the invariant held.
func (c *SafetyContext) Pass(key string) {
	c.state.passedKeys = append(c.state.passedKeys, key)
}

// LivenessContext is passed to LivenessRule.CheckLiveness.
// It exposes Pending and Resolve for tracking conditions that must eventually hold.
type LivenessContext struct {
	ruleContext
}

// Pending records that a liveness condition has not yet been met.
// The condition is stored persistently; at teardown, unresolved items become violations.
func (c *LivenessContext) Pending(key string, v Violation) {
	v.Rule = c.ruleName
	c.state.pending[key] = v
}

// Resolve removes a previously-pending condition, indicating it has been met.
func (c *LivenessContext) Resolve(key string) {
	delete(c.state.pending, key)
	c.state.passedKeys = append(c.state.passedKeys, key)
}

// EntityResult pairs a registry key with a typed entity pointer.
type EntityResult[T any] struct {
	Key    string
	Entity *T
}

// ChangedEntities yields every entity changed since this rule's last check.
// It is the type-erased counterpart of Changed[T] for rules that select by type switch.
func (c *ruleContext) ChangedEntities() iter.Seq[EntityEntry] {
	return func(yield func(EntityEntry) bool) {
		for _, entry := range c.ModelState.QueryAll(c.sinceGeneration, c.scope) {
			if c.Err() != nil || !yield(entry) {
				return
			}
		}
	}
}

// Changed yields entities of type T that received facts since this rule's last
// check (respecting the rule's dirty-generation watermark and namespace scope).
// Iteration stops early if the context is cancelled.
//
// It is a generic method (Go 1.27+), defined once on the embedded ruleContext.
// SafetyContext and LivenessContext forward to it explicitly so external analyzers
// see the method while rules keep the same c.Changed[model.WorkflowUpdate]() call.
func (c *ruleContext) Changed[T any]() iter.Seq[EntityResult[T]] {
	return func(yield func(EntityResult[T]) bool) {
		et := EntityType(reflect.TypeOf((*T)(nil)).Elem().Name())
		for _, e := range c.ModelState.QueryEntities(et, c.sinceGeneration, c.scope) {
			if c.Err() != nil {
				return
			}
			if typed, ok := any(e.Entity).(*T); ok {
				if !yield(EntityResult[T]{Key: e.Key, Entity: typed}) {
					return
				}
			}
		}
	}
}

// Changed yields entities that changed since the safety rule's last check.
func (c *SafetyContext) Changed[T any]() iter.Seq[EntityResult[T]] {
	return c.ruleContext.Changed[T]()
}

// Changed yields entities that changed since the liveness rule's last check.
func (c *LivenessContext) Changed[T any]() iter.Seq[EntityResult[T]] {
	return c.ruleContext.Changed[T]()
}

// RuleStats holds per-rule evaluation statistics.
type RuleStats struct {
	Name   string
	Kind   string // "safety" or "liveness"
	Passes int
}

// ruleEntry is a tagged union for storing either kind of rule.
type ruleEntry struct {
	name     string
	kind     string // "safety" or "liveness"
	safety   SafetyRule
	liveness LivenessRule
}

// RuleRegistry manages rule registration, initialization, and state.
type RuleRegistry struct {
	mu       sync.RWMutex
	registry map[string]func() ruleEntry
	rules    []ruleEntry
	states   map[string]*ruleState

	ruleModelState *ModelState
	logger         log.Logger
	config         RuleConfig

	// conformance dedup: per entity key, how many recorded illegal transitions have
	// already been surfaced as violations (see checkConformance).
	conformanceMu       sync.Mutex
	reportedIllegal     map[string]int
	recordedConformance map[EntityID]map[string]Violation
}

// NewRuleRegistry creates a new rulebook.
func NewRuleRegistry() *RuleRegistry {
	return &RuleRegistry{
		registry: make(map[string]func() ruleEntry),
		states:   make(map[string]*ruleState),
	}
}

// RegisterSafety registers a safety rule factory.
func (r *RuleRegistry) RegisterSafety(factory func() SafetyRule) {
	probe := factory()
	name := validateRuleName(probe, probe.Name())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[name] = func() ruleEntry {
		return ruleEntry{name: name, kind: "safety", safety: factory()}
	}
}

// RegisterLiveness registers a liveness rule factory.
func (r *RuleRegistry) RegisterLiveness(factory func() LivenessRule) {
	probe := factory()
	name := validateRuleName(probe, probe.Name())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[name] = func() ruleEntry {
		return ruleEntry{name: name, kind: "liveness", liveness: factory()}
	}
}

func validateRuleName(probe any, name string) string {
	name = strings.TrimSpace(name)
	expected := reflect.TypeOf(probe).Elem().Name() + "Rule"
	if name != expected {
		panic(fmt.Sprintf("rule %T: Name() returned %q, expected %q (must be struct name + \"Rule\")", probe, name, expected))
	}
	return name
}

// InitRules constructs rules. If names is empty, all registered rules are used.
func (r *RuleRegistry) InitRules(registry *ModelState, logger log.Logger, config RuleConfig, names ...string) error {
	if registry == nil {
		return fmt.Errorf("registry is required")
	}
	if logger == nil {
		return fmt.Errorf("logger is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ruleModelState = registry
	r.logger = logger
	r.config = config

	if len(names) == 0 {
		for n := range r.registry {
			names = append(names, n)
		}
	}

	var out []ruleEntry
	seen := map[string]struct{}{}
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		if _, dup := seen[n]; dup {
			continue
		}
		f, ok := r.registry[n]
		if !ok {
			return fmt.Errorf("unknown rule: %q", n)
		}
		out = append(out, f())
		r.states[n] = &ruleState{
			lastReported: make(map[string]time.Time),
			pending:      make(map[string]Violation),
			reportTTL:    defaultReportTTL,
		}
		seen[n] = struct{}{}
	}
	r.rules = out
	return nil
}

// Check runs all initialized rules and returns all violations. Safety rules
// run on every call (only on dirty entities). Liveness rules run on every call
// (only on dirty entities) to update their pending set. When final is true,
// unresolved pending items are collected as violations.
func (r *RuleRegistry) Check(ctx context.Context, final bool, scope *EntityID) []Violation {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	currentGen := r.ruleModelState.Generation()
	var allViolations []Violation

	for _, entry := range r.rules {
		st := r.states[entry.name]
		st.mu.Lock()

		base := ruleContext{
			Context:         ctx,
			Now:             now,
			ModelState:      r.ruleModelState,
			Logger:          r.logger,
			Config:          r.config,
			sinceGeneration: st.lastGeneration,
			scope:           scope,
			state:           st,
			ruleName:        entry.name,
		}

		switch entry.kind {
		case "safety":
			rc := &SafetyContext{ruleContext: base}
			entry.safety.CheckSafety(rc)
			allViolations = append(allViolations, rc.violations...)
		case "liveness":
			rc := &LivenessContext{ruleContext: base}
			entry.liveness.CheckLiveness(rc)
			allViolations = append(allViolations, rc.violations...)
			// At teardown, collect unresolved pending items (in scope) as
			// violations, and drop them so they aren't re-reported when another
			// scope tears down.
			if final {
				for key, v := range st.pending {
					if keyInScope(key, scope) {
						allViolations = append(allViolations, v)
						delete(st.pending, key)
					}
				}
			}
		}

		st.lastGeneration = currentGen
		st.pruneReported(now)
		st.mu.Unlock()
	}

	// Built-in conformance: surface transitions the model itself judged illegal at
	// fire-time. This is not a pluggable rule — it runs for every Lifecycled entity,
	// always, as the model judging its own transitions.
	allViolations = append(allViolations, r.checkConformance(scope)...)
	allViolations = append(allViolations, r.recordedConformanceViolations(scope)...)

	return allViolations
}

// RecordConformance retains one scoped, deduplicated conformance violation until the scope is purged.
func (r *RuleRegistry) RecordConformance(scope EntityID, key string, violation Violation) {
	if scope.Type == "" || scope.ID == "" || key == "" {
		return
	}
	if violation.Rule == "" {
		violation.Rule = "Conformance"
	}
	violation.Tags = cloneViolationTags(violation.Tags)
	r.conformanceMu.Lock()
	defer r.conformanceMu.Unlock()
	if r.recordedConformance == nil {
		r.recordedConformance = map[EntityID]map[string]Violation{}
	}
	if r.recordedConformance[scope] == nil {
		r.recordedConformance[scope] = map[string]Violation{}
	}
	if _, exists := r.recordedConformance[scope][key]; !exists {
		r.recordedConformance[scope][key] = violation
	}
}

func (r *RuleRegistry) recordedConformanceViolations(scope *EntityID) []Violation {
	r.conformanceMu.Lock()
	defer r.conformanceMu.Unlock()
	type entry struct {
		scope EntityID
		key   string
		value Violation
	}
	var entries []entry
	for recordedScope, violations := range r.recordedConformance {
		if scope != nil && recordedScope != *scope {
			continue
		}
		for key, violation := range violations {
			entries = append(entries, entry{scope: recordedScope, key: key, value: violation})
		}
	}
	slices.SortFunc(entries, func(left, right entry) int {
		if result := strings.Compare(left.scope.String(), right.scope.String()); result != 0 {
			return result
		}
		return strings.Compare(left.key, right.key)
	})
	result := make([]Violation, len(entries))
	for index, recorded := range entries {
		result[index] = recorded.value
		result[index].Tags = cloneViolationTags(recorded.value.Tags)
	}
	return result
}

func cloneViolationTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for key, value := range tags {
		result[key] = value
	}
	return result
}

// checkConformance surfaces the illegal transitions the model recorded at fire-time
// (Classify == Illegal, via Lifecycle.Fire, driven from OnFact) as conformance
// violations. Each illegal transition is reported once per entity — deduped by how
// many were already surfaced — so repeated Checks don't re-report the same one.
func (r *RuleRegistry) checkConformance(scope *EntityID) []Violation {
	r.conformanceMu.Lock()
	defer r.conformanceMu.Unlock()
	if r.reportedIllegal == nil {
		r.reportedIllegal = map[string]int{}
	}
	var out []Violation
	for _, e := range r.ruleModelState.QueryAll(0, scope) {
		lc, ok := e.Entity.(Lifecycled)
		if !ok {
			continue
		}
		illegal := lc.Lifecycle().Illegal()
		for i := r.reportedIllegal[e.Key]; i < len(illegal); i++ {
			it := illegal[i]
			out = append(out, Violation{
				Rule:    "Conformance",
				Message: fmt.Sprintf("illegal transition: event %q is not legal from state %q", it.Event, it.From),
				Tags:    map[string]string{"entity": e.Key, "from": it.From, "event": it.Event},
			})
		}
		r.reportedIllegal[e.Key] = len(illegal)
	}
	return out
}

// keyInScope reports whether a registry key falls under the given scope root.
// A nil scope matches every key (global check).
func keyInScope(key string, scope *EntityID) bool {
	if scope == nil {
		return true
	}
	prefix := fmt.Sprintf("%s:%s", scope.Type, scope.ID)
	return key == prefix || strings.HasPrefix(key, prefix+"@")
}

// PurgeScope drops all per-rule state (pending, dedup, passed keys) for entities
// rooted at the given ancestor, so a torn-down namespace leaves nothing behind.
func (r *RuleRegistry) PurgeScope(root EntityID) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	scope := &root
	for _, st := range r.states {
		st.mu.Lock()
		for k := range st.pending {
			if keyInScope(k, scope) {
				delete(st.pending, k)
			}
		}
		for k := range st.lastReported {
			if keyInScope(k, scope) {
				delete(st.lastReported, k)
			}
		}
		st.passedKeys = slices.DeleteFunc(st.passedKeys, func(k string) bool { return keyInScope(k, scope) })
		st.mu.Unlock()
	}
	r.conformanceMu.Lock()
	for k := range r.reportedIllegal {
		if keyInScope(k, scope) {
			delete(r.reportedIllegal, k)
		}
	}
	delete(r.recordedConformance, root)
	r.conformanceMu.Unlock()
}

// RuleCount returns the count of initialized rules by kind.
func (r *RuleRegistry) RuleCount() (safety, liveness int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, entry := range r.rules {
		switch entry.kind {
		case "safety":
			safety++
		case "liveness":
			liveness++
		}
	}
	return
}

// Stats returns per-rule evaluation statistics.
func (r *RuleRegistry) Stats() []RuleStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := make([]RuleStats, 0, len(r.rules))
	for _, entry := range r.rules {
		st := r.states[entry.name]
		st.mu.Lock()
		s := RuleStats{Name: entry.name, Kind: entry.kind, Passes: len(st.passedKeys)}
		st.mu.Unlock()
		stats = append(stats, s)
	}
	return stats
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (r *RuleRegistry) PassedKeys(ruleName string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.states[ruleName]
	if !ok {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	result := make([]string, len(st.passedKeys))
	copy(result, st.passedKeys)
	return result
}

func (s *ruleState) pruneReported(now time.Time) {
	ttl := s.reportTTL
	if ttl == 0 {
		ttl = defaultReportTTL
	}
	cutoff := now.Add(-ttl)
	for key, lr := range s.lastReported {
		if lr.Before(cutoff) {
			delete(s.lastReported, key)
		}
	}
}
