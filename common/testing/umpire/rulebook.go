package umpire

import (
	"context"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// CheckSafetyRule is a test helper that runs a single safety rule and returns violations.
// It uses sinceGeneration=0 so all entities are visible.
func CheckSafetyRule(ctx context.Context, m SafetyRule, registry *Registry, logger log.Logger, config RuleConfig) []Violation {
	st := &ruleState{
		lastReported: make(map[string]time.Time),
		reportTTL:    defaultReportTTL,
	}
	rc := &SafetyContext{
		ruleContext: ruleContext{
			Context:  ctx,
			Now:      time.Now(),
			Registry: registry,
			Logger:   logger,
			Config:   config,
			state:    st,
			ruleName: m.Name(),
		},
	}
	m.CheckSafety(rc)
	return rc.violations
}

// CheckLivenessRule is a test helper that runs a single liveness rule and returns violations.
// It uses sinceGeneration=0 so all entities are visible, then collects pending items.
func CheckLivenessRule(ctx context.Context, m LivenessRule, registry *Registry, logger log.Logger, config RuleConfig) []Violation {
	st := &ruleState{
		lastReported: make(map[string]time.Time),
		pending:      make(map[string]Violation),
		reportTTL:    defaultReportTTL,
	}
	rc := &LivenessContext{
		ruleContext: ruleContext{
			Context:  ctx,
			Now:      time.Now(),
			Registry: registry,
			Logger:   logger,
			Config:   config,
			state:    st,
			ruleName: m.Name(),
		},
	}
	m.CheckLiveness(rc)
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
type RuleConfig struct{}

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
	Registry        *Registry
	Logger          log.Logger
	Config          RuleConfig
	sinceGeneration uint64 // only query entities changed after this generation

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

// dirtyQuerier is implemented by both SafetyContext and LivenessContext,
// allowing ChangedEntities to accept either context type.
type dirtyQuerier interface {
	dirtyQuery() (context.Context, *Registry, uint64)
}

func (c *SafetyContext) dirtyQuery() (context.Context, *Registry, uint64) {
	return c.Context, c.Registry, c.sinceGeneration
}
func (c *LivenessContext) dirtyQuery() (context.Context, *Registry, uint64) {
	return c.Context, c.Registry, c.sinceGeneration
}

// EntityResult pairs a registry key with a typed entity pointer.
type EntityResult[T any] struct {
	Key    string
	Entity *T
}

// ChangedEntities returns entities of type T that received facts since this rule's last check.
// Iteration stops early if the context is cancelled.
func ChangedEntities[T any](c dirtyQuerier) iter.Seq[EntityResult[T]] {
	ctx, reg, since := c.dirtyQuery()
	return func(yield func(EntityResult[T]) bool) {
		et := EntityType(reflect.TypeOf((*T)(nil)).Elem().Name())
		for _, e := range reg.QueryEntities(et, since) {
			if ctx.Err() != nil {
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

// Rulebook manages rule registration, initialization, and state.
type Rulebook struct {
	mu       sync.RWMutex
	registry map[string]func() ruleEntry
	rules    []ruleEntry
	states   map[string]*ruleState

	ruleRegistry *Registry
	logger       log.Logger
	config       RuleConfig
}

// NewRulebook creates a new rulebook.
func NewRulebook() *Rulebook {
	return &Rulebook{
		registry: make(map[string]func() ruleEntry),
		states:   make(map[string]*ruleState),
	}
}

// RegisterSafety registers a safety rule factory.
func (r *Rulebook) RegisterSafety(factory func() SafetyRule) {
	probe := factory()
	name := validateRuleName(probe, probe.Name())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[name] = func() ruleEntry {
		return ruleEntry{name: name, kind: "safety", safety: factory()}
	}
}

// RegisterLiveness registers a liveness rule factory.
func (r *Rulebook) RegisterLiveness(factory func() LivenessRule) {
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
	typeName := reflect.TypeOf(probe).Elem().Name()
	if name != typeName {
		panic(fmt.Sprintf("rule %T: Name() returned %q, expected %q (must match struct name)", probe, name, typeName))
	}
	return name
}

// InitRules constructs rules. If names is empty, all registered rules are used.
func (r *Rulebook) InitRules(registry *Registry, logger log.Logger, config RuleConfig, names ...string) error {
	if registry == nil {
		return fmt.Errorf("registry is required")
	}
	if logger == nil {
		return fmt.Errorf("logger is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ruleRegistry = registry
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
func (r *Rulebook) Check(ctx context.Context, final bool) []Violation {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	currentGen := r.ruleRegistry.Generation()
	var allViolations []Violation

	for _, entry := range r.rules {
		st := r.states[entry.name]
		st.mu.Lock()

		base := ruleContext{
			Context:         ctx,
			Now:             now,
			Registry:        r.ruleRegistry,
			Logger:          r.logger,
			Config:          r.config,
			sinceGeneration: st.lastGeneration,
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
			// At teardown, collect all unresolved pending items as violations.
			if final {
				for _, v := range st.pending {
					allViolations = append(allViolations, v)
				}
			}
		}

		st.lastGeneration = currentGen
		st.pruneReported(now)
		st.mu.Unlock()
	}

	return allViolations
}

// RuleCount returns the count of initialized rules by kind.
func (r *Rulebook) RuleCount() (safety, liveness int) {
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
func (r *Rulebook) Stats() []RuleStats {
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
func (r *Rulebook) PassedKeys(ruleName string) []string {
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
