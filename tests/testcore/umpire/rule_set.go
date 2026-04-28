package umpire

import (
	"fmt"
	"sort"
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

// RuleSet is a package-level registry for rules that self-register during
// package initialization.
type RuleSet struct {
	rules []Rule
}

// Register accepts a SafetyRule, a LivenessRule, or a builder closure
// (func(*RuleBuilder) SafetyRule / LivenessRule) that constructs one. The
// `any` parameter is the one place the type cannot be statically known —
// the builder-closure forms have distinct function types — so the conversion
// to the sealed Rule interface happens here, once.
func (s *RuleSet) Register(rule any) struct{} {
	s.rules = append(s.rules, asRule(rule))
	return struct{}{}
}

// asRule converts a registration argument into a Rule, panicking on an
// unsupported type or an empty rule.
func asRule(rule any) Rule {
	var r Rule
	switch v := rule.(type) {
	case SafetyRule:
		r = v
	case LivenessRule:
		r = v
	case func(*RuleBuilder) SafetyRule:
		builder := &RuleBuilder{}
		r = builder.safetyRule(v(builder))
	case func(*RuleBuilder) LivenessRule:
		builder := &RuleBuilder{}
		r = builder.livenessRule(v(builder))
	default:
		panic("umpire: rule must be SafetyRule, LivenessRule, func(*RuleBuilder) SafetyRule, or func(*RuleBuilder) LivenessRule")
	}
	requireRuleNonEmpty(r)
	return r
}

// requireRuleNonEmpty rejects rules that declare neither a Check function
// nor an Examples closure. An empty rule has no observable behavior and is
// almost certainly a wiring mistake; this catches it at registration time.
func requireRuleNonEmpty(rule Rule) {
	if rule.ruleIsEmpty() {
		panic(fmt.Sprintf("umpire: rule %q must declare a Check function or an Examples closure", rule.ruleName()))
	}
}

type RuleBuilder struct {
	coverage []CoveragePoint
}

type CoveragePointOption func(*CoveragePoint)

func MinVerified(n int) CoveragePointOption {
	return func(point *CoveragePoint) {
		point.MinVerified = n
	}
}

func (b *RuleBuilder) CoveragePoint(name string, description string, options ...CoveragePointOption) CoveragePoint {
	point := CoveragePoint{
		Name:        name,
		Description: description,
	}
	for _, option := range options {
		option(&point)
	}
	b.coverage = append(b.coverage, point)
	return point
}

func (b *RuleBuilder) safetyRule(rule SafetyRule) SafetyRule {
	rule.Coverage = appendRuleCoverage(b.coverage, rule.Coverage)
	return rule
}

func (b *RuleBuilder) livenessRule(rule LivenessRule) LivenessRule {
	rule.Coverage = appendRuleCoverage(b.coverage, rule.Coverage)
	return rule
}

func appendRuleCoverage(builderCoverage, ruleCoverage []CoveragePoint) []CoveragePoint {
	coverage := make([]CoveragePoint, 0, len(builderCoverage)+len(ruleCoverage))
	coverage = append(coverage, builderCoverage...)
	coverage = append(coverage, ruleCoverage...)
	return coverage
}

func (s *RuleSet) Rules() []Rule {
	rules := make([]Rule, len(s.rules))
	copy(rules, s.rules)
	sortRules(rules)
	return rules
}

// Attach adds every registered rule to u in deterministic Name order so
// violation output is stable across reordering of Register calls.
func (s *RuleSet) Attach(u *Umpire) {
	for _, rule := range s.Rules() {
		u.AddRule(rule)
	}
}

// Merge appends rules from other into s. Rules whose Name already exists in
// s are skipped — this lets multiple model components register overlapping
// rule sets safely. Returns s for chaining.
func (s *RuleSet) Merge(other *RuleSet) *RuleSet {
	if other == nil {
		return s
	}
	seen := make(map[string]struct{})
	for _, r := range s.rules {
		if name := r.ruleName(); name != "" {
			seen[name] = struct{}{}
		}
	}
	for _, r := range other.rules {
		name := r.ruleName()
		if name != "" {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
		}
		s.rules = append(s.rules, r)
	}
	return s
}

// Add appends an already-formed rule to the set without going through the
// builder-aware path of Register. Use this when you want to combine a
// computed rule (e.g. RPCRegistryRule()) with an existing self-registered
// set; for self-registration during package init, prefer Register.
func (s *RuleSet) Add(rule Rule) *RuleSet {
	requireRuleNonEmpty(rule)
	s.rules = append(s.rules, rule)
	return s
}

// RunExamples runs each rule's Examples as parallel t.Run subtests. The
// example bodies are entirely user-defined — typically they build a
// synthetic history with HistoryFromFacts, invoke the rule's Check via
// NewRuleContext, and assert on the resulting Violations. The framework
// only takes care of subtest naming and t.Parallel.
//
// Use this from a Go test:
//
//	func TestExamples(t *testing.T) { myRules.RunExamples(t) }
func (s *RuleSet) RunExamples(t *testing.T) {
	t.Helper()
	for _, rule := range s.rules {
		runRuleExamples(t, rule)
	}
}

func runRuleExamples(t *testing.T, rule Rule) {
	t.Helper()
	examples := rule.ruleExamples()
	if examples == nil {
		return
	}
	parallelsuite.Wrap(t).Run(rule.ruleName(), examples)
}

func sortRules(rules []Rule) {
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ruleName() < rules[j].ruleName()
	})
}
