package umpire

import (
	"fmt"
	"sync"
)

// Umpire records traffic facts and supports rule-based property checking.
type Umpire struct {
	mu            sync.Mutex
	nextSeq       int64
	history       []*Record
	safetyRules   []SafetyRule
	livenessRules []LivenessRule
	coverage      *coverageCollector
	coverageRunID int64
}

// Record adds a fact to the history.
func (u *Umpire) Record(fact Fact) *Record {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.nextSeq++
	r := &Record{Seq: u.nextSeq, Fact: fact}
	u.history = append(u.history, r)
	return r
}

// AddRule registers a rule. The rule must be a SafetyRule or LivenessRule.
func (u *Umpire) AddRule(rule any) {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch r := rule.(type) {
	case SafetyRule:
		u.safetyRules = append(u.safetyRules, r)
		if u.coverage != nil {
			u.coverage.declare(r.Name, r.Coverage)
		}
	case LivenessRule:
		u.livenessRules = append(u.livenessRules, r)
		if u.coverage != nil {
			u.coverage.declare(r.Name, r.Coverage)
		}
	default:
		panic(fmt.Sprintf("umpire: rule %T must be SafetyRule or LivenessRule", rule))
	}
}

// CheckRules runs all registered rules against the history.
// Safety rules are checked first, then liveness rules.
// When final is true, unresolved liveness conditions become violations.
func (u *Umpire) CheckRules(final bool) []Violation {
	u.mu.Lock()
	history := make([]*Record, len(u.history))
	copy(history, u.history)
	safetyRules := u.safetyRules
	livenessRules := u.livenessRules
	coverage := u.coverage
	coverageRunID := u.coverageRunID
	u.mu.Unlock()

	var violations []Violation
	for _, rule := range safetyRules {
		if rule.Check != nil {
			ctx := &RuleContext{rule: rule.Name, collector: coverage, runID: coverageRunID}
			rule.Check(ctx, history)
			violations = append(violations, ctx.ruleViolations()...)
		}
	}
	for _, rule := range livenessRules {
		if rule.Check != nil {
			ctx := &RuleContext{rule: rule.Name, collector: coverage, runID: coverageRunID}
			rule.Check(ctx, history, final)
			violations = append(violations, ctx.ruleViolations()...)
		}
	}
	return violations
}

func (u *Umpire) startCoverageRun(coverage *coverageCollector) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.coverage = coverage
	if coverage == nil {
		u.coverageRunID = 0
		return
	}
	u.coverageRunID = coverage.startRun()
}

func (u *Umpire) CoverageSummary() []CoverageSummaryLine {
	u.mu.Lock()
	coverage := u.coverage
	u.mu.Unlock()
	if coverage == nil {
		return nil
	}
	return coverage.summary()
}

// Reset clears all state: history and registered rules.
func (u *Umpire) Reset() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.history = nil
	u.nextSeq = 0
	u.safetyRules = nil
	u.livenessRules = nil
}

// History returns a snapshot of all records.
func (u *Umpire) History() []*Record {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]*Record, len(u.history))
	copy(out, u.history)
	return out
}

func (u *Umpire) String() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return fmt.Sprintf("Umpire{records=%d, safety=%d, liveness=%d}",
		len(u.history), len(u.safetyRules), len(u.livenessRules))
}
