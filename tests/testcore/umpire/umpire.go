package umpire

import (
	"fmt"
	"sync"
	"time"
)

// Umpire records traffic facts and supports rule-based property checking.
type Umpire struct {
	mu            sync.Mutex
	nextSeq       int64
	history       []*Record
	rules         []Rule
	coverage      *coverageCollector
	coverageRunID int64
	now           func() time.Time
}

// SetClock overrides the clock used to stamp Record.At and to supply
// RuleContext.Now. Pass a virtual clock's Now (e.g. env.Clock().Now) to make
// time-bounded rules advance with the fake clock; the default is wall time.
func (u *Umpire) SetClock(now func() time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.now = now
}

// timeNow reads the configured clock, falling back to wall time so a
// zero-value Umpire works without setup. Callers must not hold u.mu.
func (u *Umpire) timeNow() time.Time {
	u.mu.Lock()
	now := u.now
	u.mu.Unlock()
	if now != nil {
		return now()
	}
	return time.Now()
}

// Record adds a fact to the history, stamping it with the current sequence
// number and clock time.
func (u *Umpire) Record(fact Fact) *Record {
	at := u.timeNow()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.nextSeq++
	r := &Record{Seq: u.nextSeq, At: at, Fact: fact}
	u.history = append(u.history, r)
	return r
}

// AddRule registers a rule for checking against the history.
func (u *Umpire) AddRule(rule Rule) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rules = append(u.rules, rule)
}

// CheckRules runs all registered rules against the history. Each rule is a
// pure function over the full history, so it is re-scanned from scratch on
// every call — the cost of a run is therefore quadratic in the number of
// steps, which is acceptable for the histories property tests produce.
//
// Safety rules are checked first, then liveness rules. When final is true,
// unresolved liveness conditions become violations.
func (u *Umpire) CheckRules(final bool) []Violation {
	now := u.timeNow()
	u.mu.Lock()
	history := make([]*Record, len(u.history))
	copy(history, u.history)
	rules := u.rules
	coverage := u.coverage
	coverageRunID := u.coverageRunID
	u.mu.Unlock()

	var violations []Violation
	for _, phase := range []rulePhase{phaseSafety, phaseLiveness} {
		for _, rule := range rules {
			if rule.rulePhase() != phase {
				continue
			}
			// Declare coverage lazily here rather than in AddRule so a rule's
			// points show up regardless of whether it was registered before or
			// after the coverage run started. declare is idempotent.
			if coverage != nil {
				coverage.declare(rule.ruleName(), rule.ruleCoverage())
			}
			ctx := &RuleContext{rule: rule.ruleName(), collector: coverage, runID: coverageRunID, now: now}
			rule.run(ctx, history, final)
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

// Reset clears per-run state: the history and the registered rules. Coverage
// is deliberately left intact, since it aggregates across runs.
func (u *Umpire) Reset() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.history = nil
	u.nextSeq = 0
	u.rules = nil
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
	var safety, liveness int
	for _, rule := range u.rules {
		if rule.rulePhase() == phaseSafety {
			safety++
		} else {
			liveness++
		}
	}
	return fmt.Sprintf("Umpire{records=%d, safety=%d, liveness=%d}",
		len(u.history), safety, liveness)
}
