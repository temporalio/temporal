package umpire

import (
	"fmt"
	"sort"

	"go.temporal.io/server/common/testing/parallelsuite"
)

// Violation describes a rule violation found during checking.
type Violation struct {
	Rule    string
	Point   string
	Message string
	Tags    map[string]string
}

func (v Violation) String() string {
	s := fmt.Sprintf("[%s] %s", v.Rule, v.Message)
	if v.Point != "" {
		s += fmt.Sprintf(" point=%s", v.Point)
	}
	if len(v.Tags) > 0 {
		keys := make([]string, 0, len(v.Tags))
		for k := range v.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		s += " {"
		for i, k := range keys {
			if i > 0 {
				s += ", "
			}
			s += fmt.Sprintf("%s=%s", k, v.Tags[k])
		}
		s += "}"
	}
	return s
}

// CoveragePoint describes a rule state worth reporting in the property-test
// summary.
type CoveragePoint struct {
	Name        string
	Description string
	MinVerified int
}

// CoverageSummaryLine is one row in the aggregate umpire coverage summary.
type CoverageSummaryLine struct {
	Rule        string
	Point       string
	Description string
	MinVerified int
	Reached     int
	Verified    int
}

// RuleContext lets rules report coverage and violations while checking
// history.
type RuleContext struct {
	rule       string
	collector  *coverageCollector
	runID      int64
	violations []Violation
}

func (c *RuleContext) Reached(point CoveragePoint, key any) {
	if c == nil || c.collector == nil {
		return
	}
	c.collector.markReached(c.runID, c.rule, point.Name, key)
}

func (c *RuleContext) Verified(point CoveragePoint, key any) {
	if c == nil || c.collector == nil {
		return
	}
	c.collector.markVerified(c.runID, c.rule, point.Name, key)
}

func (c *RuleContext) Check(
	point CoveragePoint,
	key any,
	ok bool,
	message string,
	tags map[string]string,
) {
	c.Reached(point, key)
	if ok {
		c.Verified(point, key)
		return
	}
	c.ViolatePoint(point, message, tags)
}

func (c *RuleContext) Violate(message string, tags map[string]string) {
	c.violate("", message, tags)
}

func (c *RuleContext) ViolatePoint(point CoveragePoint, message string, tags map[string]string) {
	c.violate(point.Name, message, tags)
}

func (c *RuleContext) violate(point string, message string, tags map[string]string) {
	if c == nil {
		return
	}
	c.violations = append(c.violations, Violation{
		Rule:    c.rule,
		Point:   point,
		Message: message,
		Tags:    tags,
	})
}

func (c *RuleContext) ruleViolations() []Violation {
	if c == nil || len(c.violations) == 0 {
		return nil
	}
	violations := make([]Violation, len(c.violations))
	copy(violations, c.violations)
	return violations
}

// SafetyRule checks invariants that must hold at every observation point.
// Violations indicate immediate bugs. Check is a pure function over the
// history — derive any working state from the history rather than holding
// it across calls.
//
// At least one of Check or Examples must be non-nil; an empty rule is
// rejected at registration time. Examples is a closure where the rule
// author writes example-based unit tests for the rule using t.Run; the *T
// passed in auto-parallels every subtest, so callers don't need to invoke
// t.Parallel themselves. RuleSet.RunExamples wraps the closure in a
// parallel subtest named after the rule.
type SafetyRule struct {
	Name     string
	Coverage []CoveragePoint
	Check    func(ctx *RuleContext, history []*Record)
	Examples func(t *parallelsuite.T)
}

// LivenessRule tracks conditions that must eventually be satisfied. Check is
// invoked repeatedly with final=false during a run and once with final=true
// at the end; on the final call, any pending-but-unresolved conditions
// should be reported as violations. Check should be a pure function over its
// inputs — derive pending state from the history rather than holding it
// across calls.
//
// At least one of Check or Examples must be non-nil; an empty rule is
// rejected at registration time. Examples follows the same convention as
// SafetyRule.Examples.
type LivenessRule struct {
	Name     string
	Coverage []CoveragePoint
	Check    func(ctx *RuleContext, history []*Record, final bool)
	Examples func(t *parallelsuite.T)
}

// HistoryFromFacts turns an ordered list of facts into a slice of *Record
// with sequential Seq values, suitable for passing to a rule's Check from
// within an Example.
func HistoryFromFacts(facts ...Fact) []*Record {
	out := make([]*Record, len(facts))
	for i, f := range facts {
		out[i] = &Record{Seq: int64(i + 1), Fact: f}
	}
	return out
}
