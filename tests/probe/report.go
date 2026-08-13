package probe

import (
	"fmt"
	"strings"
	"testing"

	umpire "go.temporal.io/server/common/testing/umpire"
)

// Verdict is a scenario's model-derived outcome.
type Verdict int

const (
	// Recovered: reached a Success terminal (or an untagged terminal) despite the fault.
	Recovered Verdict = iota
	// Degraded: reached a Failure terminal — a modeled, acceptable failure outcome.
	Degraded
	// Flagged: a rulebook violation — the real bug.
	Flagged
	// Unreached: never settled (and no violation) — the fault kept the target out of reach.
	Unreached
)

func (v Verdict) String() string {
	switch v {
	case Recovered:
		return "recovered"
	case Degraded:
		return "degraded (modeled failure)"
	case Flagged:
		return "FLAGGED"
	case Unreached:
		return "target not reached"
	default:
		return "?"
	}
}

// Report is the outcome of a probe: the plan, the observed happy-path footprint,
// the fault-free baseline, one scenario per faulted call, and the transition coverage
// the run accumulated.
type Report struct {
	Entity, State string
	Routes        [][]string
	Observed      []string // gRPC calls the happy path made (when FaultEachObservedCall is used)
	Baseline      Scenario
	Scenarios     []Scenario
	Coverage      CoverageReport
}

// Scenario is what happened for one drive: whether the fault fired, the terminal
// state (and its modeled disposition) the target reached, the rules the Monitor
// flagged, and the resulting Verdict.
type Scenario struct {
	Label       string
	Method      string
	Fired       bool
	Terminal    string
	Disposition umpire.Disposition
	Violations  []string
	DriveErr    error
	Verdict     Verdict
}

func (r Report) log(t *testing.T) {
	t.Helper()
	t.Logf("[probe] ===== resilience report: reach %s:%s via %v =====", r.Entity, r.State, r.Routes)
	if len(r.Observed) > 0 {
		t.Logf("[probe] observed footprint (%d calls): %v", len(r.Observed), shortNames(r.Observed))
	}
	logScenario(t, r.Baseline)
	var byVerdict [4]int
	for _, sc := range r.Scenarios {
		logScenario(t, sc)
		byVerdict[sc.Verdict]++
	}
	if len(r.Scenarios) > 0 {
		t.Logf("[probe] summary: %d faulted call(s) -> %d recovered, %d degraded, %d FLAGGED, %d target-not-reached",
			len(r.Scenarios), byVerdict[Recovered], byVerdict[Degraded], byVerdict[Flagged], byVerdict[Unreached])
	}
	if cov := r.Coverage; cov.Total > 0 {
		t.Logf("[probe] transition coverage for %s: %d/%d edges exercised", cov.Entity, cov.Covered, cov.Total)
		for _, ec := range cov.Edges {
			mark := " "
			if ec.Covered {
				mark = "x"
			}
			t.Logf("[probe]   [%s] %s --%s--> %s", mark, ec.Edge.From, ec.Edge.Event, ec.Edge.To)
		}
		if miss := cov.Missing(); len(miss) > 0 {
			t.Logf("[probe] %d valid transition(s) NOT yet exercised (drive more outcomes to cover them)", len(miss))
		}
	}
}

func logScenario(t *testing.T, sc Scenario) {
	t.Helper()
	detail := sc.Verdict.String()
	switch sc.Verdict {
	case Flagged:
		detail += ": " + strings.Join(sc.Violations, ", ")
	case Recovered, Degraded:
		if sc.Terminal != "" {
			detail += " [" + sc.Terminal + "]"
		}
	}
	if sc.DriveErr != nil {
		detail += ": drive error: " + sc.DriveErr.Error()
	}
	fired := ""
	if sc.Method != "" {
		fired = fmt.Sprintf("  (fault fired=%v)", sc.Fired)
	}
	t.Logf("[probe]   %-24s -> %s%s", sc.Label, detail, fired)
}
