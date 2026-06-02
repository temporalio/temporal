package flakeshake

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/jstemmer/go-junit-report/v2/parser/gotest"
	"go.temporal.io/server/tools/common/gotestparse"
)

const maxDetailBytes = 2048

type alertAgg struct {
	typ     string
	summary string
	tests   []string
	sample  string
	rounds  int
}

// aggregator folds the output of one or more rounds into a flake report. Failing tests and
// their failure details come from parsing the `go test -v` output into JUnit (via
// go-junit-report) and reusing gotestparse.ParseFailureDetails; data races / panics / fatals
// and timeouts are extracted from the raw output via gotestparse.
//
// Committed state (failedTests/details/timeouts/alerts) is updated once per round in
// commitRound. While a round streams, setLive records the failures seen so far so the report
// can show them immediately without inflating the per-round counts.
type aggregator struct {
	roundsDone   int
	roundsFailed int
	failedTests  map[string]int    // test name -> rounds it failed in
	details      map[string]string // test name -> abridged failure detail
	alerts       map[string]*alertAgg
	lastStatus   string

	liveRound int               // round currently streaming (0 = none)
	live      map[string]string // live failures for that round (test name -> detail)
}

func newAggregator() *aggregator {
	return &aggregator{
		failedTests: map[string]int{},
		details:     map[string]string{},
		alerts:      map[string]*alertAgg{},
	}
}

// setLive records the in-progress round's failures (idempotent; the full set each time).
func (a *aggregator) setLive(round int, failures map[string]string) {
	a.liveRound = round
	a.live = failures
}

// commitRound folds a completed round's output into the aggregate and clears the live view.
// A round counts as failed only if a test actually reported a failure (a "--- FAIL" with its
// own detail). The go test exit code is deliberately ignored: binary timeouts, post-completion
// panics, and other "go test failures" that aren't a reported test failure don't count.
func (a *aggregator) commitRound(out string) {
	a.roundsDone++

	failures := parseFailures(out)
	if len(failures) > 0 {
		a.roundsFailed++
	}
	for name, d := range failures {
		a.failedTests[name]++
		if _, ok := a.details[name]; !ok {
			a.details[name] = d
		}
	}

	seen := map[string]struct{}{}
	for _, al := range gotestparse.ParseAlerts(out) {
		key := string(al.Type) + "|" + gotestparse.PrimaryTestName(al.Tests)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		agg := a.alerts[key]
		if agg == nil {
			agg = &alertAgg{typ: string(al.Type), summary: al.Summary, tests: al.Tests, sample: clip(al.Details, maxDetailBytes)}
			a.alerts[key] = agg
		}
		agg.rounds++
	}

	a.lastStatus = roundStatus(len(failures))
	a.liveRound, a.live = 0, nil
}

// parseFailures extracts failing tests with their own assertion detail from `go test -v`
// output, keyed by test name. Parent/suite aggregation entries (no own detail) are dropped, so
// the report shows the actual failing test for each flake — no more, no less. It is safe to
// call repeatedly on a growing buffer: it returns the full set each time (no double-counting).
func parseFailures(out string) map[string]string {
	res := map[string]string{}
	for _, tc := range failedTestcases(out) {
		d := gotestparse.ParseFailureDetails(tc.Failure.Data)
		if d == gotestparse.NoFailureDetails {
			continue
		}
		if _, ok := res[tc.Name]; !ok {
			res[tc.Name] = clip(d, maxDetailBytes)
		}
	}
	return res
}

// failedTestcases parses `go test -v` output into JUnit testcases and returns the failing ones.
func failedTestcases(out string) []junit.Testcase {
	report, err := gotest.NewParser().Parse(strings.NewReader(out))
	if err != nil {
		return nil
	}
	var failed []junit.Testcase
	for _, suite := range junit.CreateFromReport(report, "").Suites {
		for _, tc := range suite.Testcases {
			if tc.Failure != nil {
				failed = append(failed, tc)
			}
		}
	}
	return failed
}

func roundStatus(numFailures int) string {
	if numFailures == 0 {
		return "pass (no reported test failures)"
	}
	return fmt.Sprintf("FAIL (%d reported)", numFailures)
}

type reportMeta struct {
	budget   string // human-readable run budget, e.g. "duration 30m0s" or "5 rounds"
	elapsed  time.Duration
	goArgs   []string
	rpcMul   float64
	persMul  float64
	baseNote string
}

func (a *aggregator) report(m reportMeta) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# flakeshake report\n\n")
	fmt.Fprintf(&b, "Generated %s.\n\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&b, "- **Rounds completed**: %d (%d had failures) — budget: %s\n", a.roundsDone, a.roundsFailed, m.budget)
	fmt.Fprintf(&b, "- **Elapsed**: %s\n", m.elapsed.Round(time.Second))
	fmt.Fprintf(&b, "- **Latency** (rpc %gx, persistence %gx): %s\n", m.rpcMul, m.persMul, m.baseNote)
	fmt.Fprintf(&b, "- **Distinct flakes**: %d\n\n", len(a.failedTests))
	fmt.Fprintf(&b, "```\ngo %s\n```\n\n", strings.Join(m.goArgs, " "))

	if len(a.live) > 0 {
		fmt.Fprintf(&b, "## Round %d in progress — failing so far (%d)\n\n", a.liveRound, len(a.live))
		for _, name := range sortedKeys(a.live) {
			fmt.Fprintf(&b, "### `%s`\n\n", name)
			if d := a.live[name]; d != "" {
				fmt.Fprintf(&b, "```\n%s\n```\n\n", d)
			}
		}
	}

	if len(a.failedTests) == 0 {
		b.WriteString("No flakes reproduced.\n\n")
	} else {
		fmt.Fprintf(&b, "## Flakes found (%d)\n\n", len(a.failedTests))
		for _, name := range sortedByCountDesc(a.failedTests) {
			fmt.Fprintf(&b, "### `%s` — %d/%d rounds\n\n", name, a.failedTests[name], a.roundsDone)
			if d := a.details[name]; d != "" {
				fmt.Fprintf(&b, "```\n%s\n```\n\n", d)
			}
		}
	}

	if len(a.alerts) > 0 {
		fmt.Fprintf(&b, "## Alerts (%d)\n\n", len(a.alerts))
		keys := make([]string, 0, len(a.alerts))
		for k := range a.alerts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			al := a.alerts[k]
			fmt.Fprintf(&b, "### %s: %s (%d/%d rounds)\n\n", al.typ, al.summary, al.rounds, a.roundsDone)
			if len(al.tests) > 0 {
				fmt.Fprintf(&b, "Tests: %s\n\n", strings.Join(al.tests, ", "))
			}
			fmt.Fprintf(&b, "```\n%s\n```\n\n", al.sample)
		}
	}
	return b.String()
}

// sortedByCountDesc returns map keys ordered by descending count, then name.
func sortedByCountDesc(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	return keys
}

// sortedKeys returns the map keys sorted alphabetically.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func clip(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "\n… (truncated)"
	}
	return s
}
