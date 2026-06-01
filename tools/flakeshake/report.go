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
type aggregator struct {
	roundsFailed int
	failedTests  map[string]int    // test name -> rounds it failed in
	details      map[string]string // test name -> abridged failure detail
	timeouts     map[string]int    // test name -> rounds it timed out in
	alerts       map[string]*alertAgg
	lastStatus   string
}

func newAggregator() *aggregator {
	return &aggregator{
		failedTests: map[string]int{},
		details:     map[string]string{},
		timeouts:    map[string]int{},
		alerts:      map[string]*alertAgg{},
	}
}

// addRound folds one round's output into the aggregate.
func (a *aggregator) addRound(out string, failed bool) {
	if failed {
		a.roundsFailed++
	}

	for _, tc := range failedTestcases(out) {
		// Only count tests that have their own failure detail. This drops parent/suite
		// aggregation entries (which fail solely because a subtest did) so the report shows
		// the actual failing test for each flake — no more, no less.
		d := gotestparse.ParseFailureDetails(tc.Failure.Data)
		if d == gotestparse.NoFailureDetails {
			continue
		}
		a.failedTests[tc.Name]++
		if _, ok := a.details[tc.Name]; !ok {
			a.details[tc.Name] = clip(d, maxDetailBytes)
		}
	}

	if _, timedOut := gotestparse.ParseTestTimeouts(out); len(timedOut) > 0 {
		for _, name := range timedOut {
			a.timeouts[name]++
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

	a.lastStatus = roundStatus(failed, out)
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

func roundStatus(failed bool, out string) string {
	if !failed {
		return "pass"
	}
	if _, timedOut := gotestparse.ParseTestTimeouts(out); len(timedOut) > 0 {
		return "FAIL (timeout)"
	}
	return "FAIL"
}

type reportMeta struct {
	rounds   int
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
	fmt.Fprintf(&b, "- **Rounds**: %d (%d had failures)\n", m.rounds, a.roundsFailed)
	fmt.Fprintf(&b, "- **Duration**: %s\n", m.elapsed.Round(time.Second))
	fmt.Fprintf(&b, "- **Latency** (rpc %gx, persistence %gx): %s\n", m.rpcMul, m.persMul, m.baseNote)
	fmt.Fprintf(&b, "- **Distinct flakes**: %d\n\n", len(a.failedTests))
	fmt.Fprintf(&b, "```\ngo %s\n```\n\n", strings.Join(m.goArgs, " "))

	if len(a.failedTests) == 0 {
		b.WriteString("No flakes reproduced.\n\n")
	} else {
		fmt.Fprintf(&b, "## Flakes found (%d)\n\n", len(a.failedTests))
		for _, name := range sortedByCountDesc(a.failedTests) {
			to := ""
			if a.timeouts[name] > 0 {
				to = fmt.Sprintf(", timed out in %d", a.timeouts[name])
			}
			fmt.Fprintf(&b, "### `%s` — %d/%d rounds%s\n\n", name, a.failedTests[name], m.rounds, to)
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
			fmt.Fprintf(&b, "### %s: %s (%d/%d rounds)\n\n", al.typ, al.summary, al.rounds, m.rounds)
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

func clip(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "\n… (truncated)"
	}
	return s
}
