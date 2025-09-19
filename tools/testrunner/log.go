package testrunner

import (
	"fmt"
	"io"
	"log"
	"slices"
	"strings"

	"github.com/maruel/panicparse/v2/stack"
)

// parseTestTimeouts parses the stdout of a test run and returns the stacktrace and names of tests that timed out.
func parseTestTimeouts(stdout string) (stacktrace string, timedoutTests []string) {
	lines := strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "FAIL") {
			// ignore
		} else if strings.HasPrefix(line, "panic: test timed out after") {
			// parse names of tests that timed out
			for {
				i++
				line = strings.TrimSpace(lines[i])
				if strings.HasPrefix(line, "Test") {
					timedoutTests = append(timedoutTests, strings.Split(line, " ")[0])
				}
				if line == "" {
					break
				}
			}
		} else if len(timedoutTests) > 0 {
			// collect stracktrace
			stacktrace += line + "\n"
		}
	}

	stacktrace = fmt.Sprintf("%d timed out test(s):\n\t%v\n\n%v",
		len(timedoutTests), strings.Join(timedoutTests, "\n\t"), testOnlyStacktrace(stacktrace))
	return
}

// testOnlyStacktrace removes all but the test stacktraces from the full stacktrace.
func testOnlyStacktrace(stacktrace string) string {
	var res string
	snap, _, err := stack.ScanSnapshot(strings.NewReader(stacktrace), io.Discard, stack.DefaultOpts())
	if err != nil && err != io.EOF {
		return fmt.Sprintf("failed to parse stacktrace: %v", err)
	}
	if snap == nil {
		return "failed to find a stacktrace"
	}
	res = "abridged stacktrace:\n"
	for _, goroutine := range snap.Goroutines {
		shouldPrint := slices.ContainsFunc(goroutine.Stack.Calls, func(call stack.Call) bool {
			return strings.HasSuffix(call.RemoteSrcPath, "_test.go")
		})
		if shouldPrint {
			res += fmt.Sprintf("\tgoroutine %d [%v]:\n", goroutine.ID, goroutine.State)
			for _, call := range goroutine.Stack.Calls {
				file := call.RemoteSrcPath
				res += fmt.Sprintf("\t\t%s:%d\n", file, call.Line)
			}
			res += "\n"
		}
	}
	return res
}

// alertKind represents a category of high-priority alert detected in test output.
type alertKind string

const (
	alertKindDataRace alertKind = "DATA RACE"
	alertKindPanic    alertKind = "PANIC"
	alertKindFatal    alertKind = "FATAL"
)

// alert captures a prominent issue detected from stdout/stderr of test runs.
type alert struct {
	Kind    alertKind
	Summary string
	Details string
	Tests   []string
}

// primaryTestName returns a single representative test name for an alert.
// Preference order:
// 1) Fully-qualified test name containing ".Test"
// 2) First detected test name
func primaryTestName(tests []string) string {
	if len(tests) == 0 {
		return ""
	}
	for _, t := range tests {
		if strings.Contains(t, ".Test") {
			return t
		}
	}
	return tests[0]
}

// parseAlerts scans a gotestsum/go test stdout stream and extracts high-priority
// alerts such as data races and panics. It returns a slice of alerts in the
// order they were encountered.
func parseAlerts(stdout string) []alert {
	lines := strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n")
	var alerts []alert

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if a, next, ok := tryParseDataRace(lines, i, line); ok {
			alerts = append(alerts, a)
			i = next
			continue
		}
		if a, next, ok := tryParsePanic(lines, i, line); ok {
			alerts = append(alerts, a)
			i = next
			continue
		}
		if a, next, ok := tryParseFatal(lines, i, line); ok {
			alerts = append(alerts, a)
			i = next
			continue
		}
	}

	return alerts
}

// extractTestNames tries to identify Go test function names from a log block.
// It looks for fully-qualified names like pkg.TestXxx(...) and '--- FAIL: TestXxx' lines.
func extractTestNames(block string) []string {
	var tests []string
	seen := make(map[string]struct{})
	for _, line := range strings.Split(block, "\n") {
		l := strings.TrimSpace(line)
		if l == "" {
			continue
		}
		if name, ok := parseTripleDashTestName(l); ok {
			addUniqueTest(&tests, seen, name)
			continue
		}
		if name, ok := parseFullyQualifiedTestName(l); ok {
			addUniqueTest(&tests, seen, name)
			continue
		}
		if name, ok := parsePlainTestName(l); ok {
			addUniqueTest(&tests, seen, name)
		}
	}
	return tests
}

// addUniqueTest appends name to tests if not already seen.
func addUniqueTest(tests *[]string, seen map[string]struct{}, name string) {
	if _, ok := seen[name]; ok {
		return
	}
	seen[name] = struct{}{}
	*tests = append(*tests, name)
}

// parseTripleDashTestName parses lines like:
// "--- FAIL: TestName (0.00s)" and returns the test name if present.
func parseTripleDashTestName(line string) (string, bool) {
	if !strings.HasPrefix(line, "--- ") {
		return "", false
	}
	parts := strings.Split(line, ":")
	if len(parts) < 2 {
		return "", false
	}
	name := strings.TrimSpace(parts[1])
	name = strings.Split(name, " ")[0]
	if !strings.HasPrefix(name, "Test") {
		return "", false
	}
	return name, true
}

// parseFullyQualifiedTestName extracts names like "pkg/path.TestName" from a line.
func parseFullyQualifiedTestName(line string) (string, bool) {
	idx := strings.Index(line, ".Test")
	if idx < 0 {
		return "", false
	}
	rest := line[idx+1:]
	if p := strings.Index(rest, "("); p > 0 {
		fq := rest[:p]
		return fq, true
	}
	return "", false
}

// parsePlainTestName extracts a leading "TestName(" form.
func parsePlainTestName(line string) (string, bool) {
	if !strings.HasPrefix(line, "Test") || !strings.Contains(line, "(") {
		return "", false
	}
	name := line
	if p := strings.Index(name, "("); p > 0 {
		name = name[:p]
	}
	return name, true
}

// tryParseDataRace parses a data race alert at position i if present.
func tryParseDataRace(lines []string, i int, line string) (alert, int, bool) {
	if !strings.HasPrefix(line, "WARNING: DATA RACE") {
		return alert{}, i, false
	}
	start := findRaceBlockStart(lines, i)
	block, end := collectBlock(lines, start, shouldStopDataRace)
	return alert{
		Kind:    alertKindDataRace,
		Summary: "Data race detected",
		Details: block,
		Tests:   extractTestNames(block),
	}, end, true
}

// tryParsePanic parses a non-timeout panic alert at position i if present.
func tryParsePanic(lines []string, i int, line string) (alert, int, bool) {
	if !(strings.HasPrefix(line, "panic: ") && !strings.HasPrefix(line, "panic: test timed out after")) {
		return alert{}, i, false
	}
	block, end := collectBlock(lines, i, shouldStopOnTestBoundary)
	return alert{
		Kind:    alertKindPanic,
		Summary: strings.TrimSpace(strings.TrimPrefix(line, "panic: ")),
		Details: block,
		Tests:   extractTestNames(block),
	}, end, true
}

// tryParseFatal parses a runtime fatal error alert at position i if present.
func tryParseFatal(lines []string, i int, line string) (alert, int, bool) {
	if !strings.HasPrefix(line, "fatal error: ") {
		return alert{}, i, false
	}
	block, end := collectBlock(lines, i, shouldStopOnTestBoundary)
	return alert{
		Kind:    alertKindFatal,
		Summary: strings.TrimSpace(strings.TrimPrefix(line, "fatal error: ")),
		Details: block,
		Tests:   extractTestNames(block),
	}, end, true
}

// findRaceBlockStart searches upward for the race report delimiter.
func findRaceBlockStart(lines []string, i int) int {
	start := i
	for j := i - 1; j >= 0; j-- {
		if isRaceBoundary(lines[j]) {
			start = j
			break
		}
	}
	return start
}

// collectBlock builds a block from start until the stop condition is met.
func collectBlock(lines []string, start int, stop func(line string, idx, start int) bool) (string, int) {
	var b strings.Builder
	end := len(lines) - 1
	for j := start; j < len(lines); j++ {
		b.WriteString(lines[j])
		b.WriteByte('\n')
		if stop(lines[j], j, start) {
			end = j
			break
		}
		end = j
	}
	return b.String(), end
}

func isRaceBoundary(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "==================")
}

func isTestResultBoundary(line string) bool {
	return strings.HasPrefix(line, "FAIL") || strings.HasPrefix(line, "PASS")
}

func shouldStopDataRace(line string, idx, start int) bool {
	if idx > start && isRaceBoundary(line) {
		return true
	}
	return isTestResultBoundary(line)
}

func shouldStopOnTestBoundary(line string, _ int, _ int) bool {
	return isTestResultBoundary(line)
}

// dedupeAlerts removes duplicate alerts (e.g., repeated across retries) based
// on kind and details while preserving the first-seen order.
func dedupeAlerts(alerts []alert) []alert {
	seen := make(map[string]struct{}, len(alerts))
	var out []alert
	for _, a := range alerts {
		key := string(a.Kind) + "\n" + a.Details
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	return out
}

// printAlertsSummary emits a prominent banner with a concise list of alerts,
// followed by their details for quick triage in CI logs.
func printAlertsSummary(alerts []alert) {
	if len(alerts) == 0 {
		return
	}
	banner := strings.Repeat("=", 80)
	log.Printf("\n%s\nALERTS DETECTED: %d\n%s\n", banner, len(alerts), banner)
	for _, a := range alerts {
		// Include originating test when available to aid quick triage in CI logs.
		if len(a.Tests) > 0 {
			primary := primaryTestName(a.Tests)
			if len(a.Tests) == 1 {
				log.Printf("[%s] %s — in %s", a.Kind, a.Summary, primary)
			} else {
				log.Printf("[%s] %s — in %s (+%d more)", a.Kind, a.Summary, primary, len(a.Tests)-1)
			}
		} else {
			log.Printf("[%s] %s", a.Kind, a.Summary)
		}
	}
	log.Printf("%s\nFULL ALERT DETAILS:\n%s", banner, banner)
	for idx, a := range alerts {
		if len(a.Tests) > 0 {
			log.Printf("-- Alert %d: [%s] %s --\nDetected in tests:\n\t%s\n\n%s", idx+1, a.Kind, a.Summary, strings.Join(a.Tests, "\n\t"), a.Details)
		} else {
			log.Printf("-- Alert %d: [%s] %s --\n%s", idx+1, a.Kind, a.Summary, a.Details)
		}
	}
	log.Printf("%s\n", banner)
}
