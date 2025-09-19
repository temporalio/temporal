package testrunner

import (
	"fmt"
	"io"
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

// preferFullyQualifiedTestName returns the best display name for a test.
// If the primary name is not fully-qualified (e.g., "TestXxx"), but a
// fully-qualified variant exists in the list (e.g., "pkg/path.TestXxx"),
// this returns the fully-qualified variant.
func preferFullyQualifiedTestName(tests []string) string {
	primary := primaryTestName(tests)
	if primary == "" || strings.Contains(primary, ".Test") {
		return primary
	}
	// Try to find an FQN that ends with "."+primary
	suffix := "." + primary
	for _, t := range tests {
		if strings.HasSuffix(t, suffix) {
			return t
		}
	}
	return primary
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
	// Include the package/path qualifier preceding ".Test"
	start := 0
	if sp := strings.LastIndex(line[:idx], " "); sp >= 0 {
		start = sp + 1
	}
	if p := strings.Index(line[idx:], "("); p > 0 {
		return line[start : idx+p], true
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
	// Merge contiguous race-report sections into a single alert. The Go race
	// detector may emit multiple "WARNING: DATA RACE" blocks back-to-back,
	// each wrapped by a line of ==================. Treat adjacent sections as
	// a single logical alert until we either hit a test boundary or a race
	// boundary that is not followed by another race section.
	block, end := collectBlock(lines, start, func(curLine string, idx, start int) bool {
		// Stop at PASS/FAIL boundaries always.
		if isTestResultBoundary(curLine) {
			return true
		}
		// If we hit a race boundary after we've started, only stop if the next
		// non-current line does not continue the race report.
		if idx > start && isRaceBoundary(curLine) {
			if idx+1 < len(lines) {
				next := strings.TrimSpace(lines[idx+1])
				if isRaceBoundary(next) || strings.HasPrefix(next, "WARNING: DATA RACE") {
					return false
				}
			}
			return true
		}
		return false
	})
	return alert{
		Kind:    alertKindDataRace,
		Summary: "Data race detected",
		Details: block,
		Tests:   extractTestNames(block),
	}, end, true
}

// tryParsePanic parses a non-timeout panic alert at position i if present.
func tryParsePanic(lines []string, i int, line string) (alert, int, bool) {
	if !strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "panic: test timed out after") {
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

func shouldStopOnTestBoundary(line string, _ int, _ int) bool {
	return isTestResultBoundary(line)
}
