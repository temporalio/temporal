package testrunner2

import (
	"slices"
	"strings"
)

type failureKind string

const (
	failureKindDataRace failureKind = "race"
	failureKindPanic    failureKind = "panic"
	failureKindFatal    failureKind = "fatal"
	failureKindTimeout  failureKind = "timeout"
	failureKindCrash    failureKind = "crash"
)

// alert captures a prominent issue detected from stdout/stderr of test runs.
type alert struct {
	Kind    failureKind
	Summary string
	Tests   []string
}

// alerts is a collection of alert items.
type alerts []alert

// dedupe removes duplicate alerts (e.g., repeated across window trims) based
// on kind, summary, and test names while preserving the first-seen order.
func (a alerts) dedupe() alerts {
	seen := make(map[string]struct{}, len(a))
	var out alerts
	for _, alert := range a {
		key := string(alert.Kind) + "\n" + alert.Summary + "\n" + strings.Join(alert.Tests, ",")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, alert)
	}
	return out
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

// splitTestName splits a possibly fully-qualified Go test name into a package
// (classname) and a short test name.
//
//	"go.temporal.io/server/pkg.TestFoo.func1" → ("go.temporal.io/server/pkg", "TestFoo")
//	"TestFoo/SubTest" → ("", "TestFoo/SubTest")
func splitTestName(fullName string) (classname, name string) {
	if idx := strings.LastIndex(fullName, ".Test"); idx >= 0 {
		classname = fullName[:idx]
		name = fullName[idx+1:]
	} else {
		name = fullName
	}
	// Strip anonymous function suffixes like .func1
	if i := strings.Index(name, ".func"); i >= 0 {
		name = name[:i]
	}
	return classname, name
}

// parseAlerts scans a gotestsum/go test stdout stream and extracts high-priority
// alerts such as data races, panics, and timeouts.
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
		if timeoutAlerts, next, ok := tryParseTimeout(lines, i, line); ok {
			alerts = append(alerts, timeoutAlerts...)
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
	for line := range strings.SplitSeq(block, "\n") {
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
		Kind:    failureKindDataRace,
		Summary: "Data race detected",
		Tests:   extractTestNames(block),
	}, end, true
}

// tryParsePanic parses a non-timeout panic alert at position i if present.
func tryParsePanic(lines []string, i int, line string) (alert, int, bool) {
	if !strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "panic: test timed out after") {
		return alert{}, i, false
	}
	return tryParsePrefixedAlert(lines, i, line, "panic: ", failureKindPanic)
}

// tryParseFatal parses a runtime fatal error alert at position i if present.
func tryParseFatal(lines []string, i int, line string) (alert, int, bool) {
	if !strings.HasPrefix(line, "fatal error: ") {
		return alert{}, i, false
	}
	return tryParsePrefixedAlert(lines, i, line, "fatal error: ", failureKindFatal)
}

// tryParsePrefixedAlert parses a block alert that starts with a known prefix.
func tryParsePrefixedAlert(lines []string, i int, line, prefix string, kind failureKind) (alert, int, bool) {
	block, end := collectBlock(lines, i, shouldStopOnTestBoundary)
	return alert{
		Kind:    kind,
		Summary: strings.TrimSpace(strings.TrimPrefix(line, prefix)),
		Tests:   extractTestNames(block),
	}, end, true
}

func tryParseTimeout(lines []string, i int, line string) ([]alert, int, bool) {
	if !strings.HasPrefix(line, "panic: test timed out after") {
		return nil, i, false
	}
	block, end := collectBlock(lines, i, shouldStopOnTestBoundary)
	summary := strings.TrimSpace(strings.TrimPrefix(line, "panic: "))

	// Extract timed out test names from the block
	var tests []string
	for l := range strings.SplitSeq(block, "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "Test") && strings.Contains(l, " ") {
			tests = append(tests, strings.Split(l, " ")[0])
		}
	}

	// Filter out parent tests when their subtests are present.
	// E.g., if TestFoo1 and TestFoo1/SubTest1 are both listed, only keep TestFoo1/SubTest1.
	slices.Sort(tests)
	filtered := filterParentNames(tests)

	// Create a separate alert for each test
	var out []alert
	for _, t := range filtered {
		out = append(out, alert{
			Kind:    failureKindTimeout,
			Summary: summary,
			Tests:   []string{t},
		})
	}
	return out, end, true
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

// testifyErrorSections are the section headers used by testify assertions
var testifyErrorSections = []string{"Error Trace:", "Error:", "Messages:"}

// extractErrorTrace parses test failure output and extracts just the testify
// error sections (Error Trace, Error, Messages), filtering out log lines.
func extractErrorTrace(output []string) string {
	var result []string
	inErrorSection := false

	for _, line := range output {
		trimmed := strings.TrimSpace(line)

		// Skip the "Test:" section — the test name is already in the header.
		if strings.HasPrefix(trimmed, "Test:") {
			inErrorSection = false
			continue
		}

		// Check for testify section headers
		if isTestifySection(trimmed) {
			inErrorSection = true
			result = append(result, line)
			continue
		}

		// If we're in an error section, keep continuation lines (indented lines)
		if inErrorSection {
			if strings.HasPrefix(line, "\t\t") || strings.HasPrefix(line, "        ") {
				result = append(result, line)
				continue
			}
			if trimmed != "" && !strings.HasPrefix(line, "\t") && !strings.HasPrefix(line, "    ") {
				inErrorSection = false
			} else if trimmed != "" {
				result = append(result, line)
				continue
			}
		}
	}

	return strings.Join(result, "\n")
}

func isTestifySection(trimmed string) bool {
	for _, section := range testifyErrorSections {
		if strings.HasPrefix(trimmed, section) {
			return true
		}
	}
	return false
}
