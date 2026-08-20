package testrunner

import (
	"fmt"
	"slices"
	"strings"
)

const goTestFailLinePrefix = "--- FAIL:"

type outputScope struct {
	packageName string
	test        *testID
}

func extractFailureEvidence(output string) failureEvidence {
	lines := normalizedFailureLines(output)

	// Prefer assertion blocks because they contain the useful testify failure
	// detail and can be selected from the end while ignoring trailing logs.
	if block, ok := findLastAssertionFailureBlock(lines); ok {
		return failureEvidence{details: block, actionable: true}
	}
	if diagnostics := extractDiagnostics(outputScope{}, output); len(diagnostics) > 0 {
		details := make([]string, 0, len(diagnostics))
		for _, diagnostic := range diagnostics {
			if detail := strings.TrimSpace(diagnostic.details); detail != "" {
				details = append(details, detail)
			}
		}
		if len(details) > 0 {
			return failureEvidence{details: strings.Join(details, "\n\n"), actionable: true}
		}
	}
	// Some failures, such as gomock errors, do not include an Error Trace.
	// Fall back to the last Go test output block in those cases.
	if start, end, ok := findLastTestOutputFailureBlock(lines); ok {
		details := strings.Join(lines[start:end], "\n")
		return failureEvidence{
			details: details,
			actionable: strings.Contains(details, "Unexpected call to") ||
				strings.Contains(details, "missing call(s) to") ||
				strings.Contains(details, "test exceeded timeout") ||
				strings.Contains(details, "panic:") ||
				strings.Contains(details, "WARNING: DATA RACE") ||
				strings.Contains(details, "fatal error:"),
		}
	}
	return failureEvidence{}
}

func normalizedFailureLines(data string) []string {
	lines := strings.Split(strings.ReplaceAll(data, "\r\n", "\n"), "\n")
	for len(lines) > 0 {
		trimmed := strings.TrimSpace(lines[len(lines)-1])
		if trimmed != "" && trimmed != "FAIL" {
			break
		}
		lines = lines[:len(lines)-1]
	}
	return lines
}

func findLastAssertionFailureBlock(lines []string) (string, bool) {
	var failLine string
	for i, rawLine := range slices.Backward(lines) {
		line := strings.TrimSpace(rawLine)
		if failLine == "" && strings.HasPrefix(line, goTestFailLinePrefix) {
			// Keep the final Go test failure line because it carries the test duration.
			failLine = line
			continue
		}
		if !strings.Contains(rawLine, "Error Trace:") {
			continue
		}

		// Include the nearest preceding line when present. For testify this is
		// the file header; for await failures this is the attempt marker.
		start := i
		for previous, previousLine := range slices.Backward(lines[:i]) {
			if strings.TrimSpace(previousLine) != "" {
				start = previous
				break
			}
		}
		out := append([]string{}, lines[start:endOfAssertionBlock(lines, i+1)]...)
		if failLine != "" {
			out = append(out, "", failLine)
		}
		return strings.Join(out, "\n"), true
	}
	return "", false
}

func endOfAssertionBlock(lines []string, start int) int {
	sawTestLine := false
	for i := start; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, goTestFailLinePrefix) {
			return i
		}
		if strings.HasPrefix(line, "Test:") {
			sawTestLine = true
			continue
		}
		// Logs written after the Test line are not part of the assertion block.
		if sawTestLine && isTestOutputLine(lines[i]) {
			return i
		}
	}
	return len(lines)
}

func findLastTestOutputFailureBlock(lines []string) (start, end int, ok bool) {
	for start, line := range slices.Backward(lines) {
		if !isTestOutputLine(line) {
			continue
		}
		end := start + 1
		for end < len(lines) && !isTestOutputLine(lines[end]) && lines[end] != "" {
			end++
		}
		return start, end, true
	}
	return 0, 0, false
}

// isTestOutputLine reports whether line is a Go test-framework output line,
// i.e. "    file.go:N: …" — exactly 4 spaces then a non-whitespace character.
// Testify assertion content is indented further (8+ spaces or tabs), so this
// distinguishes log entries from assertion block content.
func isTestOutputLine(line string) bool {
	return len(line) > 4 && line[:4] == "    " && line[4] != ' ' && line[4] != '\t'
}

// extractDiagnostics scans one event-owned output scope for high-priority
// diagnostics such as data races, panics, fatals, and test-binary timeouts.
func extractDiagnostics(scope outputScope, output string) []diagnostic {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	var diagnostics []diagnostic
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		var parsed diagnostic
		var next int
		switch {
		case strings.HasPrefix(line, "WARNING: DATA RACE"):
			parsed, next = tryParseDataRace(lines, i)
		case strings.HasPrefix(line, "panic: ") && !strings.HasPrefix(line, "panic: test timed out after"):
			parsed, next = tryParsePanic(lines, i)
		case strings.HasPrefix(line, "fatal error: "):
			parsed, next = tryParseFatal(lines, i)
		default:
			continue
		}
		parsed.packageName = scope.packageName
		parsed.tests = diagnosticTestIDs(scope, extractTestNames(parsed.details))
		diagnostics = append(diagnostics, parsed)
		i = next
	}
	if timeout := extractTimeoutDiagnostic(scope, output); timeout != nil {
		diagnostics = append(diagnostics, *timeout)
	}
	return diagnostics
}

func diagnosticTestIDs(scope outputScope, names []string) []testID {
	if scope.test != nil {
		return []testID{*scope.test}
	}
	if scope.packageName == "" {
		return nil
	}
	var ids []testID
	seen := make(map[testID]struct{})
	for _, name := range names {
		if index := strings.LastIndex(name, ".Test"); index >= 0 {
			name = name[index+1:]
		}
		id := testID{scope.packageName, name}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids
}

func collectAttemptDiagnostics(result attemptResult) []diagnostic {
	var diagnostics []diagnostic
	for _, pkg := range result.packages {
		for _, execution := range pkg.executions {
			id := execution.id
			diagnostics = append(diagnostics, extractDiagnostics(outputScope{
				packageName: pkg.name,
				test:        &id,
			}, execution.output)...)
		}
		diagnostics = append(diagnostics, extractDiagnostics(outputScope{packageName: pkg.name}, pkg.output)...)
	}
	diagnostics = append(diagnostics, extractDiagnostics(outputScope{}, result.process.stderr)...)
	diagnostics = append(diagnostics, extractDiagnostics(outputScope{}, result.unstructuredOutput)...)
	for i := range diagnostics {
		diagnostics[i].tests = slices.DeleteFunc(diagnostics[i].tests, func(id testID) bool {
			return !result.observed(id)
		})
	}
	return dedupeDiagnostics(diagnostics)
}

func dedupeDiagnostics(diagnostics []diagnostic) []diagnostic {
	seen := make(map[string]struct{}, len(diagnostics))
	result := make([]diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		tests := slices.Clone(diagnostic.tests)
		slices.SortFunc(tests, compareTestID)
		var key strings.Builder
		key.WriteString(fmt.Sprintf("%d\n%s\n%s", diagnostic.kind, diagnostic.packageName, diagnostic.details))
		for _, id := range tests {
			key.WriteString("\n" + id.packageName + "\t" + id.testName)
		}
		if _, ok := seen[key.String()]; ok {
			continue
		}
		seen[key.String()] = struct{}{}
		result = append(result, diagnostic)
	}
	return result
}

// tryParseDataRace parses a data race diagnostic at position index.
func tryParseDataRace(lines []string, index int) (diagnostic, int) {
	start := findRaceBlockStart(lines, index)
	// Merge contiguous race-report sections into a single diagnostic. The Go race
	// detector may emit multiple "WARNING: DATA RACE" blocks back-to-back,
	// each wrapped by a line of ==================. Treat adjacent sections as
	// a single logical diagnostic until we either hit a test boundary or a race
	// boundary that is not followed by another race section.
	block, end := collectBlock(lines, start, func(current string, currentIndex, start int) bool {
		// Stop at PASS/FAIL boundaries always.
		if isTestResultBoundary(current) {
			return true
		}
		// If we hit a race boundary after we've started, only stop if the next
		// non-current line does not continue the race report.
		if currentIndex > start && isRaceBoundary(current) {
			if currentIndex+1 < len(lines) {
				next := strings.TrimSpace(lines[currentIndex+1])
				if isRaceBoundary(next) || strings.HasPrefix(next, "WARNING: DATA RACE") {
					return false
				}
			}
			return true
		}
		return false
	})
	return diagnostic{
		kind:    diagnosticDataRace,
		summary: "Data race detected",
		details: block,
	}, end
}

// tryParsePanic parses a non-timeout panic diagnostic at position index.
func tryParsePanic(lines []string, index int) (diagnostic, int) {
	block, end := collectBlock(lines, index, shouldStopOnTestBoundary)
	return diagnostic{
		kind:    diagnosticPanic,
		summary: strings.TrimSpace(strings.TrimPrefix(lines[index], "panic: ")),
		details: block,
	}, end
}

// tryParseFatal parses a runtime fatal error diagnostic at position index.
func tryParseFatal(lines []string, index int) (diagnostic, int) {
	block, end := collectBlock(lines, index, shouldStopOnTestBoundary)
	return diagnostic{
		kind:    diagnosticFatal,
		summary: strings.TrimSpace(strings.TrimPrefix(lines[index], "fatal error: ")),
		details: block,
	}, end
}

// extractTestNames tries to identify Go test function names from a log block.
// It looks for fully-qualified names like pkg.TestXxx(...) and Go test failure lines.
func extractTestNames(block string) []string {
	var tests []string
	seen := make(map[string]struct{})
	for line := range strings.SplitSeq(block, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if name, ok := parseTripleDashTestName(line); ok {
			addUniqueTest(&tests, seen, name)
			continue
		}
		if name, ok := parseFullyQualifiedTestName(line); ok {
			addUniqueTest(&tests, seen, name)
			continue
		}
		if name, ok := parsePlainTestName(line); ok {
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

// parseTripleDashTestName parses Go test failure lines and returns the test name if present.
func parseTripleDashTestName(line string) (string, bool) {
	if !strings.HasPrefix(line, goTestFailLinePrefix) {
		return "", false
	}
	name := strings.TrimSpace(strings.TrimPrefix(line, goTestFailLinePrefix))
	name, _, _ = strings.Cut(name, " ")
	if !strings.HasPrefix(name, "Test") {
		return "", false
	}
	return name, true
}

// parseFullyQualifiedTestName extracts names like "pkg/path.TestName" from a line.
func parseFullyQualifiedTestName(line string) (string, bool) {
	index := strings.Index(line, ".Test")
	if index < 0 {
		return "", false
	}
	// Include the package/path qualifier preceding ".Test"
	start := 0
	if space := strings.LastIndex(line[:index], " "); space >= 0 {
		start = space + 1
	}
	if parenthesis := strings.Index(line[index:], "("); parenthesis > 0 {
		return line[start : index+parenthesis], true
	}
	return "", false
}

// parsePlainTestName extracts a leading "TestName(" form.
func parsePlainTestName(line string) (string, bool) {
	if !strings.HasPrefix(line, "Test") || !strings.Contains(line, "(") {
		return "", false
	}
	name := line
	if parenthesis := strings.Index(name, "("); parenthesis > 0 {
		name = name[:parenthesis]
	}
	return name, true
}

// findRaceBlockStart searches upward for the race report delimiter.
func findRaceBlockStart(lines []string, index int) int {
	start := index
	for i := index - 1; i >= 0; i-- {
		if isRaceBoundary(lines[i]) {
			start = i
			break
		}
	}
	return start
}

// collectBlock builds a block from start until the stop condition is met.
func collectBlock(lines []string, start int, stop func(line string, index, start int) bool) (string, int) {
	var block strings.Builder
	for i := start; i < len(lines); i++ {
		block.WriteString(lines[i])
		block.WriteByte('\n')
		if stop(lines[i], i, start) {
			return block.String(), i
		}
	}
	return block.String(), len(lines) - 1
}

func isRaceBoundary(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "==================")
}

func isTestResultBoundary(line string) bool {
	return strings.HasPrefix(line, "FAIL") || strings.HasPrefix(line, "PASS")
}

func shouldStopOnTestBoundary(line string, _, _ int) bool {
	return isTestResultBoundary(line)
}

func packageAbortLogSummary(details string) string {
	headline, _, _ := strings.Cut(details, "\n")
	for line := range strings.Lines(details) {
		cause := strings.TrimSpace(line)
		if strings.Contains(line, "\tfatal\t") ||
			strings.HasPrefix(cause, "panic:") ||
			strings.HasPrefix(cause, "fatal error:") ||
			strings.HasPrefix(cause, "WARNING: DATA RACE") ||
			strings.HasPrefix(cause, "signal: ") ||
			strings.HasPrefix(cause, "exit status ") ||
			strings.HasPrefix(cause, "*** Test killed") ||
			strings.Contains(cause, "test exceeded timeout") {
			return fmt.Sprintf("likely cause: %s\n%s", cause, headline)
		}
	}
	return headline
}
