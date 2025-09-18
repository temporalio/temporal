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

// parseAlerts scans a gotestsum/go test stdout stream and extracts high-priority
// alerts such as data races and panics. It returns a slice of alerts in the
// order they were encountered.
func parseAlerts(stdout string) []alert {
	lines := strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n")
	var alerts []alert

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		// Detect Go race detector reports.
		if strings.HasPrefix(line, "WARNING: DATA RACE") {
			start := i
			for j := i - 1; j >= 0; j-- {
				if strings.HasPrefix(strings.TrimSpace(lines[j]), "==================") {
					start = j
					break
				}
			}
			var b strings.Builder
			for j := start; j < len(lines); j++ {
				b.WriteString(lines[j])
				b.WriteByte('\n')
				if j > start && strings.HasPrefix(strings.TrimSpace(lines[j]), "==================") {
					i = j
					break
				}
				if strings.HasPrefix(lines[j], "FAIL") || strings.HasPrefix(lines[j], "PASS") {
					i = j
					break
				}
			}
			block := b.String()
			alerts = append(alerts, alert{
				Kind:    alertKindDataRace,
				Summary: "Data race detected",
				Details: block,
				Tests:   extractTestNames(block),
			})
			continue
		}

		// Detect panics that are not timeouts.
		if strings.HasPrefix(line, "panic: ") && !strings.HasPrefix(line, "panic: test timed out after") {
			var b strings.Builder
			b.WriteString(line)
			b.WriteByte('\n')
			j := i + 1
			for ; j < len(lines); j++ {
				b.WriteString(lines[j])
				b.WriteByte('\n')
				if strings.HasPrefix(lines[j], "FAIL") || strings.HasPrefix(lines[j], "PASS") {
					break
				}
			}
			block := b.String()
			alerts = append(alerts, alert{
				Kind:    alertKindPanic,
				Summary: strings.TrimSpace(strings.TrimPrefix(line, "panic: ")),
				Details: block,
				Tests:   extractTestNames(block),
			})
			i = j
			continue
		}

		// Detect runtime fatal errors.
		if strings.HasPrefix(line, "fatal error: ") {
			var b strings.Builder
			b.WriteString(line)
			b.WriteByte('\n')
			j := i + 1
			for ; j < len(lines); j++ {
				b.WriteString(lines[j])
				b.WriteByte('\n')
				if strings.HasPrefix(lines[j], "FAIL") || strings.HasPrefix(lines[j], "PASS") {
					break
				}
			}
			block := b.String()
			alerts = append(alerts, alert{
				Kind:    alertKindFatal,
				Summary: strings.TrimSpace(strings.TrimPrefix(line, "fatal error: ")),
				Details: block,
				Tests:   extractTestNames(block),
			})
			i = j
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
		// Match '--- FAIL: TestName' or '--- PASS: TestName' etc.
		if strings.HasPrefix(l, "--- ") {
			// format: --- FAIL: TestName (0.00s)
			parts := strings.Split(l, ":")
			if len(parts) >= 2 {
				name := strings.TrimSpace(parts[1])
				// name may contain duration suffix; split on space
				name = strings.Split(name, " ")[0]
				if strings.HasPrefix(name, "Test") {
					if _, ok := seen[name]; !ok {
						seen[name] = struct{}{}
						tests = append(tests, name)
					}
					continue
				}
			}
		}
		// Match fully-qualified 'pkg/path.TestName(' occurrences.
		// Find ' TestName(' token preceded by a dot.
		// Example: go.temporal.io/server/tools/testrunner.TestShowPanic(...)
		if idx := strings.Index(l, ".Test"); idx >= 0 {
			// Extract token until '('
			rest := l[idx+1:]
			if p := strings.Index(rest, "("); p > 0 {
				fq := rest[:p]
				// Keep the full qualifier to aid identification in CI
				if _, ok := seen[fq]; !ok {
					seen[fq] = struct{}{}
					tests = append(tests, fq)
				}
			}
		}
		// Fallback: plain 'TestName(' at line start
		if strings.HasPrefix(l, "Test") && strings.Contains(l, "(") {
			name := l
			if p := strings.Index(name, "("); p > 0 {
				name = name[:p]
			}
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				tests = append(tests, name)
			}
		}
	}
	return tests
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
		log.Printf("[%s] %s", a.Kind, a.Summary)
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
