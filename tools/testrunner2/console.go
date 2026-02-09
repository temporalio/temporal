package testrunner2

import (
	"cmp"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// writeConsoleResult formats and prints the test result to the console.
func writeConsoleResult(r *runner, cfg execConfig, result commandResult,
	numTests, numFailed int, failureKind string, detectedAlerts alerts,
	results testResults, start time.Time) {

	failed := result.exitCode != 0 || numFailed > 0 || numTests == 0
	status := "❌️"
	if !failed {
		if r.progress != nil {
			completed, total := r.progress.complete(1)
			status = fmt.Sprintf("✅ [%d/%d]", completed, total)
		} else {
			status = "✅"
		}
	}
	passedTests := numTests - numFailed
	failureInfo := ""
	if failed {
		failureInfo = fmt.Sprintf(", failure=%s", cmp.Or(failureKind, "failed"))
	}

	var header string
	if failureKind != "" {
		header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/?%s, runtime=%v)",
			logPrefix, time.Now().Format("15:04:05"), status, cfg.label, cfg.attempt, passedTests, failureInfo, time.Since(start).Round(time.Second))
	} else {
		header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/%d%s, runtime=%v)",
			logPrefix, time.Now().Format("15:04:05"), status, cfg.label, cfg.attempt, passedTests, numTests, failureInfo, time.Since(start).Round(time.Second))
	}

	var body strings.Builder

	// Append alerts if test failed
	if failed && len(detectedAlerts) > 0 {
		for _, a := range detectedAlerts.dedupe() {
			if testName := primaryTestName(a.Tests); testName != "" {
				fmt.Fprintf(&body, "--- %s: %s — in %s\n", strings.ToUpper(string(a.Kind)), a.Summary, testName)
			} else {
				fmt.Fprintf(&body, "--- %s: %s\n", strings.ToUpper(string(a.Kind)), a.Summary)
			}
		}
	}

	// Append test failure details
	if failed && len(results.failures) > 0 {
		for _, f := range results.failures {
			fmt.Fprintf(&body, "\n--- %s\n", f.Name)
			if f.ErrorTrace != "" {
				for line := range strings.SplitSeq(f.ErrorTrace, "\n") {
					fmt.Fprintf(&body, "%s\n", line)
				}
			}
		}
	}

	r.console.WriteGrouped(header, body.String())
}

// formatWorkUnits builds a human-readable summary of work units for logging.
func formatWorkUnits(units []workUnit, groupBy GroupMode) string {
	var sb strings.Builder
	if groupBy == GroupByTest {
		formatWorkUnitsByTest(&sb, units)
	} else {
		formatWorkUnitsByFile(&sb, units)
	}
	return sb.String()
}

func formatWorkUnitsByTest(sb *strings.Builder, units []workUnit) {
	testsByPkg := make(map[string][]string)
	pkgOrder := make([]string, 0)
	for _, u := range units {
		if _, seen := testsByPkg[u.pkg]; !seen {
			pkgOrder = append(pkgOrder, u.pkg)
			testsByPkg[u.pkg] = nil
		}
		for _, tc := range u.tests {
			testsByPkg[u.pkg] = append(testsByPkg[u.pkg], tc.name)
		}
	}
	for _, pkg := range pkgOrder {
		sb.WriteString("\n  ")
		sb.WriteString(pkg)
		if tests := testsByPkg[pkg]; len(tests) > 0 {
			sb.WriteString(" (")
			sb.WriteString(strings.Join(tests, ", "))
			sb.WriteString(")")
		}
	}
}

func formatWorkUnitsByFile(sb *strings.Builder, units []workUnit) {
	seen := make(map[string]bool)
	for _, u := range units {
		if !seen[u.pkg] {
			seen[u.pkg] = true
			sb.WriteString("\n  ")
			sb.WriteString(u.pkg)
		}
	}
}

// consoleWriter writes grouped output to a writer.
type consoleWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

// WriteGrouped writes output with a header line and indented body.
func (cw *consoleWriter) WriteGrouped(header, body string) {
	var out strings.Builder
	out.WriteString(header)
	out.WriteByte('\n')

	// Indent body lines
	for line := range strings.SplitSeq(body, "\n") {
		if line != "" {
			out.WriteString("    ")
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}

	cw.mu.Lock()
	_, _ = io.WriteString(cw.w, out.String())
	cw.mu.Unlock()
}
