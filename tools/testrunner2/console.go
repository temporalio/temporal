package testrunner2

import (
	"cmp"
	"fmt"
	"strings"
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
	testsByFile := make(map[string][]string)
	fileOrder := make([]string, 0)
	for _, u := range units {
		for _, tf := range u.files {
			if _, seen := testsByFile[tf.path]; !seen {
				fileOrder = append(fileOrder, tf.path)
				testsByFile[tf.path] = nil
			}
			for _, tc := range tf.tests {
				testsByFile[tf.path] = append(testsByFile[tf.path], tc.name)
			}
		}
	}
	for _, path := range fileOrder {
		sb.WriteString("\n  ")
		sb.WriteString(path)
		if tests := testsByFile[path]; len(tests) > 0 {
			sb.WriteString(" (")
			sb.WriteString(strings.Join(tests, ", "))
			sb.WriteString(")")
		}
	}
}

func formatWorkUnitsByFile(sb *strings.Builder, units []workUnit) {
	seen := make(map[string]bool)
	for _, u := range units {
		for _, tf := range u.files {
			if !seen[tf.path] {
				seen[tf.path] = true
				sb.WriteString("\n  ")
				sb.WriteString(tf.path)
			}
		}
	}
}
