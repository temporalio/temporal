package testrunner

import (
	"fmt"
	"io"
	"strings"
)

type goTestConsole struct {
	output io.Writer
}

func newGoTestConsole(output io.Writer) *goTestConsole {
	return &goTestConsole{output: output}
}

func (c *goTestConsole) packageOutput(output string) {
	_, _ = fmt.Fprint(c.output, output)
}

func (c *goTestConsole) unstructuredOutput(output string) {
	_, _ = fmt.Fprintln(c.output, output)
}

func (c *goTestConsole) completeTest(execution testExecution, hasFailedDescendant bool) {
	// Only failing tests are shown; passing and skipped test output is hidden
	// from the live console, since their framing and body add no signal there.
	if execution.outcome != testFailed || hasFailedDescendant && !execution.failure.actionable {
		return
	}
	_, _ = fmt.Fprint(c.output, goTestLiveOutput(execution.output))
}

func (c *goTestConsole) incompleteDiagnostics(execution testExecution, diagnostics []diagnostic) {
	if len(diagnostics) == 0 {
		return
	}
	_, _ = fmt.Fprintf(c.output, "=== RUN   %s\n", execution.id.testName)
	for _, diagnostic := range diagnostics {
		_, _ = fmt.Fprintln(c.output, strings.TrimRight(diagnostic.details, "\n"))
	}
}

func (c *goTestConsole) finish(result attemptResult) {
	var tests, skipped, failures, buildErrors int
	for _, pkg := range result.packages {
		packageHasFailedTest := false
		for _, execution := range pkg.executions {
			if execution.outcome == testIncomplete {
				continue
			}
			tests++
			switch execution.outcome {
			case testFailed:
				failures++
				packageHasFailedTest = true
			case testSkipped:
				skipped++
			default:
			}
		}
		if pkg.outcome == packageFailed && !packageHasFailedTest {
			failures++
		}
	}
	for _, build := range result.builds {
		if build.failed {
			buildErrors++
		}
	}
	if len(result.packages) == 0 && len(result.builds) == 0 {
		return
	}

	var summary strings.Builder
	fmt.Fprintf(&summary, "\nDONE %d tests", tests)
	if skipped > 0 {
		fmt.Fprintf(&summary, ", %d skipped", skipped)
	}
	if failures > 0 {
		label := "failure"
		if failures > 1 {
			label = "failures"
		}
		fmt.Fprintf(&summary, ", %d %s", failures, label)
	}
	if buildErrors > 0 {
		label := "error"
		if buildErrors > 1 {
			label = "errors"
		}
		fmt.Fprintf(&summary, ", %d %s", buildErrors, label)
	}
	fmt.Fprintf(&summary, " in %.3fs\n", result.process.duration.Seconds())
	_, _ = fmt.Fprint(c.output, summary.String())
}

func goTestLiveOutput(output string) string {
	var live strings.Builder
	for line := range strings.Lines(output) {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "=== PAUSE ") ||
			strings.HasPrefix(trimmed, "=== CONT  ") ||
			strings.HasPrefix(trimmed, "=== NAME  ") {
			continue
		}
		live.WriteString(line)
	}
	return live.String()
}
