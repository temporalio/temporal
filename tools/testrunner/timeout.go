package testrunner

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/maruel/panicparse/v2/stack"
)

func extractTimeoutDiagnostic(scope outputScope, output string) *diagnostic {
	if !strings.Contains(output, "panic: test timed out after") {
		return nil
	}
	stacktrace, names := parseTestTimeouts(output)
	details := stacktrace
	if len(names) == 0 {
		details = ""
	}
	diagnostic := &diagnostic{
		kind:        diagnosticTimeout,
		summary:     timeoutHeadline(output),
		details:     details,
		packageName: scope.packageName,
	}
	if scope.test != nil {
		diagnostic.tests = []testID{*scope.test}
		return diagnostic
	}
	if scope.packageName != "" {
		for _, name := range names {
			if name == "" {
				continue
			}
			diagnostic.tests = append(diagnostic.tests, testID{scope.packageName, name})
		}
	}
	return diagnostic
}

func timeoutHeadline(output string) string {
	for line := range strings.Lines(output) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "panic: test timed out after") {
			return line
		}
	}
	return "test timed out"
}

// parseTestTimeouts parses the stdout of a test run and returns the stacktrace and names of tests that timed out.
func parseTestTimeouts(stdout string) (stacktrace string, timedoutTests []string) {
	lines := strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "FAIL") {
			// ignore
		} else if strings.HasPrefix(line, "panic: test timed out after") {
			// parse names of tests that timed out
			for i+1 < len(lines) {
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
			// collect stacktrace
			stacktrace += line + "\n"
		}
	}

	stacktrace = fmt.Sprintf("%d timed out test(s):\n\t%v\n\n%v",
		len(timedoutTests), strings.Join(timedoutTests, "\n\t"), testOnlyStacktrace(stacktrace))
	return
}

// testOnlyStacktrace removes all but the test stacktraces from the full stacktrace.
func testOnlyStacktrace(stacktrace string) string {
	var result string
	snapshot, _, err := stack.ScanSnapshot(strings.NewReader(stacktrace), io.Discard, stack.DefaultOpts())
	if err != nil && err != io.EOF {
		return fmt.Sprintf("failed to parse stacktrace: %v", err)
	}
	if snapshot == nil {
		return "failed to find a stacktrace"
	}
	result = "abridged stacktrace:\n"
	for _, goroutine := range snapshot.Goroutines {
		shouldPrint := slices.ContainsFunc(goroutine.Stack.Calls, func(call stack.Call) bool {
			return strings.HasSuffix(call.RemoteSrcPath, "_test.go")
		})
		if shouldPrint {
			result += fmt.Sprintf("\tgoroutine %d [%v]:\n", goroutine.ID, goroutine.State)
			for _, call := range goroutine.Stack.Calls {
				result += fmt.Sprintf("\t\t%s:%d\n", call.RemoteSrcPath, call.Line)
			}
			result += "\n"
		}
	}
	return result
}
