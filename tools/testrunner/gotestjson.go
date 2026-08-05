package testrunner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jstemmer/go-junit-report/v2/gtr"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/jstemmer/go-junit-report/v2/parser/gotest"
)

type goTestEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type goTestID struct {
	packageName string
	testName    string
}

type packageAbort struct {
	packageName string
	details     string
}

type goTestJSONOutput struct {
	line                    strings.Builder
	input                   strings.Builder
	output                  strings.Builder
	testOutputs             map[goTestID]*strings.Builder
	testOutputOrder         []goTestID
	terminalResults         map[goTestID]gtr.Result
	failedTests             map[goTestID]struct{}
	failedTestDetails       map[goTestID]string
	packages                map[string]struct{}
	packagesWithFailedTests map[string]struct{}
	tests                   int
	skipped                 int
	failures                int
	errors                  int
	elapsed                 float64
	startTime               time.Time
	endTime                 time.Time
	summaryWritten          bool
	stdout                  io.Writer
}

func newGoTestJSONOutput() *goTestJSONOutput {
	return &goTestJSONOutput{
		testOutputs:             make(map[goTestID]*strings.Builder),
		terminalResults:         make(map[goTestID]gtr.Result),
		failedTests:             make(map[goTestID]struct{}),
		failedTestDetails:       make(map[goTestID]string),
		packages:                make(map[string]struct{}),
		packagesWithFailedTests: make(map[string]struct{}),
		stdout:                  os.Stdout,
	}
}

func (o *goTestJSONOutput) Write(p []byte) (int, error) {
	for _, b := range p {
		o.line.WriteByte(b)
		if b == '\n' {
			o.writeLine(strings.TrimSuffix(o.line.String(), "\n"))
			o.line.Reset()
		}
	}
	return len(p), nil
}

func (o *goTestJSONOutput) finish() string {
	if o.line.Len() > 0 {
		o.writeLine(o.line.String())
		o.line.Reset()
	}
	for _, test := range o.testOutputOrder {
		// A package abort leaves scheduler framing buffered for every unfinished
		// test. Surface high-priority alerts without presenting ordinary output
		// from unfinished tests as live failures, and preserve the full buffers
		// for reports.
		o.writeIncompleteTestAlerts(test, o.testOutputs[test])
		o.flushTestOutput(test, false)
	}
	o.writeSummary()
	return o.output.String()
}

func (o *goTestJSONOutput) writeLine(line string) {
	var event goTestEvent
	if err := o.decodeLine(line, &event); err != nil {
		_, _ = fmt.Fprintln(o.stdout, line)
		o.output.WriteString(line)
		o.output.WriteByte('\n')
		return
	}
	o.input.WriteString(line)
	o.input.WriteByte('\n')
	if event.Output != "" {
		o.writeEventOutput(event)
	}
	o.recordEvent(event)
}

func (o *goTestJSONOutput) writeEventOutput(event goTestEvent) {
	if event.Test == "" {
		o.writeOutput(event.Output)
		return
	}
	test := goTestID{packageName: event.Package, testName: event.Test}
	testOutput, ok := o.testOutputs[test]
	if !ok {
		testOutput = &strings.Builder{}
		o.testOutputs[test] = testOutput
		o.testOutputOrder = append(o.testOutputOrder, test)
	}
	testOutput.WriteString(event.Output)
}

func (o *goTestJSONOutput) recordEvent(event goTestEvent) {
	o.recordTime(event.Time)
	switch event.Action {
	case "bench":
		o.flushTestOutput(goTestID{packageName: event.Package, testName: event.Test}, true)
	case "fail", "pass", "skip":
		o.recordTerminalEvent(event)
	case "start":
		if event.Test == "" && event.Package != "" {
			o.packages[event.Package] = struct{}{}
		}
	case "build-output":
		r, _ := utf8.DecodeRuneInString(event.Output)
		if !strings.HasPrefix(event.Output, "# ") && !unicode.IsSpace(r) {
			o.errors++
		}
	default:
	}
}

func (o *goTestJSONOutput) recordTime(eventTime time.Time) {
	if eventTime.IsZero() {
		return
	}
	if o.startTime.IsZero() || eventTime.Before(o.startTime) {
		o.startTime = eventTime
	}
	if o.endTime.IsZero() || eventTime.After(o.endTime) {
		o.endTime = eventTime
	}
}

func (o *goTestJSONOutput) recordTerminalEvent(event goTestEvent) {
	if event.Test == "" {
		if event.Package != "" {
			o.packages[event.Package] = struct{}{}
			o.elapsed = max(o.elapsed, event.Elapsed)
			if event.Action == "fail" {
				if _, ok := o.packagesWithFailedTests[event.Package]; !ok {
					o.failures++
				}
			}
		}
		return
	}

	test := goTestID{packageName: event.Package, testName: event.Test}
	show := event.Action == "fail"
	switch event.Action {
	case "fail":
		o.terminalResults[test] = gtr.Fail
	case "skip":
		o.terminalResults[test] = gtr.Skip
	default:
		o.terminalResults[test] = gtr.Pass
	}
	if show {
		if testOutput := o.testOutputs[test]; testOutput != nil {
			if details := parseFailureDetails(testOutput.String()); details != noFailureDetails {
				o.failedTestDetails[test] = details
			}
		}
	}
	if show && o.hasFailedDescendant(test) && !hasGoTestDiagnosticOutput(o.testOutputs[test]) {
		show = false
	}
	// Only failing tests are shown; passing and skipped test output is hidden
	// from the live console, since their framing and body add no signal there.
	o.flushTestOutput(test, show)
	o.tests++
	switch event.Action {
	case "fail":
		o.failedTests[test] = struct{}{}
		o.failures++
		o.packagesWithFailedTests[event.Package] = struct{}{}
	case "skip":
		o.skipped++
	default:
	}
}

func (o *goTestJSONOutput) hasFailedDescendant(test goTestID) bool {
	prefix := test.testName + "/"
	for failedTest := range o.failedTests {
		if failedTest.packageName == test.packageName && strings.HasPrefix(failedTest.testName, prefix) {
			return true
		}
	}
	return false
}

func hasGoTestDiagnosticOutput(output *strings.Builder) bool {
	if output == nil {
		return false
	}
	for line := range strings.SplitSeq(output.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" ||
			strings.HasPrefix(line, "=== RUN   ") ||
			strings.HasPrefix(line, "=== PAUSE ") ||
			strings.HasPrefix(line, "=== CONT  ") ||
			strings.HasPrefix(line, "=== NAME  ") ||
			strings.HasPrefix(line, "--- FAIL: ") {
			continue
		}
		return true
	}
	return false
}

func (o *goTestJSONOutput) writeSummary() {
	if o.summaryWritten || len(o.packages) == 0 {
		return
	}

	var summary strings.Builder
	fmt.Fprintf(&summary, "\nDONE %d tests", o.tests)
	if o.skipped > 0 {
		fmt.Fprintf(&summary, ", %d skipped", o.skipped)
	}
	if o.failures > 0 {
		failure := "failure"
		if o.failures > 1 {
			failure += "s"
		}
		fmt.Fprintf(&summary, ", %d %s", o.failures, failure)
	}
	if o.errors > 0 {
		buildError := "error"
		if o.errors > 1 {
			buildError += "s"
		}
		fmt.Fprintf(&summary, ", %d %s", o.errors, buildError)
	}
	elapsed := o.elapsed
	if !o.startTime.IsZero() && !o.endTime.IsZero() {
		elapsed = o.endTime.Sub(o.startTime).Seconds()
	}
	fmt.Fprintf(&summary, " in %.3fs\n", elapsed)
	o.writeOutput(summary.String())
	o.summaryWritten = true
}

func (o *goTestJSONOutput) flushTestOutput(test goTestID, show bool) {
	testOutput, ok := o.testOutputs[test]
	if !ok {
		return
	}
	output := testOutput.String()
	if show {
		_, _ = fmt.Fprint(o.stdout, goTestLiveOutput(output))
	}
	o.output.WriteString(output)
	delete(o.testOutputs, test)
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

func (o *goTestJSONOutput) writeOutput(output string) {
	_, _ = fmt.Fprint(o.stdout, output)
	o.output.WriteString(output)
}

func (o *goTestJSONOutput) writeIncompleteTestAlerts(test goTestID, output *strings.Builder) {
	if output == nil {
		return
	}
	alerts := parseAlerts(output.String())
	if len(alerts) == 0 {
		return
	}
	_, _ = fmt.Fprintf(o.stdout, "=== RUN   %s\n", test.testName)
	for _, alert := range alerts {
		_, _ = fmt.Fprintln(o.stdout, strings.TrimRight(alert.Details, "\n"))
	}
}

func (o *goTestJSONOutput) decodeLine(line string, event *goTestEvent) error {
	return json.Unmarshal([]byte(line), event)
}

func (o *goTestJSONOutput) junitReport() (*junitReport, error) {
	report, err := gotest.NewJSONParser().Parse(strings.NewReader(o.input.String()))
	if err != nil {
		return &junitReport{}, err
	}
	var abortedPackages []packageAbort
	for i := range report.Packages {
		pkg := &report.Packages[i]
		if pkg.Name == "" {
			pkg.Name = o.packageNameForTests(pkg.Tests)
		}
		var incompleteTests []gtr.Test
		tests := pkg.Tests
		packageHasAlerts := len(parseAlerts(strings.Join(pkg.Output, "\n"))) > 0
		pkg.Tests = tests[:0]
		for _, test := range tests {
			testID := goTestID{
				packageName: pkg.Name,
				testName:    test.Name,
			}
			if result, ok := o.terminalResults[testID]; ok {
				test.Result = result
			}
			// Incomplete tests from a runner abort have run/pause output but no terminal result.
			if test.Result == gtr.Unknown {
				if unknownTestIsIncomplete(test, tests, packageHasAlerts) {
					incompleteTests = append(incompleteTests, test)
				}
				continue
			}
			if details, ok := o.failedTestDetails[testID]; ok {
				test.Output = strings.Split(details, "\n")
			}
			pkg.Tests = append(pkg.Tests, test)
		}
		if len(incompleteTests) > 0 {
			abortedPackages = append(abortedPackages, packageAbort{
				packageName: pkg.Name,
				details:     packageAbortDetails(pkg.Name, incompleteTests),
			})
		}
	}
	junitReport := &junitReport{Testsuites: junit.CreateFromReport(report, "")}
	for _, abortedPackage := range abortedPackages {
		junitReport.appendSyntheticFailure(
			fmt.Sprintf("testrunner.PackageAborted: %s", abortedPackage.packageName),
			failureTypeAborted,
			abortedPackage.details,
		)
	}
	return junitReport, nil
}

func unknownTestIsIncomplete(test gtr.Test, tests []gtr.Test, packageHasAlerts bool) bool {
	hasDescendant := slices.ContainsFunc(tests, func(other gtr.Test) bool {
		return strings.HasPrefix(other.Name, test.Name+"/")
	})
	// Alerts distinguish an interrupted parent from parser-only parent nodes.
	return !hasDescendant ||
		packageHasAlerts ||
		len(parseAlerts(strings.Join(test.Output, "\n"))) > 0
}

func (o *goTestJSONOutput) packageNameForTests(tests []gtr.Test) string {
	var candidates map[string]struct{}
	for _, test := range tests {
		testCandidates := make(map[string]struct{})
		for _, id := range o.testOutputOrder {
			if id.testName == test.Name {
				testCandidates[id.packageName] = struct{}{}
			}
		}
		if candidates == nil {
			candidates = testCandidates
			continue
		}
		for packageName := range candidates {
			if _, ok := testCandidates[packageName]; !ok {
				delete(candidates, packageName)
			}
		}
	}
	if len(candidates) == 1 {
		for packageName := range candidates {
			return packageName
		}
	}
	return ""
}

// packageAbortDetails summarizes a package that exited before its incomplete
// tests produced terminal results.
func packageAbortDetails(packageName string, incompleteTests []gtr.Test) string {
	testNodes := "test nodes"
	if len(incompleteTests) == 1 {
		testNodes = "test node"
	}
	var details strings.Builder
	fmt.Fprintf(
		&details,
		"package %s aborted; %d %s had no final result, and others may not have started",
		packageName,
		len(incompleteTests),
		testNodes,
	)
	details.WriteString("\n\nTests without final results:")
	for _, test := range incompleteTests {
		fmt.Fprintf(&details, "\n- %s", test.Name)
		if failureDetails := incompleteTestFailureDetails(test.Output); failureDetails != "" {
			details.WriteString("\n  Details:\n    ")
			details.WriteString(strings.ReplaceAll(failureDetails, "\n", "\n    "))
		}
	}
	return sanitizeXML(details.String())
}

func incompleteTestFailureDetails(output []string) string {
	data := strings.Join(output, "\n")
	if alerts := parseAlerts(data); len(alerts) > 0 {
		details := make([]string, 0, len(alerts))
		for _, alert := range alerts {
			details = append(details, strings.TrimSpace(alert.Details))
		}
		return strings.Join(details, "\n\n")
	}
	if details := parseFailureDetails(data); details != noFailureDetails {
		return details
	}
	return ""
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
