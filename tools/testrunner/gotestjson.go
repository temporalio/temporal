package testrunner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	line            strings.Builder
	input           strings.Builder
	output          strings.Builder
	testOutputs     map[goTestID]*strings.Builder
	testOutputOrder []goTestID
	packages        map[string]struct{}
	failedPackages  map[string]bool
	packageFailures map[string]bool
	tests           int
	skipped         int
	failures        int
	errors          int
	elapsed         float64
	startTime       time.Time
	endTime         time.Time
	summaryWritten  bool
	stdout          io.Writer
}

func newGoTestJSONOutput() *goTestJSONOutput {
	return &goTestJSONOutput{
		testOutputs:     make(map[goTestID]*strings.Builder),
		packages:        make(map[string]struct{}),
		failedPackages:  make(map[string]bool),
		packageFailures: make(map[string]bool),
		stdout:          os.Stdout,
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

func (o *goTestJSONOutput) String() string {
	if o.line.Len() > 0 {
		o.writeLine(o.line.String())
		o.line.Reset()
	}
	for _, test := range o.testOutputOrder {
		o.flushTestOutput(test, true)
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
				o.packageFailures[event.Package] = true
				if !o.failedPackages[event.Package] {
					o.failures++
				}
			}
		}
		return
	}

	// Only failing tests stream to the live console; passing and skipped test
	// output stays in the buffered report but is hidden from the CI logs.
	o.flushTestOutput(
		goTestID{packageName: event.Package, testName: event.Test},
		event.Action == "fail",
	)
	o.tests++
	switch event.Action {
	case "fail":
		o.failures++
		o.failedPackages[event.Package] = true
	case "skip":
		o.skipped++
	default:
	}
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

func (o *goTestJSONOutput) flushTestOutput(test goTestID, live bool) {
	testOutput, ok := o.testOutputs[test]
	if !ok {
		return
	}
	output := testOutput.String()
	if live {
		_, _ = fmt.Fprint(o.stdout, output)
	}
	o.output.WriteString(output)
	delete(o.testOutputs, test)
}

func (o *goTestJSONOutput) writeOutput(output string) {
	_, _ = fmt.Fprint(o.stdout, output)
	o.output.WriteString(output)
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
		var incompleteTests []gtr.Test
		tests := pkg.Tests
		pkg.Tests = tests[:0]
		for _, test := range tests {
			// Incomplete tests from a runner abort have run/pause output but no terminal result.
			if test.Result != gtr.Unknown {
				pkg.Tests = append(pkg.Tests, test)
			} else if o.packageFailures[pkg.Name] {
				incompleteTests = append(incompleteTests, test)
			}
		}
		if len(incompleteTests) > 0 {
			abortedPackages = append(abortedPackages, packageAbort{
				packageName: pkg.Name,
				details:     packageAbortDetails(pkg.Name, pkg.Output, incompleteTests),
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

// packageAbortDetails summarizes a package that exited before its incomplete
// tests produced terminal results. The parser has already stripped framing and
// passed-test chatter, so the output here is limited to the incomplete tests'
// own lines and the package-level output (e.g. a fatal infra error).
func packageAbortDetails(packageName string, packageOutput []string, incompleteTests []gtr.Test) string {
	details := fmt.Sprintf(
		"package %s exited before %d tests produced terminal results",
		packageName,
		len(incompleteTests),
	)
	var recent strings.Builder
	for _, test := range incompleteTests {
		for _, line := range test.Output {
			recent.WriteString(line)
			recent.WriteByte('\n')
		}
	}
	for _, line := range packageOutput {
		recent.WriteString(line)
		recent.WriteByte('\n')
	}
	if recent.Len() > 0 {
		details += "\n\nRecent package output:\n" + recent.String()
	}
	return truncateAlertDetails(sanitizeXML(details))
}
