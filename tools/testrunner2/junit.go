package testrunner2

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/gtr"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/jstemmer/go-junit-report/v2/parser/gotest"
)

const (
	alertsSuiteName  = "ALERTS"
	noFailureDetails = "(error details not found)"
)

type junitReport struct {
	junit.Testsuites
	path          string
	attempt       int // 1-based attempt number (1 = first run, 2+ = retry)
	reportingErrs []error
}

type testFailureRef struct {
	pkg  string
	name string
}

func (j *junitReport) read() error {
	f, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	decoder := xml.NewDecoder(f)
	if err = decoder.Decode(&j.Testsuites); err != nil {
		return fmt.Errorf("failed to read junit report file: %w", err)
	}
	return nil
}

func (j *junitReport) write() error {
	f, err := os.Create(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err = encoder.Encode(j.Testsuites); err != nil {
		return fmt.Errorf("failed to write junit report file: %w", err)
	}
	return nil
}

// appendAlerts adds a synthetic JUnit suite for all alerts.
func (j *junitReport) appendAlerts(alerts alerts) {
	if len(alerts) == 0 {
		return
	}

	var cases []junit.Testcase
	for _, alert := range alerts.dedupe() {
		// Use the test name as the case name so it's properly tracked for retries.
		// splitTestName extracts a clean test name and package classname from
		// fully-qualified goroutine trace names.
		fullName := primaryTestName(alert.Tests)
		var classname, name string
		if fullName == "" {
			name = fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		} else {
			classname, name = splitTestName(fullName)
		}
		failureType := alertFailureType(alert.Kind)
		message := fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		var details string
		if len(alert.Tests) > 0 {
			details = fmt.Sprintf("Detected in tests:\n\t%s", strings.Join(alert.Tests, "\n\t"))
		}
		r := &junit.Result{Message: message, Type: string(failureType), Data: details}
		cases = append(cases, junit.Testcase{
			Classname: classname,
			Name:      name,
			Failure:   r,
		})
	}

	suite := junit.Testsuite{
		Name:      alertsSuiteName,
		Failures:  len(cases),
		Tests:     len(cases),
		Testcases: cases,
	}
	j.Suites = append(j.Suites, suite)
	j.Failures += suite.Failures
	j.Tests += suite.Tests
}

// testcases iterates over all test cases across all suites.
func (j *junitReport) testcases() iter.Seq[junit.Testcase] {
	return func(yield func(junit.Testcase) bool) {
		for _, suite := range j.Suites {
			for _, tc := range suite.Testcases {
				if !yield(tc) {
					return
				}
			}
		}
	}
}

func (j *junitReport) collectTestCases() map[string]struct{} {
	cases := make(map[string]struct{})
	for tc := range j.testcases() {
		cases[tc.Name] = struct{}{}
	}
	return cases
}

func (j *junitReport) collectTestCaseFailures() []string {
	var failures []string
	for tc := range j.testcases() {
		if tc.Failure != nil {
			failures = append(failures, tc.Name)
		}
	}
	slices.Sort(failures)
	return filterParentNames(failures)
}

func (j *junitReport) collectTestCaseFailureRefs() []testFailureRef {
	byPackage := make(map[string][]string)
	for _, suite := range j.Suites {
		for _, tc := range suite.Testcases {
			if tc.Failure == nil {
				continue
			}
			pkg := tc.Classname
			if pkg == "" {
				pkg = suite.Name
			}
			byPackage[pkg] = append(byPackage[pkg], tc.Name)
		}
	}

	var refs []testFailureRef
	for pkg, names := range byPackage {
		slices.Sort(names)
		for _, name := range filterParentNames(names) {
			refs = append(refs, testFailureRef{pkg: pkg, name: name})
		}
	}
	slices.SortFunc(refs, func(a, b testFailureRef) int {
		if c := strings.Compare(a.pkg, b.pkg); c != 0 {
			return c
		}
		return strings.Compare(a.name, b.name)
	})
	return refs
}

func mergeReports(reports []*junitReport) (*junitReport, error) {
	if len(reports) == 0 {
		return nil, errors.New("no reports to merge")
	}

	var combined junit.Testsuites
	combined.XMLName = reports[0].XMLName
	combined.Name = reports[0].Name

	for _, report := range reports {
		addReportCounts(&combined, report)
		suffix := retrySuffix(report.attempt)
		for _, suite := range report.Suites {
			if mergedSuite, ok := mergeSuite(suite, suffix); ok {
				combined.Suites = append(combined.Suites, mergedSuite)
			}
		}
	}

	return &junitReport{
		Testsuites:    combined,
		reportingErrs: validateRetries(reports),
	}, nil
}

func addReportCounts(combined *junit.Testsuites, report *junitReport) {
	combined.Tests += report.Tests
	combined.Errors += report.Errors
	combined.Failures += report.Failures
	combined.Skipped += report.Skipped
	combined.Disabled += report.Disabled
	combined.Time += report.Time
}

func retrySuffix(attempt int) string {
	if attempt <= 1 {
		return ""
	}
	return fmt.Sprintf(" (retry %d)", attempt-1)
}

func mergeSuite(suite junit.Testsuite, suffix string) (junit.Testsuite, bool) {
	if len(suite.Testcases) == 0 {
		return junit.Testsuite{}, false
	}

	testcases := slices.Clone(suite.Testcases)
	slices.SortFunc(testcases, func(a, b junit.Testcase) int {
		return strings.Compare(a.Name, b.Name)
	})

	merged := suite
	merged.SystemOut = nil
	merged.SystemErr = nil
	merged.Name += suffix
	merged.Testcases = make([]junit.Testcase, 0, len(testcases))

	leafSet := leafTestCaseSet(testcases)
	for _, testCase := range testcases {
		if _, isLeaf := leafSet[testCase.Name]; !isLeaf {
			continue
		}
		merged.Testcases = append(merged.Testcases, mergeTestCase(testCase, suite.Name, suffix))
	}
	return merged, true
}

func leafTestCaseSet(testcases []junit.Testcase) map[string]struct{} {
	names := make([]string, len(testcases))
	for i, tc := range testcases {
		names[i] = tc.Name
	}
	leafSet := make(map[string]struct{}, len(names))
	for _, name := range filterParentNames(names) {
		leafSet[name] = struct{}{}
	}
	return leafSet
}

func mergeTestCase(testCase junit.Testcase, suiteName, suffix string) junit.Testcase {
	testCase.SystemOut = nil
	testCase.SystemErr = nil
	if testCase.Failure != nil {
		normalizeFailure(testCase.Failure, suiteName)
	}
	testCase.Name += suffix
	return testCase
}

func normalizeFailure(failure *junit.Result, suiteName string) {
	if failure.Data != "" {
		if details := parseFailureDetails(failure.Data); details != noFailureDetails {
			failure.Data = details
		}
	}
	if suiteName == alertsSuiteName {
		failure.Type = canonicalAlertFailureType(failure)
	} else {
		failure.Type = string(failureTypeFailed)
	}
}

// validateRetries checks that all failures from attempt N were retried in attempt N+1.
func validateRetries(reports []*junitReport) []error {
	byAttempt := make(map[int][]*junitReport)
	for _, r := range reports {
		byAttempt[r.attempt] = append(byAttempt[r.attempt], r)
	}
	var attempts []int
	for a := range byAttempt {
		if a > 0 {
			attempts = append(attempts, a)
		}
	}
	slices.Sort(attempts)

	var errs []error
	for i := 0; i < len(attempts)-1; i++ {
		currAttempt, nextAttempt := attempts[i], attempts[i+1]
		prevFailures := make(map[string]bool)
		for _, r := range byAttempt[currAttempt] {
			for _, f := range r.collectTestCaseFailures() {
				prevFailures[f] = true
			}
		}
		nextCases := make(map[string]bool)
		for _, r := range byAttempt[nextAttempt] {
			for k := range r.collectTestCases() {
				nextCases[k] = true
			}
		}
		var missing []string
		for f := range prevFailures {
			if !nextCases[f] {
				missing = append(missing, f)
			}
		}
		if len(missing) > 0 {
			errs = append(errs, fmt.Errorf(
				"expected rerun of all failures from attempt %d, missing in attempt %d: %v",
				currAttempt, nextAttempt, missing))
		}
	}
	return errs
}

func alertFailureType(kind failureKind) failureType {
	switch kind {
	case failureKindDataRace:
		return failureTypeDataRace
	case failureKindPanic:
		return failureTypePanic
	case failureKindFatal:
		return failureTypeFatal
	case failureKindTimeout:
		return failureTypeTimeout
	case failureKindCrash:
		return failureTypeCrash
	default:
		return failureTypeFailed
	}
}

func canonicalAlertFailureType(result *junit.Result) string {
	if result.Type != "" {
		return result.Type
	}
	message := strings.ToLower(result.Message)
	switch {
	case strings.Contains(message, "data race"), strings.Contains(message, "race"):
		return string(failureTypeDataRace)
	case strings.Contains(message, "panic"):
		return string(failureTypePanic)
	case strings.Contains(message, "fatal"):
		return string(failureTypeFatal)
	case strings.Contains(message, "timeout"), strings.Contains(message, "stuck"):
		return string(failureTypeTimeout)
	case strings.Contains(message, "crash"):
		return string(failureTypeCrash)
	default:
		return string(failureTypeFailed)
	}
}

// testFailure holds information about a failed test.
type testFailure struct {
	Name       string
	ErrorTrace string // extracted error trace/details
}

// testResults holds failures and passes extracted from test output.
type testResults struct {
	failures []testFailure
	passes   []string
}

// newJUnitReport parses go test -v output from a log file and writes a JUnit XML report.
// Returns the test results including failures and passes.
func newJUnitReport(logPath string, junitPath string) testResults {
	data, err := os.ReadFile(logPath)
	if err != nil {
		log.Printf("[runner] warning: failed to open log file for JUnit report: %v", err)
		return testResults{}
	}
	output := stripLogHeader(string(data))

	report, isJSON, err := parseGoTestOutput(output)
	if err != nil {
		log.Printf("[runner] warning: failed to parse test output for JUnit report: %v", err)
		return testResults{}
	}

	// Extract failed and passed tests from a clean parse (without test2json markers).
	results := extractResults(report)
	if !isJSON {
		results = extractResultsClean(output)
	}

	// Convert to JUnit and write
	hostname, _ := os.Hostname()
	testsuites := junit.CreateFromReport(report, hostname)

	// Filter failure/error data to just error traces
	for i := range testsuites.Suites {
		for j := range testsuites.Suites[i].Testcases {
			tc := &testsuites.Suites[i].Testcases[j]
			if tc.Failure != nil && tc.Failure.Data != "" {
				if details := extractErrorTrace(strings.Split(tc.Failure.Data, "\n")); details != "" {
					tc.Failure.Data = cleanFailureDetails(details)
				}
				tc.Failure.Type = string(failureTypeFailed)
			}
			if tc.Error != nil && tc.Error.Data != "" {
				if details := extractErrorTrace(strings.Split(tc.Error.Data, "\n")); details != "" {
					tc.Error.Data = cleanFailureDetails(details)
				}
			}
		}
	}

	jf, err := os.Create(junitPath)
	if err != nil {
		return results
	}
	defer func() { _ = jf.Close() }()

	if _, err := jf.WriteString(xml.Header); err != nil {
		return results
	}
	encoder := xml.NewEncoder(jf)
	encoder.Indent("", "    ")
	if err := encoder.Encode(testsuites); err != nil {
		return results
	}

	return results
}

func parseGoTestOutput(output string) (gtr.Report, bool, error) {
	// Use ExcludeParents to filter out parent test entries (e.g., TestSuite when
	// we only care about TestSuite/TestMethod). This works correctly with
	// -test.v=test2json which emits --- PASS/FAIL lines as subtests complete.
	if strings.HasPrefix(strings.TrimSpace(output), "{") {
		report, err := parseGoTestJSONOutput(output)
		return report, true, err
	}
	parser := gotest.NewParser(gotest.SetSubtestMode(gotest.ExcludeParents))
	report, err := parser.Parse(strings.NewReader(output))
	return report, false, err
}

type goTestJSONEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type goTestJSONPackage struct {
	pkg           gtr.Package
	testOrder     []string
	tests         map[string]*gtr.Test
	packageFailed bool
}

func parseGoTestJSONOutput(output string) (gtr.Report, error) {
	packages := make(map[string]*goTestJSONPackage)
	var packageOrder []string
	reader := bufio.NewReader(strings.NewReader(output))
	for {
		line, err := reader.ReadString('\n')
		if err != nil && line == "" {
			break
		}
		line = strings.TrimSpace(line)
		if line != "" {
			var ev goTestJSONEvent
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				return gtr.Report{}, err
			}
			if ev.Package != "" {
				state := packages[ev.Package]
				if state == nil {
					state = &goTestJSONPackage{
						pkg:   gtr.Package{Name: ev.Package},
						tests: make(map[string]*gtr.Test),
					}
					packages[ev.Package] = state
					packageOrder = append(packageOrder, ev.Package)
				}
				state.apply(ev)
			}
		}
		if err != nil {
			break
		}
	}

	report := gtr.Report{Packages: make([]gtr.Package, 0, len(packageOrder))}
	for _, name := range packageOrder {
		report.Packages = append(report.Packages, packages[name].reportPackage())
	}
	return report, nil
}

func (p *goTestJSONPackage) apply(ev goTestJSONEvent) {
	if p.pkg.Timestamp.IsZero() && !ev.Time.IsZero() {
		p.pkg.Timestamp = ev.Time
	}
	if ev.Test == "" {
		if ev.Output != "" {
			p.pkg.Output = appendOutputLines(p.pkg.Output, ev.Output)
		}
		switch ev.Action {
		case "pass", "fail", "skip":
			p.pkg.Duration = secondsDuration(ev.Elapsed)
			if ev.Action == "fail" {
				p.packageFailed = true
			}
		default:
		}
		return
	}

	test := p.test(ev.Test)
	if ev.Output != "" {
		test.Output = appendOutputLines(test.Output, ev.Output)
	}
	switch ev.Action {
	case "pass":
		test.Result = gtr.Pass
		test.Duration = secondsDuration(ev.Elapsed)
	case "fail":
		test.Result = gtr.Fail
		test.Duration = secondsDuration(ev.Elapsed)
	case "skip":
		test.Result = gtr.Skip
		test.Duration = secondsDuration(ev.Elapsed)
	default:
	}
}

func (p *goTestJSONPackage) test(name string) *gtr.Test {
	if test := p.tests[name]; test != nil {
		return test
	}
	test := gtr.NewTest(len(p.testOrder), name)
	p.tests[name] = &test
	p.testOrder = append(p.testOrder, name)
	return &test
}

func (p *goTestJSONPackage) reportPackage() gtr.Package {
	pkg := p.pkg
	leafNames := filterParentNames(p.testOrder)
	leafSet := make(map[string]struct{}, len(leafNames))
	for _, name := range leafNames {
		leafSet[name] = struct{}{}
	}
	hasFailure := false
	for _, name := range p.testOrder {
		if _, ok := leafSet[name]; !ok {
			continue
		}
		test := *p.tests[name]
		if test.Result == gtr.Fail && len(test.Output) == 0 {
			test.Output = p.ancestorOutput(name)
		}
		if test.Result == gtr.Fail {
			hasFailure = true
		}
		pkg.Tests = append(pkg.Tests, test)
	}
	if p.packageFailed && !hasFailure {
		pkg.RunError = gtr.Error{
			Name:   pkg.Name,
			Cause:  "go test failed",
			Output: pkg.Output,
		}
	}
	return pkg
}

func (p *goTestJSONPackage) ancestorOutput(name string) []string {
	var output []string
	for _, ancestor := range ancestorNames(name) {
		test := p.tests[ancestor]
		if test == nil {
			continue
		}
		output = append(output, test.Output...)
	}
	return output
}

func ancestorNames(name string) []string {
	var names []string
	for {
		idx := strings.LastIndexByte(name, '/')
		if idx < 0 {
			break
		}
		name = name[:idx]
		names = append(names, name)
	}
	slices.Reverse(names)
	return names
}

func appendOutputLines(lines []string, output string) []string {
	output = strings.TrimSuffix(output, "\n")
	if output == "" {
		return lines
	}
	return append(lines, strings.Split(output, "\n")...)
}

func secondsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// extractResults returns failed and passed tests from a gtr.Report.
// This extracts directly from the parsed report, which is more reliable than
// reading back the JUnit XML (which may mark tests as errors if they didn't complete).
func extractResults(report gtr.Report) testResults {
	var results testResults
	for _, pkg := range report.Packages {
		for _, test := range pkg.Tests {
			switch test.Result {
			case gtr.Fail:
				trace := extractErrorTrace(test.Output)
				if trace == "" {
					trace = lastLines(test.Output, 20)
				}
				results.failures = append(results.failures, testFailure{
					Name:       test.Name,
					ErrorTrace: trace,
				})
			case gtr.Pass:
				results.passes = append(results.passes, test.Name)
			default:
				// skip other results (e.g., skip, unknown)
			}
		}
	}
	return results
}

// extractResultsClean re-parses the output with test2json markers stripped so
// that test output is correctly attributed to individual tests (needed for
// extracting error traces for console display).
func extractResultsClean(output string) testResults {
	clean := strings.ReplaceAll(output, "\x16", "")
	parser := gotest.NewParser(gotest.SetSubtestMode(gotest.ExcludeParents))
	report, err := parser.Parse(strings.NewReader(clean))
	if err != nil {
		return testResults{}
	}
	return extractResults(report)
}

// stripLogHeader removes the TESTRUNNER LOG header from log file content.
func stripLogHeader(content string) string {
	headerEnd := logHeaderSeparator + "\n\n"
	if idx := strings.Index(content, headerEnd); idx >= 0 {
		// Find the last occurrence of the separator (the header has 3 of them)
		rest := content[idx+len(headerEnd):]
		return rest
	}
	return content
}

// parseAlertsFromFile reads a log file from disk and parses alerts from it.
func parseAlertsFromFile(logPath string) []alert {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil
	}
	output := stripLogHeader(string(data))
	return parseAlerts(output)
}

// lastLines returns up to n non-empty lines from the end of output.
func lastLines(output []string, n int) string {
	var lines []string
	for i := len(output) - 1; i >= 0 && len(lines) < n; i-- {
		if s := strings.TrimSpace(output[i]); s != "" {
			lines = append(lines, output[i])
		}
	}
	slices.Reverse(lines)
	return strings.Join(lines, "\n")
}

func parseFailureDetails(data string) string {
	lines := normalizedFailureLines(data)

	if start, end, ok := findTestifyFailureBlock(lines); ok {
		return cleanFailureDetails(strings.Join(lines[start:end], "\n"))
	}
	if start, end, ok := findLastFailureBlock(lines); ok {
		return cleanFailureDetails(strings.Join(lines[start:end], "\n"))
	}
	return noFailureDetails
}

func cleanFailureDetails(details string) string {
	return strings.ReplaceAll(details, "\t", "    ")
}

func normalizedFailureLines(data string) []string {
	lines := strings.Split(strings.ReplaceAll(data, "\r\n", "\n"), "\n")
	for len(lines) > 0 {
		t := strings.TrimSpace(lines[len(lines)-1])
		if t != "" && t != "FAIL" {
			break
		}
		lines = lines[:len(lines)-1]
	}
	return lines
}

func findTestifyFailureBlock(lines []string) (start, end int, ok bool) {
	for i, line := range lines {
		if !strings.Contains(line, "Error Trace:") {
			continue
		}
		start = i
		if i > 0 {
			start = i - 1
		}
		return start, endOfFailureBlock(lines, start+1), true
	}
	return 0, 0, false
}

func findLastFailureBlock(lines []string) (start, end int, ok bool) {
	lastStart := -1
	for i, line := range lines {
		if isTestOutputLine(line) {
			lastStart = i
		}
	}
	if lastStart < 0 {
		return 0, 0, false
	}
	return lastStart, endOfFailureBlock(lines, lastStart+1), true
}

func endOfFailureBlock(lines []string, start int) int {
	end := start
	for end < len(lines) && !isTestOutputLine(lines[end]) && lines[end] != "" {
		end++
	}
	return end
}

func isTestOutputLine(line string) bool {
	return len(line) > 4 && line[:4] == "    " && line[4] != ' ' && line[4] != '\t'
}
