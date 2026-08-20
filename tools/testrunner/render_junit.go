package testrunner

import (
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/junit"
)

type junitRenderer struct {
	history []attemptResult
	report  junit.Testsuites
}

func renderJUnit(results []attemptResult) junit.Testsuites {
	renderer := junitRenderer{
		history: results,
		report:  junit.Testsuites{XMLName: xml.Name{Local: "testsuites"}},
	}
	for attemptIndex := range results {
		renderer.renderAttempt(attemptIndex)
	}
	var duration time.Duration
	for _, result := range results {
		duration += result.process.duration
	}
	renderer.report.Time = formatDuration(duration)
	return renderer.report
}

func (r *junitRenderer) renderAttempt(attemptIndex int) {
	result := r.history[attemptIndex]
	suffix := attemptSuffix(attemptIndex, len(r.history))
	runtimeFailures := make(map[string]struct{})
	for _, pkg := range result.runtimeFailures() {
		runtimeFailures[pkg.name] = struct{}{}
	}
	packages := slices.Clone(result.packages)
	slices.SortFunc(packages, func(a, b packageResult) int { return strings.Compare(a.name, b.name) })
	for _, pkg := range packages {
		if suite, ok := r.renderPackage(attemptIndex, pkg, suffix, runtimeFailures); ok {
			r.report.AddSuite(suite)
		}
	}
	builds := slices.Clone(result.builds)
	slices.SortFunc(builds, func(a, b buildResult) int { return strings.Compare(a.importPath, b.importPath) })
	for _, build := range builds {
		if build.failed || strings.TrimSpace(build.output) != "" {
			r.report.AddSuite(renderBuildSuite(build, suffix, len(r.report.Suites)))
		}
	}
	r.renderSyntheticFailures(result, suffix)
}

func (r *junitRenderer) renderPackage(
	attemptIndex int,
	pkg packageResult,
	suffix string,
	runtimeFailures map[string]struct{},
) (junit.Testsuite, bool) {
	suite := junit.Testsuite{
		Name: pkg.name + suffix,
		ID:   len(r.report.Suites),
		Time: formatDuration(pkg.duration),
	}
	if !pkg.startedAt.IsZero() {
		suite.Timestamp = pkg.startedAt.Format(time.RFC3339Nano)
	}
	if pkg.coverage != nil {
		suite.AddProperty("coverage.statements.pct", fmt.Sprintf("%.2f", *pkg.coverage))
	}

	executions := slices.Clone(pkg.executions)
	slices.SortFunc(executions, func(a, b testExecution) int {
		if byName := strings.Compare(a.id.testName, b.id.testName); byName != 0 {
			return byName
		}
		return a.occurrence - b.occurrence
	})
	var retainedParentOutput strings.Builder
	for _, execution := range executions {
		if testcase, parentOutput, ok := r.renderExecution(attemptIndex, pkg, execution, suffix); ok {
			suite.AddTestcase(testcase)
		} else if parentOutput != "" {
			fmt.Fprintf(&retainedParentOutput, "[%s]\n%s\n", execution.id.testName, parentOutput)
		}
	}
	if _, ok := runtimeFailures[pkg.name]; ok {
		runtimeDetails := strings.TrimSpace(strings.Join([]string{
			cleanPackageOutput(pkg.output),
			r.history[attemptIndex].process.stderr,
		}, "\n"))
		suite.AddTestcase(junit.Testcase{
			Name:      pkg.name + " [runtime failure]" + suffix,
			Classname: pkg.name,
			Time:      formatDuration(0),
			Error: &junit.Result{
				Message: "Package runtime error",
				Type:    "ERROR",
				Data:    sanitizeXML(truncateDetails(runtimeDetails)),
			},
		})
	}
	packageOutput := strings.TrimSpace(cleanPackageOutput(pkg.output))
	parentOutput := strings.TrimSpace(retainedParentOutput.String())
	if packageOutput != "" || parentOutput != "" {
		data := strings.TrimSpace(strings.Join([]string{packageOutput, parentOutput}, "\n"))
		suite.SystemOut = &junit.Output{Data: sanitizeXML(data)}
	}
	return suite, len(suite.Testcases) > 0 || suite.SystemOut != nil || suite.Properties != nil
}

func (r *junitRenderer) renderExecution(
	attemptIndex int,
	pkg packageResult,
	execution testExecution,
	suffix string,
) (junit.Testcase, string, bool) {
	if execution.outcome == testIncomplete && !pkg.isFailedLeaf(execution) {
		return junit.Testcase{}, "", false
	}
	hasDescendant := pkg.hasExecutionDescendant(execution)
	if (execution.outcome == testPassed || execution.outcome == testSkipped) && hasDescendant {
		return junit.Testcase{}, "", false
	}
	cleanedOutput := strings.TrimSpace(cleanTestOutput(execution.output))
	if execution.outcome == testFailed && !pkg.isFailedLeaf(execution) {
		return junit.Testcase{}, cleanedOutput, false
	}
	if execution.outcome == testFailed && r.isStructuralParent(execution.id) && !execution.failure.actionable &&
		r.hasLaterTerminal(attemptIndex, execution.id) {
		return junit.Testcase{}, cleanedOutput, false
	}

	testcase := junit.Testcase{
		Name:      execution.id.testName + suffix,
		Classname: execution.id.packageName,
		Time:      formatDuration(execution.duration),
	}
	switch execution.outcome {
	case testFailed, testIncomplete:
		details := execution.failure.details
		if details == "" {
			details = cleanedOutput
		}
		if details == "" {
			details = "test failed without attributable diagnostic output; no failed descendant supplied the cause"
		}
		testcase.Failure = generateFailure(failureTypeFailed, sanitizeXML(truncateDetails(details)))
	case testSkipped:
		testcase.Skipped = &junit.Result{Message: "Skipped"}
	default:
	}
	return testcase, "", true
}

func (r *junitRenderer) isStructuralParent(id testID) bool {
	prefix := id.testName + "/"
	for _, result := range r.history {
		for _, pkg := range result.packages {
			if pkg.name != id.packageName {
				continue
			}
			if slices.ContainsFunc(pkg.executions, func(execution testExecution) bool {
				return strings.HasPrefix(execution.id.testName, prefix)
			}) {
				return true
			}
		}
	}
	return false
}

func (r *junitRenderer) hasLaterTerminal(attemptIndex int, id testID) bool {
	for _, result := range r.history[attemptIndex+1:] {
		for _, pkg := range result.packages {
			if slices.ContainsFunc(pkg.executions, func(execution testExecution) bool {
				return execution.id == id && execution.outcome != testIncomplete
			}) {
				return true
			}
		}
	}
	return false
}

func renderBuildSuite(build buildResult, suffix string, id int) junit.Testsuite {
	suite := junit.Testsuite{Name: build.importPath + suffix, ID: id, Time: formatDuration(0)}
	details := sanitizeXML(strings.TrimSpace(cleanPackageOutput(build.output)))
	if build.failed {
		suite.AddTestcase(junit.Testcase{
			Name:      "[build failed]" + suffix,
			Classname: build.importPath,
			Time:      formatDuration(0),
			Error: &junit.Result{
				Message: "Build error",
				Type:    "ERROR",
				Data:    details,
			},
		})
	} else {
		suite.SystemOut = &junit.Output{Data: details}
	}
	return suite
}

func (r *junitRenderer) renderSyntheticFailures(result attemptResult, suffix string) {
	var cases []junit.Testcase
	if result.process.state != processDeadlineExceeded {
		for _, pkg := range result.abortedPackages() {
			cases = append(cases, junit.Testcase{
				Name:    "testrunner.PackageAborted: " + pkg.name + suffix,
				Time:    formatDuration(0),
				Failure: generateFailure(failureTypeAborted, packageAbortDetails(result, pkg)),
			})
		}
	}
	for _, diagnostic := range result.diagnostics {
		diagnostic.tests = slices.Clone(diagnostic.tests)
		slices.SortFunc(diagnostic.tests, compareTestID)
		kind := failureTypeForDiagnostic(diagnostic.kind)
		details := diagnosticFailureDetails(diagnostic)
		if diagnostic.kind == diagnosticTimeout && len(diagnostic.tests) > 0 {
			for _, id := range diagnostic.tests {
				cases = append(cases, junit.Testcase{
					Name:      id.testName + " (timed out)" + suffix,
					Classname: id.packageName,
					Time:      formatDuration(0),
					Failure:   generateFailure(kind, details),
				})
			}
			continue
		}
		name := fmt.Sprintf("%s: %s", kind, diagnostic.summary)
		if len(diagnostic.tests) > 0 {
			name += " — in " + diagnostic.tests[0].testName
		}
		cases = append(cases, junit.Testcase{
			Name:    name + suffix,
			Time:    formatDuration(0),
			Failure: generateFailure(kind, details),
		})
	}
	if result.process.state == processDeadlineExceeded {
		cases = append(cases, junit.Testcase{
			Name:    "testrunner.TotalTimeout" + suffix,
			Time:    formatDuration(0),
			Failure: generateFailure(failureTypeTimeout, totalTimeoutDetails(result)),
		})
	} else if result.unexplainedProcessFailure() {
		details := strings.TrimSpace(strings.Join([]string{
			result.process.details,
			result.process.stderr,
			result.unstructuredOutput,
		}, "\n"))
		cases = append(cases, junit.Testcase{
			Name:    "testrunner.ExecutionError" + suffix,
			Time:    formatDuration(0),
			Failure: generateFailure(failureTypeCrash, sanitizeXML(truncateDetails(details))),
		})
	}
	if len(cases) == 0 {
		return
	}
	slices.SortFunc(cases, func(a, b junit.Testcase) int { return strings.Compare(a.Name, b.Name) })
	suite := junit.Testsuite{Name: testrunnerSuiteName + suffix, ID: len(r.report.Suites), Time: formatDuration(0)}
	for _, testcase := range cases {
		suite.AddTestcase(testcase)
	}
	r.report.AddSuite(suite)
}

func diagnosticFailureDetails(diagnostic diagnostic) string {
	var payload strings.Builder
	diagnosticDetails := strings.TrimSpace(diagnostic.details)
	if diagnostic.kind == diagnosticTimeout && diagnostic.summary != "" {
		payload.WriteString(diagnostic.summary)
		if diagnosticDetails != "" {
			payload.WriteByte('\n')
		}
	}
	payload.WriteString(diagnosticDetails)

	var details strings.Builder
	details.WriteString(truncateDetails(payload.String()))
	if len(diagnostic.tests) > 0 {
		if details.Len() > 0 {
			details.WriteString("\n\n")
		}
		details.WriteString("Detected in tests:")
		for _, id := range diagnostic.tests {
			fmt.Fprintf(&details, "\n\t%s.%s", id.packageName, id.testName)
		}
	}
	return sanitizeXML(details.String())
}

func failureTypeForDiagnostic(kind diagnosticKind) failureType {
	switch kind {
	case diagnosticDataRace:
		return failureTypeDataRace
	case diagnosticPanic:
		return failureTypePanic
	case diagnosticFatal:
		return failureTypeFatal
	case diagnosticTimeout:
		return failureTypeTimeout
	default:
		return failureTypeFailed
	}
}

// packageAbortDetails summarizes a package that exited before its incomplete
// tests produced terminal results.
func packageAbortDetails(result attemptResult, pkg packageResult) string {
	incomplete := pkg.meaningfulIncompleteExecutions()
	testNodes := "test nodes"
	if len(incomplete) == 1 {
		testNodes = "test node"
	}
	var details strings.Builder
	fmt.Fprintf(
		&details,
		"package %s aborted; %d %s had no final result, and others may not have started",
		pkg.name,
		len(incomplete),
		testNodes,
	)
	details.WriteString("\n\nTests without final results:")
	for _, execution := range incomplete {
		fmt.Fprintf(&details, "\n- %s", execution.id.testName)
		if execution.failure.details != "" {
			details.WriteString("\n  Details:\n    ")
			details.WriteString(strings.ReplaceAll(execution.failure.details, "\n", "\n    "))
		}
	}
	for _, diagnostic := range result.diagnostics {
		if diagnostic.packageName != pkg.name && !slices.ContainsFunc(diagnostic.tests, func(id testID) bool {
			return id.packageName == pkg.name
		}) {
			continue
		}
		diagnosticDetails := diagnosticContextSummary(diagnostic)
		if diagnosticDetails == "" || strings.Contains(details.String(), diagnosticDetails) {
			continue
		}
		details.WriteString("\n\nDiagnostic context:\n")
		details.WriteString(diagnosticDetails)
	}
	if (result.process.state != processExited || result.process.exitCode != 0) && result.process.details != "" {
		details.WriteString("\n\nProcess termination:\n")
		details.WriteString(strings.TrimSpace(result.process.details))
	}
	if processStderr := strings.TrimSpace(result.process.stderr); processStderr != "" {
		details.WriteString("\n\nProcess stderr:\n")
		details.WriteString(processStderr)
	}
	return sanitizeXML(truncateDetails(details.String()))
}

func totalTimeoutDetails(result attemptResult) string {
	details := []string{strings.TrimSpace(result.process.details)}
	for _, pkg := range result.packages {
		if len(pkg.meaningfulIncompleteExecutions()) > 0 {
			details = append(details, packageAbortDetails(result, pkg))
		}
	}
	return sanitizeXML(truncateDetails(strings.TrimSpace(strings.Join(details, "\n\n"))))
}

func diagnosticContextSummary(diagnostic diagnostic) string {
	for line := range strings.Lines(diagnostic.details) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "panic:") ||
			strings.HasPrefix(line, "fatal error:") ||
			strings.HasPrefix(line, "WARNING: DATA RACE") {
			return line
		}
	}
	return strings.TrimSpace(diagnostic.summary)
}

func cleanTestOutput(output string) string {
	var lines []string
	for line := range strings.Lines(output) {
		line = strings.TrimSuffix(line, "\n")
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "=== RUN   ") ||
			strings.HasPrefix(trimmed, "=== PAUSE ") ||
			strings.HasPrefix(trimmed, "=== CONT  ") ||
			strings.HasPrefix(trimmed, "=== NAME  ") ||
			strings.HasPrefix(trimmed, "--- PASS: ") ||
			strings.HasPrefix(trimmed, "--- FAIL: ") ||
			strings.HasPrefix(trimmed, "--- SKIP: ") {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func cleanPackageOutput(output string) string {
	var lines []string
	for line := range strings.Lines(output) {
		line = strings.TrimSuffix(line, "\n")
		if line == "FAIL" || line == "PASS" ||
			strings.HasPrefix(line, "FAIL\t") ||
			strings.HasPrefix(line, "ok  \t") ||
			strings.HasPrefix(line, "?   \t") ||
			coverageLine.MatchString(strings.TrimSpace(line)) {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func attemptSuffix(attemptIndex, attempts int) string {
	if attemptIndex == 0 {
		return ""
	}
	suffix := fmt.Sprintf(" (retry %d)", attemptIndex)
	if attemptIndex == attempts-1 {
		suffix += " (final)"
	}
	return suffix
}

func formatDuration(duration time.Duration) string {
	return fmt.Sprintf("%.6f", duration.Seconds())
}

func renderCrashJUnit(name string) junit.Testsuites {
	suite := junit.Testsuite{Name: "suite", Time: formatDuration(0)}
	suite.AddTestcase(junit.Testcase{
		Name:    name + " (crash)",
		Time:    formatDuration(0),
		Failure: generateFailure(failureTypeCrash, ""),
	})
	report := junit.Testsuites{XMLName: xml.Name{Local: "testsuites"}, Time: formatDuration(0)}
	report.AddSuite(suite)
	return report
}
