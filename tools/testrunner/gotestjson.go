package testrunner

import (
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var coverageLine = regexp.MustCompile(`^coverage: ([0-9]+(?:\.[0-9]+)?)% of statements`)

type goTestJSONEvent struct {
	Time        time.Time `json:"Time"`
	Action      string    `json:"Action"`
	Package     string    `json:"Package"`
	Test        string    `json:"Test"`
	Elapsed     float64   `json:"Elapsed"`
	Output      string    `json:"Output"`
	ImportPath  string    `json:"ImportPath"`
	FailedBuild string    `json:"FailedBuild"`
}

type goTestRecorder struct {
	line         strings.Builder
	console      *goTestConsole
	packages     map[string]*goTestPackageBuilder
	packageOrder []string
	builds       map[string]*goTestBuildBuilder
	buildOrder   []string
	unstructured strings.Builder
}

type goTestPackageBuilder struct {
	name               string
	startedAt          time.Time
	duration           time.Duration
	outcome            packageOutcome
	coverage           *float64
	output             strings.Builder
	excludedTestOutput strings.Builder
	failedBuild        string
	executions         []*goTestExecutionBuilder
	active             map[testID]*goTestExecutionBuilder
	occurrences        map[testID]int
}

type goTestExecutionBuilder struct {
	id         testID
	occurrence int
	outcome    testOutcome
	terminal   bool
	duration   time.Duration
	output     strings.Builder
}

type goTestBuildBuilder struct {
	importPath string
	failed     bool
	output     strings.Builder
}

func newGoTestRecorder(consoleOutput io.Writer) *goTestRecorder {
	return &goTestRecorder{
		console:  newGoTestConsole(consoleOutput),
		packages: make(map[string]*goTestPackageBuilder),
		builds:   make(map[string]*goTestBuildBuilder),
	}
}

func (r *goTestRecorder) Write(p []byte) (int, error) {
	for _, b := range p {
		r.line.WriteByte(b)
		if b == '\n' {
			r.recordLine(strings.TrimSuffix(r.line.String(), "\n"))
			r.line.Reset()
		}
	}
	return len(p), nil
}

func (r *goTestRecorder) recordLine(line string) {
	var event goTestJSONEvent
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		r.unstructured.WriteString(line)
		r.unstructured.WriteByte('\n')
		r.console.unstructuredOutput(line)
		return
	}
	r.record(event)
}

func (r *goTestRecorder) record(event goTestJSONEvent) {
	if event.Action == "build-output" || event.Action == "build-fail" {
		build := r.build(event.ImportPath)
		if event.Output != "" {
			build.output.WriteString(event.Output)
			r.console.packageOutput(event.Output)
		}
		if event.Action == "build-fail" {
			build.failed = true
		}
		return
	}

	if event.Package == "" {
		if event.Output != "" {
			r.unstructured.WriteString(event.Output)
			r.console.packageOutput(event.Output)
		}
		return
	}
	pkg := r.pkg(event.Package)
	if event.Test != "" {
		if strings.HasPrefix(event.Test, "Benchmark") {
			if event.Output != "" {
				pkg.excludedTestOutput.WriteString(event.Output)
			}
			return
		}
		r.recordTestEvent(pkg, event)
		return
	}

	if event.Output != "" {
		pkg.output.WriteString(event.Output)
		if match := coverageLine.FindStringSubmatch(strings.TrimSpace(event.Output)); match != nil {
			coverage, err := strconv.ParseFloat(match[1], 64)
			if err == nil {
				pkg.coverage = &coverage
			}
		}
		r.console.packageOutput(event.Output)
	}
	switch event.Action {
	case "start":
		pkg.startedAt = event.Time
	case "pass":
		pkg.outcome = packagePassed
		pkg.duration = durationSeconds(event.Elapsed)
	case "fail":
		pkg.outcome = packageFailed
		pkg.duration = durationSeconds(event.Elapsed)
		pkg.failedBuild = event.FailedBuild
	case "skip":
		pkg.outcome = packageSkipped
		pkg.duration = durationSeconds(event.Elapsed)
	default:
	}
}

func (r *goTestRecorder) recordTestEvent(pkg *goTestPackageBuilder, event goTestJSONEvent) {
	id := testID{packageName: event.Package, testName: event.Test}
	execution := pkg.active[id]
	if execution == nil ||
		execution.terminal && (event.Action == "run" || isTerminalTestAction(event.Action)) {
		execution = pkg.open(id)
	} else if execution.terminal {
		if event.Output != "" {
			execution.output.WriteString(event.Output)
		}
		return
	}
	if event.Output != "" {
		execution.output.WriteString(event.Output)
	}

	switch event.Action {
	case "pass", "fail", "skip":
		execution.terminal = true
		execution.duration = durationSeconds(event.Elapsed)
		switch event.Action {
		case "fail":
			execution.outcome = testFailed
		case "skip":
			execution.outcome = testSkipped
		default:
			execution.outcome = testPassed
		}
		if execution.outcome == testFailed {
			finished := execution.finish()
			r.console.completeTest(finished, packageBuilderHasFailedDescendant(pkg, finished))
		}
	default:
	}
}

func isTerminalTestAction(action string) bool {
	switch action {
	case "pass", "fail", "skip":
		return true
	default:
		return false
	}
}

func (r *goTestRecorder) pkg(name string) *goTestPackageBuilder {
	if pkg := r.packages[name]; pkg != nil {
		return pkg
	}
	pkg := &goTestPackageBuilder{
		name:        name,
		active:      make(map[testID]*goTestExecutionBuilder),
		occurrences: make(map[testID]int),
	}
	r.packages[name] = pkg
	r.packageOrder = append(r.packageOrder, name)
	return pkg
}

func (r *goTestRecorder) build(importPath string) *goTestBuildBuilder {
	if build := r.builds[importPath]; build != nil {
		return build
	}
	build := &goTestBuildBuilder{importPath: importPath}
	r.builds[importPath] = build
	r.buildOrder = append(r.buildOrder, importPath)
	return build
}

func (p *goTestPackageBuilder) open(id testID) *goTestExecutionBuilder {
	rootID := testID{packageName: id.packageName, testName: strings.SplitN(id.testName, "/", 2)[0]}
	occurrence := p.occurrences[id]
	if id != rootID {
		if root := p.active[rootID]; root != nil && !root.terminal {
			occurrence = root.occurrence
			p.occurrences[id] = max(p.occurrences[id], occurrence+1)
		} else {
			p.occurrences[id]++
		}
	} else {
		p.occurrences[id]++
	}
	execution := &goTestExecutionBuilder{
		id:         id,
		occurrence: occurrence,
		outcome:    testIncomplete,
	}
	p.active[id] = execution
	p.executions = append(p.executions, execution)
	return execution
}

func (b *goTestExecutionBuilder) finish() testExecution {
	output := b.output.String()
	return testExecution{
		id:         b.id,
		occurrence: b.occurrence,
		outcome:    b.outcome,
		duration:   b.duration,
		output:     output,
		failure:    extractFailureEvidence(output),
	}
}

func packageBuilderHasFailedDescendant(pkg *goTestPackageBuilder, parent testExecution) bool {
	prefix := parent.id.testName + "/"
	for _, execution := range pkg.executions {
		if execution.id.packageName == parent.id.packageName &&
			execution.occurrence == parent.occurrence && execution.terminal &&
			execution.outcome == testFailed && strings.HasPrefix(execution.id.testName, prefix) {
			return true
		}
	}
	return false
}

func durationSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func (r *goTestRecorder) finish(process processResult) attemptResult {
	if r.line.Len() > 0 {
		r.recordLine(r.line.String())
		r.line.Reset()
	}
	result := attemptResult{
		process:            process,
		unstructuredOutput: r.unstructured.String(),
	}
	for _, name := range r.packageOrder {
		builder := r.packages[name]
		pkg := packageResult{
			name:        builder.name,
			startedAt:   builder.startedAt,
			duration:    builder.duration,
			outcome:     builder.outcome,
			coverage:    builder.coverage,
			output:      builder.output.String(),
			failedBuild: builder.failedBuild,
		}
		if builder.outcome == packageFailed {
			pkg.output += builder.excludedTestOutput.String()
		}
		for _, execution := range builder.executions {
			finished := execution.finish()
			pkg.executions = append(pkg.executions, finished)
			if finished.outcome == testIncomplete {
				// A package abort leaves scheduler framing buffered for every unfinished
				// test. Surface high-priority diagnostics without presenting ordinary
				// output from unfinished tests as live failures, while retaining the full
				// event-owned output in the canonical result.
				diagnostics := extractDiagnostics(outputScope{
					packageName: name,
					test:        &finished.id,
				}, finished.output)
				r.console.incompleteDiagnostics(finished, diagnostics)
			}
		}
		result.packages = append(result.packages, pkg)
	}
	for _, importPath := range r.buildOrder {
		build := r.builds[importPath]
		result.builds = append(result.builds, buildResult{
			importPath: build.importPath,
			failed:     build.failed,
			output:     build.output.String(),
		})
	}
	result.diagnostics = collectAttemptDiagnostics(result)
	r.console.finish(result)
	return result
}
