package testrunner

import (
	"slices"
	"strings"
	"time"
)

type testID struct {
	packageName string
	testName    string
}

type testOutcome uint8

const (
	testIncomplete testOutcome = iota
	testPassed
	testFailed
	testSkipped
)

type packageOutcome uint8

const (
	packageIncomplete packageOutcome = iota
	packagePassed
	packageFailed
	packageSkipped
)

type failureEvidence struct {
	details string
	// actionable means evidence is attributed to this execution, so it can be
	// reported and retried as a failed leaf even when execution is incomplete.
	actionable bool
}

type testExecution struct {
	id testID
	// occurrence distinguishes repeated runs of the same test name within one
	// package, such as under -count. Parent/child matching is per occurrence.
	occurrence int
	outcome    testOutcome
	duration   time.Duration
	output     string
	failure    failureEvidence
}

type packageResult struct {
	name        string
	startedAt   time.Time
	duration    time.Duration
	outcome     packageOutcome
	coverage    *float64
	output      string
	executions  []testExecution
	failedBuild string
}

type buildResult struct {
	importPath string
	failed     bool
	output     string
}

type diagnosticKind uint8

const (
	diagnosticDataRace diagnosticKind = iota + 1
	diagnosticPanic
	diagnosticFatal
	diagnosticTimeout
)

type diagnostic struct {
	kind        diagnosticKind
	summary     string
	details     string
	packageName string
	tests       []testID
}

type processState uint8

const (
	processExited processState = iota
	processStartFailed
	processDeadlineExceeded
	processSignaled
	processWaitFailed
)

type processResult struct {
	state     processState
	exitCode  int
	startedAt time.Time
	duration  time.Duration
	stderr    string
	details   string
}

type attemptResult struct {
	packages           []packageResult
	builds             []buildResult
	diagnostics        []diagnostic
	unstructuredOutput string
	process            processResult
}

func (r attemptResult) failedLeafTests() []testID {
	seen := make(map[testID]struct{})
	var failed []testID
	for _, pkg := range r.packages {
		for _, execution := range pkg.executions {
			if !pkg.isFailedLeaf(execution) {
				continue
			}
			if _, ok := seen[execution.id]; ok {
				continue
			}
			seen[execution.id] = struct{}{}
			failed = append(failed, execution.id)
		}
	}
	slices.SortFunc(failed, compareTestID)
	return failed
}

func (p packageResult) isFailedLeaf(execution testExecution) bool {
	if !execution.isAttributableFailure() {
		return false
	}
	return execution.failure.actionable || !p.hasAttributableFailureDescendant(execution)
}

func (e testExecution) isAttributableFailure() bool {
	return e.outcome == testFailed || e.outcome == testIncomplete && e.failure.actionable
}

func (p packageResult) hasAttributableFailureDescendant(parent testExecution) bool {
	prefix := parent.id.testName + "/"
	return slices.ContainsFunc(p.executions, func(execution testExecution) bool {
		return execution.id.packageName == parent.id.packageName &&
			execution.occurrence == parent.occurrence &&
			execution.isAttributableFailure() &&
			strings.HasPrefix(execution.id.testName, prefix)
	})
}

func compareTestID(a, b testID) int {
	if byPackage := strings.Compare(a.packageName, b.packageName); byPackage != 0 {
		return byPackage
	}
	return strings.Compare(a.testName, b.testName)
}

func (r attemptResult) observed(id testID) bool {
	for _, pkg := range r.packages {
		if slices.ContainsFunc(pkg.executions, func(execution testExecution) bool {
			return execution.id == id
		}) {
			return true
		}
	}
	return false
}

func (p packageResult) meaningfulIncompleteExecutions() []testExecution {
	if p.outcome != packageFailed && p.outcome != packageIncomplete {
		return nil
	}
	var incomplete []testExecution
	for _, execution := range p.executions {
		if execution.outcome != testIncomplete {
			continue
		}
		if p.isFailedLeaf(execution) {
			continue
		}
		prefix := execution.id.testName + "/"
		hasCompletedDescendant := slices.ContainsFunc(p.executions, func(other testExecution) bool {
			return other.id.packageName == execution.id.packageName &&
				other.occurrence == execution.occurrence &&
				other.outcome != testIncomplete && strings.HasPrefix(other.id.testName, prefix)
		})
		if hasCompletedDescendant && !execution.failure.actionable {
			continue
		}
		incomplete = append(incomplete, execution)
	}
	return incomplete
}

func (r attemptResult) abortedPackages() []packageResult {
	var packages []packageResult
	for _, pkg := range r.packages {
		if r.packageHasTimeout(pkg.name) {
			continue
		}
		if pkg.outcome == packageIncomplete && r.process.state == processExited && r.process.exitCode == 0 {
			continue
		}
		if len(pkg.meaningfulIncompleteExecutions()) > 0 {
			packages = append(packages, pkg)
		}
	}
	return packages
}

func (r attemptResult) runtimeFailures() []packageResult {
	var packages []packageResult
	for _, pkg := range r.packages {
		if pkg.outcome != packageFailed || pkg.failedBuild != "" ||
			len(pkg.meaningfulIncompleteExecutions()) > 0 ||
			slices.ContainsFunc(pkg.executions, testExecution.isAttributableFailure) ||
			r.packageHasTimeout(pkg.name) {
			continue
		}
		packages = append(packages, pkg)
	}
	return packages
}

func (r attemptResult) packageHasTimeout(packageName string) bool {
	return slices.ContainsFunc(r.diagnostics, func(diagnostic diagnostic) bool {
		return diagnostic.kind == diagnosticTimeout && diagnostic.packageName == packageName
	})
}

func (r attemptResult) unexplainedProcessFailure() bool {
	if r.process.state == processExited && r.process.exitCode == 0 {
		return false
	}
	return len(r.failedLeafTests()) == 0 &&
		len(r.abortedPackages()) == 0 &&
		len(r.runtimeFailures()) == 0 &&
		!slices.ContainsFunc(r.builds, func(build buildResult) bool { return build.failed }) &&
		len(r.diagnostics) == 0
}

func (r attemptResult) canTargetFailures() bool {
	failedLeafTests := r.failedLeafTests()
	if r.process.state != processExited || len(r.abortedPackages()) > 0 ||
		len(r.runtimeFailures()) > 0 ||
		slices.ContainsFunc(r.builds, func(build buildResult) bool { return build.failed }) ||
		len(failedLeafTests) == 0 {
		return false
	}
	if slices.ContainsFunc(r.diagnostics, func(diagnostic diagnostic) bool {
		return diagnostic.kind == diagnosticTimeout || !diagnosticIsBoundToFailedLeaf(diagnostic, failedLeafTests)
	}) {
		return false
	}
	return true
}

func diagnosticIsBoundToFailedLeaf(d diagnostic, failedLeafTests []testID) bool {
	if len(d.tests) == 0 {
		return false
	}
	for _, id := range d.tests {
		if !slices.Contains(failedLeafTests, id) {
			return false
		}
	}
	return true
}

func (r attemptResult) successful() bool {
	return r.process.state == processExited && r.process.exitCode == 0 &&
		len(r.failedLeafTests()) == 0 && len(r.diagnostics) == 0 &&
		!slices.ContainsFunc(r.builds, func(build buildResult) bool { return build.failed }) &&
		!slices.ContainsFunc(r.packages, func(pkg packageResult) bool { return pkg.outcome == packageFailed })
}
