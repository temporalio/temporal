package testrunner

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type retryMode uint8

const (
	retryStop retryMode = iota
	retryRepeatScope
	retryTargeted
)

type retryPolicy struct {
	targetedThreshold int
}

type retryPlan struct {
	mode     retryMode
	reason   string
	expected []testID
}

func (p retryPolicy) plan(result attemptResult) retryPlan {
	if result.successful() {
		return retryPlan{mode: retryStop, reason: "test attempt succeeded"}
	}
	if result.process.state != processExited ||
		slices.ContainsFunc(result.diagnostics, func(diagnostic diagnostic) bool {
			return diagnostic.kind == diagnosticTimeout
		}) {
		return retryPlan{mode: retryStop, reason: "test attempt cannot be retried safely"}
	}
	if result.unexplainedProcessFailure() {
		return retryPlan{mode: retryStop, reason: "test attempt failed without any attributable failure"}
	}
	// A package abort can leave both incomplete tests and tests that never
	// started, so observed test names cannot safely narrow the rerun. This also
	// applies when a panic caused the abort.
	if !result.canTargetFailures() {
		return retryPlan{mode: retryRepeatScope, reason: "failure is not safely attributable to individual tests"}
	}
	failed := result.failedLeafTests()
	if len(failed) > p.targetedThreshold {
		return retryPlan{
			mode:   retryRepeatScope,
			reason: fmt.Sprintf("%d failures exceed targeted retry threshold %d", len(failed), p.targetedThreshold),
		}
	}
	return retryPlan{
		mode:     retryTargeted,
		reason:   fmt.Sprintf("retrying %d failed test(s)", len(failed)),
		expected: failed,
	}
}

func (p retryPlan) apply(args []string) []string {
	args = slices.Clone(args)
	if p.mode != retryTargeted {
		return args
	}
	args = stripRunFromArgs(args)
	names := make([]string, 0, len(p.expected))
	for _, id := range p.expected {
		names = append(names, id.testName)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	for i := range names {
		names[i] = goTestNameToRunFlagRegexp(names[i])
	}
	runArgs := []string{"-run", strings.Join(names, "|")}
	// -args has special semantics in Go.
	if argsIndex := slices.Index(args, "-args"); argsIndex >= 0 {
		return slices.Insert(args, argsIndex, runArgs...)
	}
	return append(args, runArgs...)
}

func (p retryPlan) validate(result attemptResult) error {
	if p.mode != retryTargeted {
		return nil
	}
	for _, expected := range p.expected {
		if result.observed(expected) || result.packageBlockedObservation(expected.packageName) {
			continue
		}
		return fmt.Errorf("expected targeted rerun was not observed: %s.%s", expected.packageName, expected.testName)
	}
	return nil
}

func stripRunFromArgs(args []string) (argsNoRun []string) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-args" {
			return append(argsNoRun, args[i:]...)
		}
		if arg == "-run" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "-run=") {
			continue
		}
		argsNoRun = append(argsNoRun, arg)
	}
	return
}

func goTestNameToRunFlagRegexp(test string) string {
	parts := strings.Split(test, "/")
	var expression strings.Builder
	for i, part := range parts {
		if i > 0 {
			expression.WriteByte('/')
		}
		expression.WriteByte('^')
		expression.WriteString(regexp.QuoteMeta(part))
		expression.WriteByte('$')
	}
	return expression.String()
}
