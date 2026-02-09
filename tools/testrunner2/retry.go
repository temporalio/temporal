package testrunner2

import (
	"slices"
	"strings"
)

// classifyAlerts determines the failure kind from a set of alerts.
func classifyAlerts(detectedAlerts alerts) string {
	for _, a := range detectedAlerts {
		switch a.Kind {
		case failureKindTimeout:
			return "timeout"
		case failureKindCrash, failureKindPanic, failureKindFatal, failureKindDataRace:
			return "crash"
		default:
		}
	}
	return ""
}

// filterEmitted returns failures not already emitted as mid-stream retries.
// It also filters out parent tests whose children were already emitted, since
// retrying the parent would duplicate the child retry.
func filterEmitted(failures []testFailure, emitted map[string]bool) []testFailure {
	var out []testFailure
	for _, f := range failures {
		if emitted[f.Name] {
			continue
		}
		// Also filter parent tests if any child was already emitted.
		isParentOfEmitted := false
		for name := range emitted {
			if strings.HasPrefix(name, f.Name+"/") {
				isParentOfEmitted = true
				break
			}
		}
		if isParentOfEmitted {
			continue
		}
		out = append(out, f)
	}
	return out
}

// retryItemFunc converts a retryPlan into a queueItem for a given attempt.
type retryItemFunc func(plan retryPlan, attempt int) *queueItem

// retryHandler encapsulates the three retry strategies (failures, crash, unknown)
// built from a shared retryItemFunc.
type retryHandler struct {
	forFailures func([]string, int) []*queueItem
	forCrash    func([]string, []string, int) []*queueItem
	forUnknown  func([]string, int) []*queueItem
}

// buildRetryHandler wires up the three retry callbacks using a shared retryItemFunc.
func (r *runner) buildRetryHandler(makeItem retryItemFunc) retryHandler {
	h := retryHandler{}

	h.forFailures = func(failedNames []string, attempt int) []*queueItem {
		plan := retryPlan{tests: failedNames}
		r.log("🔄 scheduling retry: -run %s", buildTestFilterPattern(failedNames))
		if item := makeItem(plan, attempt+1); item != nil {
			return []*queueItem{item}
		}
		return nil
	}

	h.forCrash = func(passedNames, quarantinedNames []string, attempt int) []*queueItem {
		plans := buildRetryPlans(passedNames, quarantinedNames)
		var items []*queueItem
		for _, p := range plans {
			switch {
			case p.tests != nil && p.skipTests != nil:
				r.log("🔄 scheduling retry: -run %q -skip %q",
					buildTestFilterPattern(p.tests),
					buildTestFilterPattern(p.skipTests))
			case p.skipTests != nil:
				r.log("🔄 scheduling retry: -skip %q",
					buildTestFilterPattern(p.skipTests))
			default:
				r.log("🔄 scheduling retry...")
			}
			if item := makeItem(p, attempt+1); item != nil {
				items = append(items, item)
			}
		}
		return items
	}

	h.forUnknown = func(passedNames []string, attempt int) []*queueItem {
		return h.forCrash(passedNames, nil, attempt)
	}

	return h
}

// retryPlan describes a single retry invocation in terms of -run/-skip test names.
type retryPlan struct {
	tests     []string // tests to -run (nil = all from original set)
	skipTests []string // tests to -skip
}

// buildRetryPlans computes retry plans with quarantine logic.
//
// When quarantinedTests is non-empty and there are passed sibling tests under
// the same parent, the quarantined test is isolated into a separate plan. This
// avoids mixed-depth skip patterns (e.g., "TestA" at depth 1 and "TestB/Var1"
// at depth 2) that can't be expressed cleanly in a single -test.skip pattern.
func buildRetryPlans(passedTests, quarantinedTests []string) []retryPlan {
	if len(passedTests) == 0 || len(quarantinedTests) == 0 {
		// Simple case: no quarantine needed.
		// Pass through passedTests directly; buildTestFilterPattern handles the
		// per-level regex. Some shallower names may be dropped from the pattern
		// (causing those passed tests to re-run) but never over-skipped.
		return []retryPlan{{skipTests: passedTests}}
	}

	// Build quarantine plans for each quarantined test.
	var quarantinePlans []retryPlan
	quarantinedParents := make(map[string]bool)
	quarantinedTopLevel := make(map[string]bool)
	for _, qt := range quarantinedTests {
		parent := parentTestName(qt)
		if parent == "" {
			// Top-level test (e.g., panicking TestFoo): retry in isolation,
			// skipping subtests that already passed.
			if !quarantinedTopLevel[qt] {
				quarantinedTopLevel[qt] = true
				quarantinePlans = append(quarantinePlans, retryPlan{
					tests:     []string{qt},
					skipTests: filterByPrefix(passedTests, qt),
				})
			}
			continue
		}
		if quarantinedParents[parent] {
			continue // already created a quarantine plan for this parent
		}
		siblings := filterByPrefix(passedTests, parent)
		if len(siblings) == 0 {
			continue // no passed siblings to skip, no need to quarantine
		}
		quarantinedParents[parent] = true
		quarantinePlans = append(quarantinePlans, retryPlan{
			tests:     []string{parent},
			skipTests: siblings,
		})
	}

	if len(quarantinePlans) == 0 {
		// No quarantine was possible, fall back to simple case
		return []retryPlan{{skipTests: passedTests}}
	}

	// Build the regular retry plan: skip all passed tests + quarantined tests.
	// We include the quarantined tests themselves (at their full depth) rather
	// than adding parent names at a shallower depth. Mixed depths would cause
	// buildTestFilterPattern's per-level pattern to drop the shallower entries,
	// silently un-skipping the quarantined parent.
	regularSkip := append(slices.Clone(passedTests), quarantinedTests...)

	return append(quarantinePlans, retryPlan{skipTests: regularSkip})
}

// quarantinedTestNames extracts test names from alerts that crash the process.
// These tests are quarantined (retried in isolation) so they don't take down
// the bulk retry of remaining tests.
func quarantinedTestNames(as alerts) []string {
	var names []string
	for _, a := range as {
		switch a.Kind {
		case failureKindTimeout, failureKindPanic, failureKindFatal, failureKindDataRace:
			for _, t := range a.Tests {
				// Panic/fatal/race alerts use fully-qualified names
				// (e.g., "pkg.TestFoo.func1.3") while the rest of the
				// retry system uses plain names ("TestFoo"). Clean them.
				_, name := splitTestName(t)
				names = append(names, name)
			}
		default:
		}
	}
	return names
}

// parentTestName returns the parent of a test name by truncating the last "/" segment.
// Returns "" if the name has no parent (top-level test).
func parentTestName(name string) string {
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		return name[:idx]
	}
	return ""
}

// filterByPrefix returns names that start with prefix + "/".
func filterByPrefix(names []string, prefix string) []string {
	p := prefix + "/"
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, p) {
			out = append(out, n)
		}
	}
	return out
}

// mergeUnique merges two string slices, deduplicating entries. Used to accumulate
// skip test lists across retry attempts so that subtests that passed in earlier
// attempts remain skipped in later retries.
func mergeUnique(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]bool, len(a)+len(b))
	result := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}

// filterParentFailures removes parent test names from the failure list when
// subtests of that parent are also present. Go's test framework marks parent
// tests as failed whenever a child fails, but these parent entries are redundant
// for retry purposes and cause mixed-depth names that break buildTestFilterPattern.
func filterParentFailures(failures []testFailure) []testFailure {
	names := make([]string, len(failures))
	for i, f := range failures {
		names[i] = f.Name
	}
	slices.Sort(names)
	leafSet := make(map[string]bool, len(names))
	for _, name := range filterParentNames(names) {
		leafSet[name] = true
	}
	var filtered []testFailure
	for _, f := range failures {
		if leafSet[f.Name] {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// wouldSkipAll returns true if every test in tests has a matching entry in skipTests.
// Used to detect vacuous retry plans where run and skip patterns cancel out.
func wouldSkipAll(tests []testCase, skipTests []string) bool {
	if len(skipTests) == 0 || len(tests) == 0 {
		return false
	}
	skipSet := make(map[string]bool, len(skipTests))
	for _, s := range skipTests {
		skipSet[s] = true
	}
	for _, tc := range tests {
		if !skipSet[tc.name] {
			return false
		}
	}
	return true
}

// incrementAttempts returns a copy of tests with attempts set to the given value.
func incrementAttempts(tests []testCase, attempt int) []testCase {
	result := make([]testCase, len(tests))
	for i, tc := range tests {
		result[i] = testCase{name: tc.name, attempts: attempt}
	}
	return result
}
