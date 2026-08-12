package testrunner2

import (
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

// parentTestName returns the parent of a test name by truncating the last "/" segment.
// Returns "" if the name has no parent (top-level test).
func parentTestName(name string) string {
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		return name[:idx]
	}
	return ""
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
