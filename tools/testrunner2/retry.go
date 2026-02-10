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

// retryItemFunc converts failed test names into a queueItem for a given attempt.
type retryItemFunc func(failedNames []string, attempt int) *queueItem

// retryHandler encapsulates the retry callback for test failures.
type retryHandler struct {
	forFailures func([]string, int) []*queueItem
}

// buildRetryHandler wires up the retry callback using a shared retryItemFunc.
func (r *runner) buildRetryHandler(makeItem retryItemFunc) retryHandler {
	return retryHandler{
		forFailures: func(failedNames []string, attempt int) []*queueItem {
			r.log("🔄 scheduling retry: %s", buildTestFilterPattern(failedNames))
			if item := makeItem(failedNames, attempt+1); item != nil {
				return []*queueItem{item}
			}
			return nil
		},
	}
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
