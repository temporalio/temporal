package leakcheck

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type objectGraphReport struct {
	retainedObjects   []retainedObject
	unmatchedExcludes []string
}

type retainedObject struct {
	matchPath   string
	displayPath string
	typeName    string
	excludedBy  []string
}

func (r objectGraphReport) failures() error {
	var failures []error
	for _, obj := range r.retainedObjects {
		if len(obj.excludedBy) > 0 {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s)", obj.displayPath, obj.typeName))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object graph exclusion %q did not match any object", pattern))
	}
	return errors.Join(failures...)
}

func (r objectGraphReport) String() string {
	if len(r.retainedObjects) == 0 && len(r.unmatchedExcludes) == 0 {
		return ""
	}

	var lines []string
	lines = append(lines, r.summaryLines()...)
	lines = append(lines, "", "details:")

	sort.Slice(r.retainedObjects, func(i int, j int) bool {
		if r.retainedObjects[i].displayPath != r.retainedObjects[j].displayPath {
			return r.retainedObjects[i].displayPath < r.retainedObjects[j].displayPath
		}
		return r.retainedObjects[i].typeName < r.retainedObjects[j].typeName
	})
	for _, obj := range r.retainedObjects {
		line := fmt.Sprintf("retained graph object %s (%s) [match path %s]", obj.displayPath, obj.typeName, obj.matchPath)
		if len(obj.excludedBy) > 0 {
			line += fmt.Sprintf(" [excluded by %s]", strings.Join(sortedStrings(obj.excludedBy), ", "))
		}
		lines = append(lines, line)
	}
	sort.Strings(r.unmatchedExcludes)
	for _, pattern := range r.unmatchedExcludes {
		lines = append(lines, fmt.Sprintf("object graph exclusion %q did not match any object", pattern))
	}
	return strings.Join(lines, "\n")
}

func (r objectGraphReport) summaryLines() []string {
	excludedCount := 0
	unexcludedCount := 0
	exclusionCounts := make(map[string]int)
	for _, obj := range r.retainedObjects {
		if len(obj.excludedBy) == 0 {
			unexcludedCount++
			continue
		}
		excludedCount++
		for _, pattern := range obj.excludedBy {
			exclusionCounts[pattern]++
		}
	}

	lines := []string{
		"object graph leak report",
		fmt.Sprintf("retained objects: %d total, %d excluded, %d unexcluded", len(r.retainedObjects), excludedCount, unexcludedCount),
		fmt.Sprintf("stale exclusions: %d", len(r.unmatchedExcludes)),
	}
	if len(exclusionCounts) > 0 {
		lines = append(lines, "retained objects by exclusion:")
		for _, pattern := range sortedMapKeys(exclusionCounts) {
			lines = append(lines, fmt.Sprintf("  %s: %d", pattern, exclusionCounts[pattern]))
		}
	}
	return lines
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStrings(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}
