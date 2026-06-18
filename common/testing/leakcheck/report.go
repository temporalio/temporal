package leakcheck

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type objectGraphReport struct {
	retainedObjects   []retainedObjectGroup
	totalRetained     int
	excludedRetained  int
	exclusionCounts   map[string]int
	unmatchedExcludes []string
}

type retainedObjectGroup struct {
	path       string
	typeName   string
	excludedBy []string
	count      int
}

func newObjectGraphReport(objects []trackedObject, excludes exclusions) objectGraphReport {
	report := objectGraphReport{
		exclusionCounts: make(map[string]int),
	}
	activeExclusions := append(exclusions(nil), excludes...)

	type groupKey struct {
		path       string
		typeName   string
		excludedBy string
	}
	groupByKey := make(map[groupKey]*retainedObjectGroup)
	for _, obj := range objects {
		excludedBy := activeExclusions.Match(obj)
		if obj.collected.Load() {
			continue
		}

		report.totalRetained++
		if len(excludedBy) > 0 {
			report.excludedRetained++
		}
		for _, pattern := range excludedBy {
			report.exclusionCounts[pattern]++
		}

		excludedBy = sortedStrings(excludedBy)
		key := groupKey{
			path:       obj.path.Normalized(),
			typeName:   obj.typeName,
			excludedBy: strings.Join(excludedBy, "\x00"),
		}
		group := groupByKey[key]
		if group == nil {
			group = &retainedObjectGroup{
				path:       key.path,
				typeName:   key.typeName,
				excludedBy: excludedBy,
			}
			groupByKey[key] = group
		}
		group.count++
	}
	for _, exclusion := range activeExclusions {
		if !exclusion.matched {
			report.unmatchedExcludes = append(report.unmatchedExcludes, exclusion.pattern)
		}
	}
	for _, group := range groupByKey {
		report.retainedObjects = append(report.retainedObjects, *group)
	}
	sort.Strings(report.unmatchedExcludes)
	sortRetainedObjectGroups(report.retainedObjects)
	return report
}

func (r objectGraphReport) failures() error {
	var failures []error
	for _, group := range r.retainedObjects {
		if len(group.excludedBy) > 0 {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s) retained %d times", group.path, group.typeName, group.count))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object graph exclusion %q did not match any object", pattern))
	}
	return errors.Join(failures...)
}

func (r objectGraphReport) String() string {
	if r.totalRetained == 0 && len(r.unmatchedExcludes) == 0 {
		return ""
	}

	var lines []string
	lines = append(lines, r.summaryLines()...)
	lines = append(lines, "", "retained objects:")

	for _, group := range r.retainedObjects {
		line := fmt.Sprintf("  %dx %s (%s)", group.count, group.path, group.typeName)
		if len(group.excludedBy) > 0 {
			line += fmt.Sprintf(" [excluded by %s]", strings.Join(group.excludedBy, ", "))
		}
		lines = append(lines, line)
	}
	if len(r.unmatchedExcludes) > 0 {
		lines = append(lines, "", "stale exclusions:")
	}
	for _, pattern := range r.unmatchedExcludes {
		lines = append(lines, fmt.Sprintf("  %s", pattern))
	}
	return strings.Join(lines, "\n")
}

func sortRetainedObjectGroups(groups []retainedObjectGroup) {
	sort.Slice(groups, func(i int, j int) bool {
		if groups[i].count != groups[j].count {
			return groups[i].count > groups[j].count
		}
		if groups[i].path != groups[j].path {
			return groups[i].path < groups[j].path
		}
		if groups[i].typeName != groups[j].typeName {
			return groups[i].typeName < groups[j].typeName
		}
		return strings.Join(groups[i].excludedBy, "\x00") < strings.Join(groups[j].excludedBy, "\x00")
	})
}

func (r objectGraphReport) summaryLines() []string {
	lines := []string{
		"object graph leak report",
		fmt.Sprintf("retained objects: %d total, %d excluded, %d unexcluded", r.totalRetained, r.excludedRetained, r.totalRetained-r.excludedRetained),
		fmt.Sprintf("stale exclusions: %d", len(r.unmatchedExcludes)),
	}
	if len(r.exclusionCounts) > 0 {
		lines = append(lines, "retained objects by exclusion:")
		for _, pattern := range sortedMapKeys(r.exclusionCounts) {
			lines = append(lines, fmt.Sprintf("  %s: %d", pattern, r.exclusionCounts[pattern]))
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
