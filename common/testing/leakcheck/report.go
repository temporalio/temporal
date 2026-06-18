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
	invalidExcludes   []string
}

type retainedObject struct {
	path       string
	typeName   string
	excludedBy []string
}

type retainedObjectGroup struct {
	path       string
	typeName   string
	excludedBy []string
	count      int
}

func newObjectGraphReport(objects []trackedObject, excludes []string) objectGraphReport {
	var report objectGraphReport
	matchedExcludes := make(map[string]bool, len(excludes))
	for _, pattern := range excludes {
		if hasSpecificPathIndex(pattern) {
			report.invalidExcludes = append(report.invalidExcludes, pattern)
		}
	}
	for _, obj := range objects {
		excludedBy := matchingExcludes(obj, excludes)
		for _, pattern := range excludedBy {
			matchedExcludes[pattern] = true
		}
		if obj.collected.Load() {
			continue
		}
		report.retainedObjects = append(report.retainedObjects, retainedObject{
			path:       obj.path,
			typeName:   obj.typeName,
			excludedBy: excludedBy,
		})
	}
	for _, pattern := range excludes {
		if !matchedExcludes[pattern] {
			report.unmatchedExcludes = append(report.unmatchedExcludes, pattern)
		}
	}
	return report
}

func matchingExcludes(obj trackedObject, excludes []string) []string {
	var matches []string
	path := normalizePathIndexes(obj.path)
	matchesPattern := func(pattern string, value string) bool {
		if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
			return strings.HasPrefix(value, prefix)
		}
		return value == pattern
	}
	for _, pattern := range excludes {
		if hasSpecificPathIndex(pattern) {
			continue
		}
		if matchesPattern(pattern, path) || matchesPattern(pattern, obj.typeName) {
			matches = append(matches, pattern)
		}
	}
	return matches
}

func (r objectGraphReport) failures() error {
	var failures []error
	for _, group := range r.groups() {
		if len(group.excludedBy) > 0 {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s) retained %d times", group.path, group.typeName, group.count))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object graph exclusion %q did not match any object", pattern))
	}
	for _, pattern := range r.invalidExcludes {
		failures = append(failures, fmt.Errorf("object graph exclusion %q targets a specific index; use [*] or [key*]", pattern))
	}
	return errors.Join(failures...)
}

func (r objectGraphReport) String() string {
	if len(r.retainedObjects) == 0 && len(r.unmatchedExcludes) == 0 && len(r.invalidExcludes) == 0 {
		return ""
	}

	var lines []string
	lines = append(lines, r.summaryLines()...)
	lines = append(lines, "", "retained objects:")

	for _, group := range r.groups() {
		line := fmt.Sprintf("  %dx %s (%s)", group.count, group.path, group.typeName)
		if len(group.excludedBy) > 0 {
			line += fmt.Sprintf(" [excluded by %s]", strings.Join(group.excludedBy, ", "))
		}
		lines = append(lines, line)
	}
	sort.Strings(r.unmatchedExcludes)
	if len(r.unmatchedExcludes) > 0 {
		lines = append(lines, "", "stale exclusions:")
	}
	for _, pattern := range r.unmatchedExcludes {
		lines = append(lines, fmt.Sprintf("  %s", pattern))
	}
	sort.Strings(r.invalidExcludes)
	if len(r.invalidExcludes) > 0 {
		lines = append(lines, "", "invalid exclusions:")
	}
	for _, pattern := range r.invalidExcludes {
		lines = append(lines, fmt.Sprintf("  %s targets a specific index; use [*] or [key*]", pattern))
	}
	return strings.Join(lines, "\n")
}

func (r objectGraphReport) groups() []retainedObjectGroup {
	type groupKey struct {
		path       string
		typeName   string
		excludedBy string
	}

	groupByKey := make(map[groupKey]*retainedObjectGroup)
	for _, obj := range r.retainedObjects {
		excludedBy := sortedStrings(obj.excludedBy)
		key := groupKey{
			path:       normalizePathIndexes(obj.path),
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

	groups := make([]retainedObjectGroup, 0, len(groupByKey))
	for _, group := range groupByKey {
		groups = append(groups, *group)
	}
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
	return groups
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
		fmt.Sprintf("invalid exclusions: %d", len(r.invalidExcludes)),
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

func normalizePathIndexes(path string) string {
	var out strings.Builder
	for i := 0; i < len(path); {
		if path[i] != '[' {
			out.WriteByte(path[i])
			i++
			continue
		}

		end := strings.IndexByte(path[i:], ']')
		if end < 0 {
			out.WriteString(path[i:])
			break
		}
		end += i
		index := path[i+1 : end]
		switch {
		case allDigits(index):
			out.WriteString("[*]")
		case strings.HasPrefix(index, "key") && allDigits(strings.TrimPrefix(index, "key")):
			out.WriteString("[key*]")
		default:
			out.WriteString(path[i : end+1])
		}
		i = end + 1
	}
	return out.String()
}

func hasSpecificPathIndex(path string) bool {
	for i := 0; i < len(path); {
		if path[i] != '[' {
			i++
			continue
		}

		end := strings.IndexByte(path[i:], ']')
		if end < 0 {
			return false
		}
		end += i
		index := path[i+1 : end]
		if allDigits(index) || strings.HasPrefix(index, "key") && allDigits(strings.TrimPrefix(index, "key")) {
			return true
		}
		i = end + 1
	}
	return false
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
