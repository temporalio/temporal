package leakcheck

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

type report struct {
	retainedObjects   []objectGroup
	totalRetained     int
	excludedRetained  int
	exclusionCounts   map[string]int
	unmatchedExcludes []string
}

type objectGroup struct {
	path       string
	typeName   string
	excludedBy []string
	count      int
}

func newReport(objects []trackedObject, excludes exclusions) report {
	report := report{
		exclusionCounts: make(map[string]int),
	}
	// Matching mutates exclusion.matched for stale-exclusion detection.
	activeExclusions := slices.Clone(excludes)

	type groupKey struct {
		path       string
		typeName   string
		excludedBy string
	}
	groupByKey := make(map[groupKey]*objectGroup)
	for _, obj := range objects {
		excludedBy := activeExclusions.match(obj)
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
			path:       obj.path.normalized(),
			typeName:   obj.typeName,
			excludedBy: strings.Join(excludedBy, "\x00"),
		}
		group := groupByKey[key]
		if group == nil {
			group = &objectGroup{
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
	slices.Sort(report.unmatchedExcludes)
	sortObjectGroups(report.retainedObjects)
	return report
}

func (r report) failures() error {
	var failures []error
	for _, group := range r.retainedObjects {
		if len(group.excludedBy) > 0 {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s) retained %d times", group.path, group.typeName, group.count))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object exclusion %q did not match any object", pattern))
	}
	return errors.Join(failures...)
}

func (r report) string() string {
	if r.totalRetained == 0 && len(r.unmatchedExcludes) == 0 {
		return ""
	}

	var out strings.Builder
	r.writeSummary(&out)
	out.WriteString("\n\nretained objects:\n")

	for _, group := range r.retainedObjects {
		fmt.Fprintf(&out, "  %dx %s (%s)", group.count, group.path, group.typeName)
		if len(group.excludedBy) > 0 {
			fmt.Fprintf(&out, " [excluded by %s]", strings.Join(group.excludedBy, ", "))
		}
		out.WriteByte('\n')
	}
	if len(r.unmatchedExcludes) > 0 {
		out.WriteString("\nstale exclusions:\n")
	}
	for _, pattern := range r.unmatchedExcludes {
		fmt.Fprintf(&out, "  %s\n", pattern)
	}
	return strings.TrimSuffix(out.String(), "\n")
}

func sortObjectGroups(groups []objectGroup) {
	slices.SortFunc(groups, func(a objectGroup, b objectGroup) int {
		if c := cmp.Compare(b.count, a.count); c != 0 {
			return c
		}
		if c := cmp.Compare(a.path, b.path); c != 0 {
			return c
		}
		if c := cmp.Compare(a.typeName, b.typeName); c != 0 {
			return c
		}
		return cmp.Compare(strings.Join(a.excludedBy, "\x00"), strings.Join(b.excludedBy, "\x00"))
	})
}

func (r report) writeSummary(out *strings.Builder) {
	out.WriteString("object leak report\n")
	fmt.Fprintf(out, "retained objects: %d total, %d excluded, %d unexcluded\n", r.totalRetained, r.excludedRetained, r.totalRetained-r.excludedRetained)
	fmt.Fprintf(out, "stale exclusions: %d", len(r.unmatchedExcludes))
	if len(r.exclusionCounts) > 0 {
		out.WriteString("\nretained objects by exclusion:")
		for _, pattern := range sortedMapKeys(r.exclusionCounts) {
			fmt.Fprintf(out, "\n  %s: %d", pattern, r.exclusionCounts[pattern])
		}
	}
}

func sortedMapKeys[V any](values map[string]V) []string {
	return slices.Sorted(maps.Keys(values))
}

func sortedStrings(values []string) []string {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted
}
