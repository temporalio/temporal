package leakcheck

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type report struct {
	unexpectedObjects []objectGroup
	expectedObjects   []objectGroup
	trackedRoots      int
	totalRetained     int
	expectedRetained  int
	unmatchedExcludes []string
}

type objectGroup struct {
	path     string
	typeName string
	count    int
}

func newReport(objects []trackedObject, trackedRoots int, excludes exclusions) report {
	report := report{
		trackedRoots: trackedRoots,
	}

	// Matching mutates exclusion.matched for stale-exclusion detection.
	activeExclusions := slices.Clone(excludes)

	type groupKey struct {
		path     string
		typeName string
		expected bool
	}
	groupByKey := make(map[groupKey]*objectGroup)

	// Classify each retained object and fold equivalent normalized paths into
	// a single report row.
	for _, obj := range objects {
		excludedBy := activeExclusions.match(obj)
		if obj.collected.Load() {
			continue
		}

		report.totalRetained++
		expected := len(excludedBy) > 0
		if expected {
			report.expectedRetained++
		}

		key := groupKey{
			path:     obj.path.normalized(),
			typeName: obj.typeName,
			expected: expected,
		}
		group := groupByKey[key]
		if group == nil {
			group = &objectGroup{
				path:     key.path,
				typeName: key.typeName,
			}
			groupByKey[key] = group
		}
		group.count++
	}

	// Exclusions that never matched any tracked object are stale and should be
	// removed with the fix that made them unnecessary.
	for _, exclusion := range activeExclusions {
		if !exclusion.matched {
			report.unmatchedExcludes = append(report.unmatchedExcludes, exclusion.pattern)
		}
	}

	// Keep report output stable across map iteration order and repeated runs.
	for key, group := range groupByKey {
		if key.expected {
			report.expectedObjects = append(report.expectedObjects, *group)
		} else {
			report.unexpectedObjects = append(report.unexpectedObjects, *group)
		}
	}
	sortGroups := func(groups []objectGroup) {
		slices.SortFunc(groups, func(a objectGroup, b objectGroup) int {
			if c := cmp.Compare(b.count, a.count); c != 0 {
				return c
			}
			if c := cmp.Compare(a.path, b.path); c != 0 {
				return c
			}
			return cmp.Compare(a.typeName, b.typeName)
		})
	}
	sortGroups(report.unexpectedObjects)
	sortGroups(report.expectedObjects)
	slices.Sort(report.unmatchedExcludes)
	return report
}

func (r report) failures() error {
	var failures []error
	for _, group := range r.unexpectedObjects {
		failures = append(failures, fmt.Errorf("retained object %s (%s) retained %d times", group.path, group.typeName, group.count))
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

	writeGroups := func(title string, groups []objectGroup) {
		fmt.Fprintf(&out, "\n\n%s:\n", title)
		if len(groups) == 0 {
			out.WriteString("  none\n")
			return
		}
		for _, group := range groups {
			fmt.Fprintf(&out, "  %dx %s (%s)\n", group.count, group.path, group.typeName)
		}
	}
	writeGroups("unexpected retained objects", r.unexpectedObjects)
	writeGroups("expected retained objects", r.expectedObjects)

	if len(r.unmatchedExcludes) > 0 {
		out.WriteString("\n\nstale exclusions:\n")
	}
	for _, pattern := range r.unmatchedExcludes {
		fmt.Fprintf(&out, "  %s\n", pattern)
	}
	return strings.TrimSuffix(out.String(), "\n")
}

func (r report) writeSummary(out *strings.Builder) {
	out.WriteString("object leak report\n\n")
	fmt.Fprintf(out, "tracked root objects: %d\n", r.trackedRoots)
	fmt.Fprintf(out, "retained objects: %d total, %d expected, %d unexpected\n", r.totalRetained, r.expectedRetained, r.totalRetained-r.expectedRetained)
	fmt.Fprintf(out, "stale exclusions: %d", len(r.unmatchedExcludes))
}
