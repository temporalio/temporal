package objectleak

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type report struct {
	unexpectedObjects              []objectGroup
	expectedObjects                []objectGroup
	trackedRoots                   int
	totalRetainedObservations      int
	totalRetainedObjects           int
	expectedRetainedObservations   int
	expectedRetainedObjects        int
	unexpectedRetainedObservations int
	unexpectedRetainedObjects      int
	unmatchedExcludes              []string
}

type objectGroup struct {
	path         string
	typeName     string
	observations int
	addresses    map[uintptr]struct{}
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
	retainedAddresses := make(map[uintptr]struct{})
	expectedAddresses := make(map[uintptr]struct{})
	unexpectedAddresses := make(map[uintptr]struct{})

	// Classify each retained object and fold equivalent normalized paths into
	// a single report row.
	for _, obj := range objects {
		if obj.collected.Load() {
			continue
		}

		excludedBy := activeExclusions.match(obj)
		report.totalRetainedObservations++
		retainedAddresses[obj.addr] = struct{}{}
		expected := len(excludedBy) > 0
		if expected {
			report.expectedRetainedObservations++
			expectedAddresses[obj.addr] = struct{}{}
		} else {
			report.unexpectedRetainedObservations++
			unexpectedAddresses[obj.addr] = struct{}{}
		}

		key := groupKey{
			path:     obj.path.normalized(),
			typeName: obj.typeName,
			expected: expected,
		}
		group := groupByKey[key]
		if group == nil {
			group = &objectGroup{
				path:      key.path,
				typeName:  key.typeName,
				addresses: make(map[uintptr]struct{}),
			}
			groupByKey[key] = group
		}
		group.observations++
		group.addresses[obj.addr] = struct{}{}
	}
	report.totalRetainedObjects = len(retainedAddresses)
	report.expectedRetainedObjects = len(expectedAddresses)
	report.unexpectedRetainedObjects = len(unexpectedAddresses)

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
			if c := cmp.Compare(b.objectCount(), a.objectCount()); c != 0 {
				return c
			}
			if c := cmp.Compare(b.observations, a.observations); c != 0 {
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
		failures = append(failures, fmt.Errorf("retained %s at %s", formatCount(group.objectCount(), "object"), group.name()))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object exclusion %q did not match any object", pattern))
	}
	return errors.Join(failures...)
}

func (r report) totals() [3]int {
	return [3]int{
		r.totalRetainedObjects,
		r.unexpectedRetainedObjects,
		len(r.unmatchedExcludes),
	}
}

func (r report) string() string {
	var out strings.Builder
	r.writeSummary(&out)

	writeGroups := func(title string, groups []objectGroup) {
		fmt.Fprintf(&out, "%s:\n", title)
		if len(groups) == 0 {
			out.WriteString("  none\n")
			return
		}
		for _, group := range groups {
			fmt.Fprintf(&out, "  %s: %s\n", group.counts(), group.name())
		}
	}
	out.WriteByte('\n')
	writeGroups("unexpected retained objects", r.unexpectedObjects)
	out.WriteByte('\n')
	writeGroups("expected retained objects", r.expectedObjects)

	if len(r.unmatchedExcludes) > 0 {
		out.WriteString("\nstale exclusions:\n")
	}
	for _, pattern := range r.unmatchedExcludes {
		fmt.Fprintf(&out, "  %s\n", pattern)
	}
	return strings.TrimSuffix(out.String(), "\n")
}

func (r report) writeSummary(out *strings.Builder) {
	out.WriteString("object leak report\n\n")
	fmt.Fprintf(out, "tracked root objects: %d\n", r.trackedRoots)
	fmt.Fprintf(
		out,
		"retained observations: %d total, %d expected, %d unexpected\n",
		r.totalRetainedObservations,
		r.expectedRetainedObservations,
		r.unexpectedRetainedObservations,
	)
	fmt.Fprintf(
		out,
		"retained objects: %d total, %d expected, %d unexpected\n",
		r.totalRetainedObjects,
		r.expectedRetainedObjects,
		r.unexpectedRetainedObjects,
	)
	fmt.Fprintf(out, "stale exclusions: %d", len(r.unmatchedExcludes))
}

func (g objectGroup) name() string {
	if g.path == "" {
		return g.typeName
	}
	return fmt.Sprintf("%s (%s)", g.path, g.typeName)
}

func (g objectGroup) objectCount() int {
	return len(g.addresses)
}

func (g objectGroup) counts() string {
	objects := g.objectCount()
	if g.observations == objects {
		return formatCount(objects, "object")
	}
	return fmt.Sprintf("%s, %s", formatCount(g.observations, "observation"), formatCount(objects, "object"))
}

func formatCount(count int, label string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", label)
	}
	return fmt.Sprintf("%d %ss", count, label)
}
