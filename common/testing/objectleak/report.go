package objectleak

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type report struct {
	unexpectedObjects         []objectGroup
	expectedObjects           []objectGroup
	trackedRoots              int
	totalRetainedPaths        int
	totalRetainedObjects      int
	expectedRetainedPaths     int
	expectedRetainedObjects   int
	unexpectedRetainedPaths   int
	unexpectedRetainedObjects int
	unmatchedExpected         []string
	unmatchedPrunes           []string
}

type objectGroup struct {
	path      string
	typeName  string
	paths     int
	addresses map[uintptr]struct{}
}

func newReport(objects []trackedObject, trackedRoots int, expected patterns, pruneTypes patterns) report {
	report := report{
		trackedRoots: trackedRoots,
	}

	// Matching mutates pattern.matched for stale-expected pattern detection.
	activeExpected := slices.Clone(expected)

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

		expectedBy := activeExpected.matchObject(obj)
		report.totalRetainedPaths++
		retainedAddresses[obj.addr] = struct{}{}
		expected := len(expectedBy) > 0
		if expected {
			report.expectedRetainedPaths++
			expectedAddresses[obj.addr] = struct{}{}
		} else {
			report.unexpectedRetainedPaths++
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
		group.paths++
		group.addresses[obj.addr] = struct{}{}
	}
	report.totalRetainedObjects = len(retainedAddresses)
	report.expectedRetainedObjects = len(expectedAddresses)
	report.unexpectedRetainedObjects = len(unexpectedAddresses)

	// Expected patterns that never matched any tracked object are stale and
	// should be removed with the fix that made them unnecessary.
	report.unmatchedExpected = activeExpected.unmatched()
	report.unmatchedPrunes = pruneTypes.unmatched()

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
			if c := cmp.Compare(b.paths, a.paths); c != 0 {
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
	slices.Sort(report.unmatchedExpected)
	slices.Sort(report.unmatchedPrunes)
	return report
}

func (r report) failures() error {
	var failures []error
	if len(r.unexpectedObjects) > 0 {
		failures = append(failures, errors.New("unexpected retained objects"))
	}
	if len(r.unmatchedExpected) > 0 {
		failures = append(failures, errors.New("stale expected patterns"))
	}
	if len(r.unmatchedPrunes) > 0 {
		failures = append(failures, errors.New("stale prunes"))
	}
	return errors.Join(failures...)
}

func (r report) totals() [3]int {
	return [3]int{
		r.totalRetainedObjects,
		r.unexpectedRetainedObjects,
		len(r.unmatchedExpected) + len(r.unmatchedPrunes),
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

	if len(r.unmatchedExpected) > 0 {
		out.WriteString("\nstale expected patterns:\n")
	}
	for _, pattern := range r.unmatchedExpected {
		fmt.Fprintf(&out, "  %s\n", pattern)
	}

	if len(r.unmatchedPrunes) > 0 {
		out.WriteString("\nstale prunes:\n")
	}
	for _, pattern := range r.unmatchedPrunes {
		fmt.Fprintf(&out, "  %s\n", pattern)
	}
	return strings.TrimSuffix(out.String(), "\n")
}

func (r report) writeSummary(out *strings.Builder) {
	out.WriteString("object leak report\n\n")
	fmt.Fprintf(out, "tracked root objects: %d\n", r.trackedRoots)
	fmt.Fprintf(
		out,
		"retained paths: %d total, %d expected, %d unexpected\n",
		r.totalRetainedPaths,
		r.expectedRetainedPaths,
		r.unexpectedRetainedPaths,
	)
	fmt.Fprintf(
		out,
		"retained objects: %d total, %d expected, %d unexpected\n",
		r.totalRetainedObjects,
		r.expectedRetainedObjects,
		r.unexpectedRetainedObjects,
	)
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
	if g.paths == objects {
		return formatCount(objects, "object")
	}
	return fmt.Sprintf("%s, %s", formatCount(g.paths, "path"), formatCount(objects, "object"))
}

func formatCount(count int, label string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", label)
	}
	return fmt.Sprintf("%d %ss", count, label)
}
