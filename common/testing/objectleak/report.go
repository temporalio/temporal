package objectleak

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type retentionClass int

const (
	retentionUnexpected retentionClass = iota
	retentionExpected
	retentionBaseline
	retentionClassCount
)

type report struct {
	retained             [retentionClassCount]retentionStats
	trackedRoots         int
	totalRetainedObjects int
	unmatchedExpected    []string
	unmatchedPrunes      []string
}

func newReport(
	objects []trackedObject,
	baseline Baseline,
	trackedRoots int,
	expected patterns,
	pruneTypes patterns,
) report {
	report := report{
		trackedRoots: trackedRoots,
	}

	// Matching mutates pattern.matched for stale-expected pattern detection.
	activeExpected := slices.Clone(expected)

	for class := range retentionClassCount {
		report.retained[class] = newRetentionStats()
	}
	retainedAddresses := make(map[uintptr]struct{})

	// Classify each retained object and fold equivalent normalized paths into
	// a single report row.
	addRetained := func(obj trackedObject, class retentionClass) {
		retainedAddresses[obj.addr] = struct{}{}
		report.retained[class].add(obj)
	}
	for obj := range retainedObjects(baseline.objects) {
		activeExpected.matchObject(obj)
		addRetained(obj, retentionBaseline)
	}
	for obj := range retainedObjects(objects) {
		expectedBy := activeExpected.matchObject(obj)
		if baseline.contains(obj) {
			continue
		}
		if len(expectedBy) > 0 {
			addRetained(obj, retentionExpected)
		} else {
			addRetained(obj, retentionUnexpected)
		}
	}
	report.totalRetainedObjects = len(retainedAddresses)

	// Expected patterns that never matched any tracked object are stale and
	// should be removed with the fix that made them unnecessary.
	report.unmatchedExpected = activeExpected.unmatched()
	report.unmatchedPrunes = pruneTypes.unmatched()

	// Keep report output stable across map iteration order and repeated runs.
	for class := range retentionClassCount {
		report.retained[class].finish()
	}
	slices.Sort(report.unmatchedExpected)
	slices.Sort(report.unmatchedPrunes)
	return report
}

func (r report) failures() error {
	var failures []error
	if len(r.retained[retentionUnexpected].groups) > 0 {
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
	writeGroups("unexpected retained objects", r.retained[retentionUnexpected].groups)
	out.WriteByte('\n')
	writeGroups("expected retained objects", r.retained[retentionExpected].groups)
	out.WriteByte('\n')
	writeGroups("baseline retained objects", r.retained[retentionBaseline].groups)

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
	baseline := r.retained[retentionBaseline]
	expected := r.retained[retentionExpected]
	unexpected := r.retained[retentionUnexpected]
	totalPaths := baseline.paths + expected.paths + unexpected.paths

	out.WriteString("object leak report\n\n")
	fmt.Fprintf(out, "tracked root objects: %d\n", r.trackedRoots)
	fmt.Fprintf(
		out,
		"retained paths: %d total, %d baseline, %d expected, %d unexpected\n",
		totalPaths,
		baseline.paths,
		expected.paths,
		unexpected.paths,
	)
	fmt.Fprintf(
		out,
		"retained objects: %d total, %d baseline, %d expected, %d unexpected\n",
		r.totalRetainedObjects,
		len(baseline.addresses),
		len(expected.addresses),
		len(unexpected.addresses),
	)
}

type objectGroupKey struct {
	path     string
	typeName string
}

type retentionStats struct {
	groups      []objectGroup
	paths       int
	addresses   map[uintptr]struct{}
	groupsByKey map[objectGroupKey]*objectGroup
}

func newRetentionStats() retentionStats {
	return retentionStats{
		addresses:   make(map[uintptr]struct{}),
		groupsByKey: make(map[objectGroupKey]*objectGroup),
	}
}

func (s *retentionStats) add(obj trackedObject) {
	s.paths++
	s.addresses[obj.addr] = struct{}{}

	key := objectGroupKey{
		path:     obj.path.normalized(),
		typeName: obj.typeName,
	}
	group := s.groupsByKey[key]
	if group == nil {
		group = &objectGroup{
			path:      key.path,
			typeName:  key.typeName,
			addresses: make(map[uintptr]struct{}),
		}
		s.groupsByKey[key] = group
	}
	group.paths++
	group.addresses[obj.addr] = struct{}{}
}

func (s *retentionStats) finish() {
	for _, group := range s.groupsByKey {
		s.groups = append(s.groups, *group)
	}
	s.groupsByKey = nil
	slices.SortFunc(s.groups, func(a objectGroup, b objectGroup) int {
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

type objectGroup struct {
	path      string
	typeName  string
	paths     int
	addresses map[uintptr]struct{}
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
