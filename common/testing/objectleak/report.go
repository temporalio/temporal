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
	retained               [retentionClassCount]retentionStats
	trackedRoots           int
	totalRetainedObjects   int
	baselineSettleTimedOut bool
	checkSettleTimedOut    bool
	unmatchedExpected      []string
	unmatchedPrunes        []string
}

func newReport(
	objects []trackedObject,
	baseline Baseline,
	checkSettleTimedOut bool,
	trackedRoots int,
	expected patterns,
	pruneTypes patterns,
) report {
	r := report{
		trackedRoots:           trackedRoots,
		baselineSettleTimedOut: baseline.settleTimedOut,
		checkSettleTimedOut:    checkSettleTimedOut,
	}

	// Matching mutates pattern.matched for stale-expected pattern detection.
	activeExpected := slices.Clone(expected)

	var retained [retentionClassCount]retentionAccumulator
	retainedAddresses := make(map[uintptr]struct{})

	// Record each classified retained object and fold equivalent normalized
	// path and type pairs into a single report row.
	addRetained := func(obj trackedObject, path string, class retentionClass) {
		retainedAddresses[obj.addr] = struct{}{}
		retained[class].add(obj, path)
	}
	for obj := range retainedObjects(baseline.objects) {
		path := obj.path.normalized()
		addRetained(obj, path, retentionBaseline)
	}
	for obj := range retainedObjects(objects) {
		path := obj.path.normalized()
		class := classifyRetention(obj, baseline, activeExpected)
		if class != retentionBaseline {
			addRetained(obj, path, class)
		}
	}
	r.totalRetainedObjects = len(retainedAddresses)

	// Expected patterns that never matched a non-baseline retained object are
	// stale and should be removed with the fix that made them unnecessary.
	r.unmatchedExpected = activeExpected.unmatched()
	r.unmatchedPrunes = pruneTypes.unmatched()

	// Keep report output stable across map iteration order and repeated runs.
	for class := range retentionClassCount {
		r.retained[class] = retained[class].finalize()
	}
	slices.Sort(r.unmatchedExpected)
	slices.Sort(r.unmatchedPrunes)
	return r
}

func classifyRetention(obj trackedObject, baseline Baseline, expected patterns) retentionClass {
	if baseline.contains(obj) {
		return retentionBaseline
	}
	if expected.matchObject(obj.path.normalized(), obj.typeName) {
		return retentionExpected
	}
	return retentionUnexpected
}

func (r report) failures() error {
	var failures []error
	if r.baselineSettleTimedOut {
		failures = append(failures, errors.New("baseline GC settling timed out"))
	}
	if r.checkSettleTimedOut {
		failures = append(failures, errors.New("check GC settling timed out"))
	}
	if r.retained[retentionUnexpected].paths > 0 {
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
	if r.baselineSettleTimedOut {
		out.WriteString("baseline GC settling timed out\n")
	}
	if r.checkSettleTimedOut {
		out.WriteString("check GC settling timed out\n")
	}
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
		baseline.objects,
		expected.objects,
		unexpected.objects,
	)
}

type objectGroupKey struct {
	path     string
	typeName string
}

type retentionStats struct {
	groups  []objectGroup
	paths   int
	objects int
}

type retentionAccumulator struct {
	groups    map[objectGroupKey]*objectGroupAccumulator
	paths     int
	addresses map[uintptr]struct{}
}

type objectGroupAccumulator struct {
	path      string
	typeName  string
	paths     int
	addresses map[uintptr]struct{}
}

func (a *retentionAccumulator) add(obj trackedObject, path string) {
	a.paths++
	if a.addresses == nil {
		a.addresses = make(map[uintptr]struct{})
		a.groups = make(map[objectGroupKey]*objectGroupAccumulator)
	}
	a.addresses[obj.addr] = struct{}{}

	key := objectGroupKey{
		path:     path,
		typeName: obj.typeName,
	}
	group := a.groups[key]
	if group == nil {
		group = &objectGroupAccumulator{
			path:      key.path,
			typeName:  key.typeName,
			addresses: make(map[uintptr]struct{}),
		}
		a.groups[key] = group
	}
	group.paths++
	group.addresses[obj.addr] = struct{}{}
}

func (a retentionAccumulator) finalize() retentionStats {
	stats := retentionStats{
		paths:   a.paths,
		objects: len(a.addresses),
	}
	for _, group := range a.groups {
		stats.groups = append(stats.groups, objectGroup{
			path:     group.path,
			typeName: group.typeName,
			paths:    group.paths,
			objects:  len(group.addresses),
		})
	}
	slices.SortFunc(stats.groups, func(a objectGroup, b objectGroup) int {
		if c := cmp.Compare(b.objects, a.objects); c != 0 {
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
	return stats
}

type objectGroup struct {
	path     string
	typeName string
	paths    int
	objects  int
}

func (g objectGroup) name() string {
	if g.path == "" {
		return g.typeName
	}
	return fmt.Sprintf("%s (%s)", g.path, g.typeName)
}

func (g objectGroup) counts() string {
	if g.paths == g.objects {
		return formatCount(g.objects, "object")
	}
	return fmt.Sprintf("%s, %s", formatCount(g.paths, "path"), formatCount(g.objects, "object"))
}

func formatCount(count int, label string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", label)
	}
	return fmt.Sprintf("%d %ss", count, label)
}
