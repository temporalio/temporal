package umpire

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"text/tabwriter"
)

type coveragePointKey struct {
	rule  string
	point string
}

type coverageObservationKey struct {
	runID int64
	rule  string
	point string
	key   string
}

type coverageCounts struct {
	reached  int
	verified int
}

type coverageCollector struct {
	mu        sync.Mutex
	nextRunID int64
	points    map[coveragePointKey]CoveragePoint
	counts    map[coveragePointKey]*coverageCounts
	reached   map[coverageObservationKey]struct{}
	verified  map[coverageObservationKey]struct{}
}

func newCoverageCollector() *coverageCollector {
	return &coverageCollector{
		points:   make(map[coveragePointKey]CoveragePoint),
		counts:   make(map[coveragePointKey]*coverageCounts),
		reached:  make(map[coverageObservationKey]struct{}),
		verified: make(map[coverageObservationKey]struct{}),
	}
}

func (c *coverageCollector) startRun() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextRunID++
	return c.nextRunID
}

func (c *coverageCollector) declare(rule string, points []CoveragePoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, point := range points {
		c.declareLocked(rule, point)
	}
}

func (c *coverageCollector) declareLocked(rule string, point CoveragePoint) {
	if point.Name == "" {
		return
	}
	key := coveragePointKey{rule: rule, point: point.Name}
	if point.MinVerified < 0 {
		point.MinVerified = 0
	}
	c.points[key] = point
	if _, ok := c.counts[key]; !ok {
		c.counts[key] = &coverageCounts{}
	}
}

func (c *coverageCollector) markReached(runID int64, rule string, point string, key any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markReachedLocked(runID, rule, point, coverageInstanceKey(key))
}

func (c *coverageCollector) markVerified(runID int64, rule string, point string, key any) {
	instanceKey := coverageInstanceKey(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markReachedLocked(runID, rule, point, instanceKey)

	obs := coverageObservationKey{
		runID: runID,
		rule:  rule,
		point: point,
		key:   instanceKey,
	}
	if _, ok := c.verified[obs]; ok {
		return
	}
	c.verified[obs] = struct{}{}
	c.countsForLocked(rule, point).verified++
}

func (c *coverageCollector) markReachedLocked(runID int64, rule string, point string, instanceKey string) {
	obs := coverageObservationKey{
		runID: runID,
		rule:  rule,
		point: point,
		key:   instanceKey,
	}
	if _, ok := c.reached[obs]; ok {
		return
	}
	c.reached[obs] = struct{}{}
	c.countsForLocked(rule, point).reached++
}

func (c *coverageCollector) countsForLocked(rule string, point string) *coverageCounts {
	key := coveragePointKey{rule: rule, point: point}
	counts, ok := c.counts[key]
	if ok {
		return counts
	}
	c.points[key] = CoveragePoint{Name: point}
	counts = &coverageCounts{}
	c.counts[key] = counts
	return counts
}

func (c *coverageCollector) summary() []CoverageSummaryLine {
	c.mu.Lock()
	defer c.mu.Unlock()

	lines := make([]CoverageSummaryLine, 0, len(c.counts))
	for key, counts := range c.counts {
		point := c.points[key]
		lines = append(lines, CoverageSummaryLine{
			Rule:        key.rule,
			Point:       key.point,
			Description: point.Description,
			MinVerified: point.MinVerified,
			Reached:     counts.reached,
			Verified:    counts.verified,
		})
	}
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].Rule != lines[j].Rule {
			return lines[i].Rule < lines[j].Rule
		}
		return lines[i].Point < lines[j].Point
	})
	return lines
}

func coverageInstanceKey(key any) string {
	return fmt.Sprintf("%#v", key)
}

func formatCoverageSummary(lines []CoverageSummaryLine) string {
	if len(lines) == 0 {
		return ""
	}

	var buf bytes.Buffer
	writer := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(writer, "umpire coverage:")
	_, _ = fmt.Fprintln(writer, "rule\tpoint\treached\tverified\tmin\tstatus\tdescription")
	for _, line := range lines {
		status := "ok"
		minVerified := line.MinVerified
		if minVerified > 0 {
			if line.Verified < minVerified {
				status = "missing"
			}
		}
		_, _ = fmt.Fprintf(
			writer,
			"%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
			line.Rule,
			line.Point,
			line.Reached,
			line.Verified,
			minVerified,
			status,
			line.Description,
		)
	}
	_ = writer.Flush()
	return buf.String()
}
