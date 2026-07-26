package probe

import (
	"sync"

	umpire "go.temporal.io/server/common/testing/umpire"
)

// Coverage accumulates, per entity type, the model edges that observed entities have
// actually traversed — across executions and namespace purges. It is the run summary:
// which of the model's valid transitions have been exercised, and which are still
// missing. Safe for concurrent use.
type Coverage struct {
	mu      sync.Mutex
	visited map[string]map[string]struct{} // entityType -> edgeKey
}

// NewCoverage returns an empty coverage tracker.
func NewCoverage() *Coverage {
	return &Coverage{visited: map[string]map[string]struct{}{}}
}

// Record adds the edges an entity of entityType traversed to the accumulated coverage.
func (c *Coverage) Record(entityType string, edges []umpire.Edge) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.visited[entityType]
	if m == nil {
		m = map[string]struct{}{}
		c.visited[entityType] = m
	}
	for _, e := range edges {
		m[edgeKey(e)] = struct{}{}
	}
}

// Report scores the accumulated coverage of entityType against total (the model's
// full set of edges, e.g. Lifecycle.Edges()).
func (c *Coverage) Report(entityType string, total []umpire.Edge) CoverageReport {
	c.mu.Lock()
	seen := c.visited[entityType]
	c.mu.Unlock()

	rep := CoverageReport{Entity: entityType, Total: len(total)}
	for _, e := range total {
		_, ok := seen[edgeKey(e)]
		if ok {
			rep.Covered++
		}
		rep.Edges = append(rep.Edges, EdgeCoverage{Edge: e, Covered: ok})
	}
	return rep
}

// EdgeCoverage is one model edge and whether any observed entity traversed it.
type EdgeCoverage struct {
	Edge    umpire.Edge
	Covered bool
}

// CoverageReport is the transition coverage of one entity type.
type CoverageReport struct {
	Entity  string
	Total   int
	Covered int
	Edges   []EdgeCoverage
}

// Missing returns the valid edges that were never exercised.
func (r CoverageReport) Missing() []umpire.Edge {
	var out []umpire.Edge
	for _, ec := range r.Edges {
		if !ec.Covered {
			out = append(out, ec.Edge)
		}
	}
	return out
}

func edgeKey(e umpire.Edge) string { return e.From + "\x00" + e.Event + "\x00" + e.To }
