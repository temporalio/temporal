package umpire

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// CoverageKind identifies one semantic observation category.
type CoverageKind string

const (
	CoverageFact          CoverageKind = "fact"
	CoverageTransition    CoverageKind = "transition"
	CoverageRelation      CoverageKind = "relation"
	CoverageAction        CoverageKind = "action"
	CoverageRuleEvaluated CoverageKind = "rule-evaluated"
	CoverageRuleViolated  CoverageKind = "rule-violated"
)

// CoveragePoint is one stable semantic identifier in a coverage catalog.
type CoveragePoint struct {
	Kind CoverageKind
	ID   string
}

var ErrCoveragePoint = errors.New("invalid coverage point")

// Coverage records an optional declared-versus-observed semantic catalog.
type Coverage struct {
	mu       sync.RWMutex
	enabled  bool
	catalog  map[CoveragePoint]struct{}
	observed map[CoveragePoint]struct{}
}

// NewCoverage validates a catalog and creates an optional collector.
func NewCoverage(enabled bool, catalog ...CoveragePoint) (*Coverage, error) {
	coverage := &Coverage{
		enabled:  enabled,
		catalog:  make(map[CoveragePoint]struct{}, len(catalog)),
		observed: make(map[CoveragePoint]struct{}),
	}
	for _, point := range catalog {
		if err := validateCoveragePoint(point); err != nil {
			return nil, err
		}
		coverage.catalog[point] = struct{}{}
	}
	return coverage, nil
}

func validateCoveragePoint(point CoveragePoint) error {
	if point.ID == "" {
		return fmt.Errorf("%w: identifier is empty", ErrCoveragePoint)
	}
	switch point.Kind {
	case CoverageFact, CoverageTransition, CoverageRelation, CoverageAction, CoverageRuleEvaluated, CoverageRuleViolated:
		return nil
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrCoveragePoint, point.Kind)
	}
}

// Record marks a valid point as observed. Disabled collectors are no-ops.
func (c *Coverage) Record(point CoveragePoint) {
	if c == nil || !c.enabled || validateCoveragePoint(point) != nil {
		return
	}
	c.mu.Lock()
	c.observed[point] = struct{}{}
	c.mu.Unlock()
}

// Snapshot returns observed points in deterministic semantic order.
func (c *Coverage) Snapshot() []CoveragePoint {
	if c == nil || !c.enabled {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return sortedCoveragePoints(c.observed)
}

// Unmet returns declared catalog points that have not been observed.
func (c *Coverage) Unmet() []CoveragePoint {
	if c == nil || !c.enabled {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	unmet := make(map[CoveragePoint]struct{})
	for point := range c.catalog {
		if _, observed := c.observed[point]; !observed {
			unmet[point] = struct{}{}
		}
	}
	return sortedCoveragePoints(unmet)
}

func sortedCoveragePoints(points map[CoveragePoint]struct{}) []CoveragePoint {
	result := make([]CoveragePoint, 0, len(points))
	for point := range points {
		result = append(result, point)
	}
	slices.SortFunc(result, func(left, right CoveragePoint) int {
		if result := cmp.Compare(coverageKindOrder(left.Kind), coverageKindOrder(right.Kind)); result != 0 {
			return result
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return result
}

func coverageKindOrder(kind CoverageKind) int {
	switch kind {
	case CoverageFact:
		return 0
	case CoverageTransition:
		return 1
	case CoverageRelation:
		return 2
	case CoverageAction:
		return 3
	case CoverageRuleEvaluated:
		return 4
	case CoverageRuleViolated:
		return 5
	default:
		return 6
	}
}
