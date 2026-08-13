package protocol

import (
	"cmp"
	"fmt"
	"slices"

	"go.temporal.io/server/common/testing/umpire"
)

// CoverageCatalogOptions selects protocol declarations for a semantic coverage denominator.
// Empty filters select every supported kind and every declared entity type.
type CoverageCatalogOptions struct {
	EntityTypes []umpire.EntityType
	Kinds       []umpire.CoverageKind
}

// CoverageCatalog derives a deterministic semantic coverage denominator from the compiled protocol.
func (p *Protocol) CoverageCatalog(options CoverageCatalogOptions) ([]umpire.CoveragePoint, error) {
	entities, err := p.coverageEntities(options.EntityTypes)
	if err != nil {
		return nil, err
	}
	kinds, err := coverageKinds(options.Kinds)
	if err != nil {
		return nil, err
	}

	points := make(map[umpire.CoveragePoint]struct{})
	if kinds[umpire.CoverageFact] {
		if len(options.EntityTypes) == 0 {
			for _, declared := range p.facts {
				points[umpire.CoveragePoint{Kind: umpire.CoverageFact, ID: declared.Name()}] = struct{}{}
			}
		} else {
			for entityType := range entities {
				for _, declared := range p.entities[entityType].facts {
					points[umpire.CoveragePoint{Kind: umpire.CoverageFact, ID: declared.Name()}] = struct{}{}
				}
			}
		}
	}
	if kinds[umpire.CoverageTransition] {
		for entityType := range entities {
			lifecycle, ok := p.Lifecycle(entityType)
			if !ok {
				continue
			}
			for _, edge := range lifecycle.Edges() {
				points[umpire.CoveragePoint{Kind: umpire.CoverageTransition, ID: TransitionCoverageID(entityType, edge)}] = struct{}{}
			}
		}
	}
	if kinds[umpire.CoverageRelation] {
		for _, relation := range p.relations {
			if entities[relation.Source] || entities[relation.Target] {
				points[umpire.CoveragePoint{Kind: umpire.CoverageRelation, ID: string(relation.Type)}] = struct{}{}
			}
		}
	}
	if kinds[umpire.CoverageAction] {
		for _, entry := range p.actionOrder {
			if entry.Action != nil && entities[entry.Key.Entity] {
				points[umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: entry.Action.Name}] = struct{}{}
			}
		}
	}

	result := make([]umpire.CoveragePoint, 0, len(points))
	for point := range points {
		result = append(result, point)
	}
	slices.SortFunc(result, compareCoveragePoints)
	return result, nil
}

// NewCoverage creates a collector whose catalog is derived from this protocol.
func (p *Protocol) NewCoverage(enabled bool, options CoverageCatalogOptions) (*umpire.Coverage, error) {
	catalog, err := p.CoverageCatalog(options)
	if err != nil {
		return nil, err
	}
	return umpire.NewCoverage(enabled, catalog...)
}

// TransitionCoverageID formats a lifecycle edge exactly as the v2 monitor records it.
func TransitionCoverageID(entityType umpire.EntityType, edge umpire.Edge) string {
	return fmt.Sprintf("%s:%s/%s/%s", entityType, edge.From, edge.Event, edge.To)
}

func (p *Protocol) coverageEntities(filter []umpire.EntityType) (map[umpire.EntityType]bool, error) {
	selected := make(map[umpire.EntityType]bool, len(p.entities))
	if len(filter) == 0 {
		for entityType := range p.entities {
			selected[entityType] = true
		}
		return selected, nil
	}
	for _, entityType := range filter {
		if _, exists := p.entities[entityType]; !exists {
			return nil, fmt.Errorf("protocol: coverage references unknown entity %q", entityType)
		}
		selected[entityType] = true
	}
	return selected, nil
}

func coverageKinds(filter []umpire.CoverageKind) (map[umpire.CoverageKind]bool, error) {
	selected := make(map[umpire.CoverageKind]bool, 4)
	if len(filter) == 0 {
		selected[umpire.CoverageFact] = true
		selected[umpire.CoverageTransition] = true
		selected[umpire.CoverageRelation] = true
		selected[umpire.CoverageAction] = true
		return selected, nil
	}
	for _, kind := range filter {
		switch kind {
		case umpire.CoverageFact, umpire.CoverageTransition, umpire.CoverageRelation, umpire.CoverageAction:
			selected[kind] = true
		default:
			return nil, fmt.Errorf("protocol: unsupported coverage kind %q", kind)
		}
	}
	return selected, nil
}

func compareCoveragePoints(left, right umpire.CoveragePoint) int {
	if result := cmp.Compare(coverageKindOrder(left.Kind), coverageKindOrder(right.Kind)); result != 0 {
		return result
	}
	return cmp.Compare(left.ID, right.ID)
}

func coverageKindOrder(kind umpire.CoverageKind) int {
	switch kind {
	case umpire.CoverageFact:
		return 0
	case umpire.CoverageTransition:
		return 1
	case umpire.CoverageRelation:
		return 2
	case umpire.CoverageAction:
		return 3
	default:
		return 4
	}
}
