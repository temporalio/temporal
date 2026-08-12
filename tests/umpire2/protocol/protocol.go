// Package protocol compiles Temporal's monitored entities and executable actions into one
// validated protocol catalog.
package protocol

import (
	"errors"
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

// Declaration is the authoring form of a protocol.
type Declaration struct {
	Facts            []umpire.Fact
	Entities         []EntityDeclaration
	Relations        []umpire.RelationSchema
	RelationDerivers []RelationDeriver
	Regression       *coreregress.Domain
}

// RelationMutation is one fact-derived change to runtime relation state.
type RelationMutation struct {
	Edge   umpire.RelationEdge
	Remove bool
}

// RelationDeriver translates one observed fact into zero or more relation mutations.
type RelationDeriver func(umpire.Fact) []RelationMutation

// EntityDeclaration associates an entity factory with its fact subscriptions and actions.
type EntityDeclaration struct {
	Type       umpire.EntityType
	New        umpire.EntityFactory
	Facts      []umpire.Fact
	Actions    []ActionBinding
	ActionGaps []ActionGap
}

// ActionKey identifies the action that realizes one lifecycle edge under one hosting.
type ActionKey struct {
	Entity  umpire.EntityType
	From    string
	Event   string
	Hosting umpire.Hosting
}

// ActionBinding associates an exact lifecycle edge with its executable action.
type ActionBinding struct {
	Key    ActionKey
	Action umpire.Action
}

// ActionGap records a lifecycle edge that deliberately has no atomic realization.
type ActionGap struct {
	Key    ActionKey
	Reason string
}

type compiledEntity struct {
	new   umpire.EntityFactory
	facts []umpire.Fact
}

// Protocol is an immutable compiled declaration.
type Protocol struct {
	facts       []umpire.Fact
	entityOrder []umpire.EntityType
	entities    map[umpire.EntityType]compiledEntity
	actions     map[ActionKey]umpire.Action
	gaps        map[ActionKey]string
	relations   []umpire.RelationSchema
	derivers    []RelationDeriver
	regression  *coreregress.Domain
}

// RelationSchemas returns a defensive copy of the protocol's relation declarations.
func (p *Protocol) RelationSchemas() []umpire.RelationSchema {
	return append([]umpire.RelationSchema(nil), p.relations...)
}

// NewRelationStore creates empty runtime relation state for this protocol.
func (p *Protocol) NewRelationStore() (*umpire.RelationStore, error) {
	return umpire.NewRelationStore(p.relations...)
}

// ApplyRelations derives and atomically applies relation mutations from observed facts.
func (p *Protocol) ApplyRelations(store *umpire.RelationStore, facts []umpire.Fact) []error {
	if store == nil {
		return []error{errors.New("protocol: relation store is nil")}
	}
	var errs []error
	for _, observed := range facts {
		for _, derive := range p.derivers {
			for _, mutation := range derive(observed) {
				var err error
				if mutation.Remove {
					_, err = store.Remove(mutation.Edge)
				} else {
					_, err = store.Add(mutation.Edge)
				}
				if err != nil {
					errs = append(errs, fmt.Errorf("protocol: derive relation from %s: %w", observed.Name(), err))
				}
			}
		}
	}
	return errs
}
