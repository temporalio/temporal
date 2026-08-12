// Package protocol compiles Temporal's monitored entities and executable actions into one
// validated protocol catalog.
package protocol

import (
	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

// Declaration is the authoring form of a protocol.
type Declaration struct {
	Facts      []umpire.Fact
	Entities   []EntityDeclaration
	Regression *coreregress.Domain
}

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
	regression  *coreregress.Domain
}
