// Package namespace models the Temporal namespace lifecycle as a property-test
// component: the Entity entity, its status enum, the driver that
// invokes RegisterNamespace / DescribeNamespace, the observer that updates
// the World from observed RPC traffic, and the rules that enforce
// invariants over namespace history.
package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
)

type Status int

const (
	StatusRegistered Status = iota
	StatusDeprecated
	StatusDeleted // terminal — DescribeNamespace returns NotFound
)

// Entity is the entity record stored in World.Namespaces.
type Entity struct {
	Name   string
	Status Status
}

// IsTerminal reports whether s is an absorbing status.
func IsTerminal(s Status) bool {
	return s == StatusDeprecated || s == StatusDeleted
}

// ToProto translates the model status to the corresponding API enum.
func ToProto(s Status) enumspb.NamespaceState {
	switch s {
	case StatusRegistered:
		return enumspb.NAMESPACE_STATE_REGISTERED
	case StatusDeprecated:
		return enumspb.NAMESPACE_STATE_DEPRECATED
	case StatusDeleted:
		return enumspb.NAMESPACE_STATE_DELETED
	default:
		panic("unknown namespace status")
	}
}
