package workflowregistry

import (
	"errors"

	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

// ErrEventNotCherryPickable should be returned by CherryPick if an event should not be cherry picked for whatever reason.
var ErrEventNotCherryPickable = errors.New("event not cherry pickable")

// EventDefinition is an alias for the workflow package's EventDefinition interface.
type EventDefinition = chasmworkflow.EventDefinition
