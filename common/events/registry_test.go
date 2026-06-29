package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistryBuildCatalogDetectsDuplicates(t *testing.T) {
	var r registry
	r.register(eventDefinition{name: "dup"})
	r.register(eventDefinition{name: "dup"})

	_, err := r.buildCatalog()
	require.ErrorIs(t, err, errEventAlreadyExists)
}

func TestRegistryBuildCatalogIndexesByName(t *testing.T) {
	var r registry
	r.register(eventDefinition{name: "a"})
	r.register(eventDefinition{name: "b"})

	c, err := r.buildCatalog()
	require.NoError(t, err)
	require.Len(t, c, 2)
	_, ok := c["a"]
	require.True(t, ok)
}

func TestNewEventDefRegistersGlobally(t *testing.T) {
	before := len(globalRegistry.definitions)
	_ = NewEventDef("test_event_unique_name", WithField("f", FieldString))
	require.Len(t, globalRegistry.definitions, before+1)
}
