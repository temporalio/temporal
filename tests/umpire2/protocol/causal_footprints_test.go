package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpirefw "go.temporal.io/server/common/testing/umpire"
)

func TestDefaultCausalFootprintsAreValidatedAndDefensive(t *testing.T) {
	first, err := DefaultCausalFootprints()
	require.NoError(t, err)
	require.Equal(t, []string{
		"ordinary-completion",
		"completion-before-start",
		"cancellation-failure-then-cancellation",
		"shared-handler-attachment",
	}, footprintNames(first))
	first[0].Footprint.Refinement.Required[0].Name = "mutated"

	second, err := DefaultCausalFootprints()
	require.NoError(t, err)
	require.Equal(t, "NexusOperationTerminal", second[0].Footprint.Refinement.Required[0].Name)
}

func TestCompileCausalFootprintsRejectsDuplicateActionsAndUnknownPatterns(t *testing.T) {
	valid := NamedCausalFootprint{
		Name:      "one",
		Footprint: factFootprint("action", "NexusOperationTerminal"),
	}

	_, err := CompileCausalFootprints([]string{"action"}, []NamedCausalFootprint{valid, {Name: "two", Footprint: valid.Footprint}})
	require.ErrorContains(t, err, "duplicate action")

	unknown := valid
	unknown.Footprint.Refinement.Required = []umpirefw.TracePattern{{Kind: umpirefw.TraceFact, Name: "UnknownFact"}}
	_, err = CompileCausalFootprints([]string{"action"}, []NamedCausalFootprint{unknown})
	require.ErrorContains(t, err, "unknown pattern")
}

func footprintNames(footprints []NamedCausalFootprint) []string {
	names := make([]string, len(footprints))
	for index, footprint := range footprints {
		names[index] = footprint.Name
	}
	return names
}
