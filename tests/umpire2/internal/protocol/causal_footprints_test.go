package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpirefw "go.temporal.io/server/common/testing/umpire"
)

func TestDefaultCausalFootprintsAreValidatedAndDefensive(t *testing.T) {
	firstProtocol, err := Default()
	require.NoError(t, err)
	first := firstProtocol.CausalFootprints()
	require.Equal(t, []string{
		"ordinary-completion",
		"completion-before-start",
		"cancellation-failure-then-cancellation",
		"shared-handler-attachment",
	}, footprintNames(first))
	first[0].Footprint.Refinement.Required[0].Name = "mutated"

	secondProtocol, err := Default()
	require.NoError(t, err)
	second := secondProtocol.CausalFootprints()
	require.Equal(t, "NexusOperationTerminal", second[0].Footprint.Refinement.Required[0].Name)
}

func TestCompileRejectsInvalidCausalFootprints(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*Declaration)
		wantError string
	}{
		{
			name: "duplicate action",
			mutate: func(declaration *Declaration) {
				duplicate := cloneNamedCausalFootprint(declaration.CausalFootprints[0])
				duplicate.Name = "duplicate"
				declaration.CausalFootprints = append(declaration.CausalFootprints, duplicate)
			},
			wantError: "duplicate action",
		},
		{
			name: "unknown action",
			mutate: func(declaration *Declaration) {
				declaration.CausalFootprints[0].Footprint.Action = "unknown"
			},
			wantError: "unknown action",
		},
		{
			name: "unknown pattern",
			mutate: func(declaration *Declaration) {
				declaration.CausalFootprints[0].Footprint.Refinement.Required = []umpirefw.TracePattern{{Kind: umpirefw.TraceFact, Name: "UnknownFact"}}
			},
			wantError: "unknown pattern",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			declaration := defaultDeclaration()
			test.mutate(&declaration)

			_, err := Compile(declaration)

			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func footprintNames(footprints []NamedCausalFootprint) []string {
	names := make([]string, len(footprints))
	for index, footprint := range footprints {
		names[index] = footprint.Name
	}
	return names
}
