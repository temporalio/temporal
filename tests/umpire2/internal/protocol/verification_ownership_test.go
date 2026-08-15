package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestVerificationOwnershipAssignsCanonicalModel(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	model, err := compiled.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	ownership, err := defaultVerificationOwnership()
	require.NoError(t, err)

	modules, err := ownership.Assign(model)

	require.NoError(t, err)
	require.Equal(t, []string{"workflow", "activity", "matching", "nexus", "callback"}, verificationModuleNames(modules))
	require.Contains(t, modules[1].Properties, "NexusActivityReverseLinkConsistency")
	require.Contains(t, modules[3].Properties, "NexusActivityForwardLinkConsistency")
}

func TestVerificationOwnershipRejectsUnownedDeclaration(t *testing.T) {
	ownership, err := defaultVerificationOwnership()
	require.NoError(t, err)

	_, err = ownership.Assign(verify.Model{Actions: []verify.Action{{Name: "unknown.action"}}})

	require.ErrorContains(t, err, `action "unknown.action" has no capability owner`)
}

func TestVerificationOwnershipRejectsAmbiguousSelectors(t *testing.T) {
	_, err := compileVerificationOwnership([]verificationOwnershipModule{
		{Name: "left", Owner: "left", Entities: []string{"Shared"}},
		{Name: "right", Owner: "right", Entities: []string{"Shared"}},
	})

	require.ErrorContains(t, err, `entity "Shared" is owned by both "left" and "right"`)
}

func verificationModuleNames(modules []verify.Module) []string {
	result := make([]string, len(modules))
	for index, module := range modules {
		result[index] = module.Name
	}
	return result
}
