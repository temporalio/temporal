package assurance

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/umpire"
)

func TestDefaultCatalogProjectsRuntimeInventoryAndCoverage(t *testing.T) {
	catalog, err := Default()
	require.NoError(t, err)

	registry := umpire.NewRuleRegistry()
	require.NoError(t, catalog.Register(registry))
	require.NoError(t, registry.InitRules(umpire.NewModelState(), log.NewNoopLogger(), umpire.RuleConfig{}, catalog.Names()...))

	runtimeKinds := make(map[string]string)
	for _, stats := range registry.Stats() {
		runtimeKinds[stats.Name] = stats.Kind
	}
	require.Equal(t, map[string]string{
		"CallbackReferenceConsistencyRule":   "safety",
		"CallbackResponseConsistencyRule":    "safety",
		"EntityProgressRule":                 "liveness",
		"NexusActivityLinkConsistencyRule":   "safety",
		"NexusOperationClosureRule":          "safety",
		"NexusOperationTimeoutSemanticsRule": "safety",
		"SpeculativeTaskCreationRule":        "safety",
		"WorkflowTaskStarvationRule":         "liveness",
	}, runtimeKinds)

	included := make(map[string]bool)
	for _, item := range catalog.VerificationInventory() {
		require.Equal(t, "rule", item.Kind)
		included[item.Name] = item.Included
	}
	require.Equal(t, map[string]bool{
		"CallbackReferenceConsistencyRule":   false,
		"CallbackResponseConsistencyRule":    false,
		"EntityProgressRule":                 true,
		"NexusActivityLinkConsistencyRule":   true,
		"NexusOperationClosureRule":          false,
		"NexusOperationTimeoutSemanticsRule": false,
		"SpeculativeTaskCreationRule":        false,
		"WorkflowTaskStarvationRule":         false,
	}, included)

	coverageNames := make([]string, 0)
	for _, point := range catalog.CoveragePoints() {
		require.Equal(t, umpire.CoverageRuleEvaluated, point.Kind)
		coverageNames = append(coverageNames, point.ID)
	}
	require.Equal(t, catalog.Names(), coverageNames)
}

func TestCatalogSnapshotsAreDefensive(t *testing.T) {
	catalog, err := Default()
	require.NoError(t, err)

	names := catalog.Names()
	inventory := catalog.VerificationInventory()
	coverage := catalog.CoveragePoints()
	names[0] = "changed"
	inventory[0].Name = "changed"
	coverage[0].ID = "changed"

	require.NotEqual(t, names, catalog.Names())
	require.NotEqual(t, inventory, catalog.VerificationInventory())
	require.NotEqual(t, coverage, catalog.CoveragePoints())
}

func TestCompileRejectsDuplicateRulesAndInvalidMigrationMetadata(t *testing.T) {
	factory := func() umpire.SafetyRule { return &catalogSafety{} }

	_, err := compile([]declaration{{safety: factory, included: true}, {safety: factory, included: true}})
	require.ErrorContains(t, err, `duplicate assurance rule "catalogSafetyRule"`)

	_, err = compile([]declaration{{safety: factory}})
	require.ErrorContains(t, err, `excluded assurance rule "catalogSafetyRule" requires a reason`)
}

type catalogSafety struct{}

func (*catalogSafety) Name() string                      { return "catalogSafetyRule" }
func (*catalogSafety) CheckSafety(*umpire.SafetyContext) {}
