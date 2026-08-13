package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectKeepsOnlySelectedModuleClosure(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:    "feature-workflow",
		Owners:  []CapabilityOwner{"workflow"},
		Modules: []string{"workflow"},
	}}

	model, report, err := Project(family, "feature-workflow")
	require.NoError(t, err)
	require.Equal(t, []string{"Workflow"}, entityNames(model.Entities))
	require.Empty(t, model.Relations)
	require.Empty(t, model.Actions)
	require.Empty(t, model.Properties)
	require.Equal(t, []string{"schedule"}, report.OmittedActions)
}

func TestProjectRetainsImportedProviderActionAsEnvironment(t *testing.T) {
	family := validModelFamily()
	family.Modules[0].Imports = []ObligationRef{{Interface: "delivery", Obligation: "accepted"}}
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"workflow"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}
	family.Targets = []VerificationTarget{
		{
			Name:    "feature-workflow",
			Owners:  []CapabilityOwner{"workflow"},
			Modules: []string{"workflow"},
		},
		{
			Name:    "feature-nexus",
			Owners:  []CapabilityOwner{"nexus"},
			Modules: []string{"nexus"},
		},
	}

	model, report, err := Project(family, "feature-workflow")
	require.NoError(t, err)
	require.Len(t, model.Actions, 1)
	require.Equal(t, "schedule", model.Actions[0].Name)
	require.True(t, model.Actions[0].Unrealized)
	require.Equal(t, []string{"schedule"}, report.EnvironmentActions)
	require.Empty(t, report.OmittedActions)
}

func TestProjectClosesTransitiveContractAssumptions(t *testing.T) {
	family := validModelFamily()
	ready := family.Model.Actions[0]
	ready.Name = "ready"
	family.Model.Actions = append(family.Model.Actions, ready)
	family.Modules[1].Actions = append(family.Modules[1].Actions, "ready")
	family.Modules[0].Imports = []ObligationRef{{Interface: "delivery", Obligation: "accepted"}}
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"workflow"},
		Obligations: []Obligation{
			{
				Name:        "accepted",
				Actions:     []string{"schedule"},
				Assumptions: []ObligationRef{{Interface: "delivery", Obligation: "ready"}},
			},
			{Name: "ready", Actions: []string{"ready"}},
		},
	}}
	family.Targets = []VerificationTarget{
		{
			Name:    "feature-workflow",
			Owners:  []CapabilityOwner{"workflow"},
			Modules: []string{"workflow"},
		},
		{
			Name:    "feature-nexus",
			Owners:  []CapabilityOwner{"nexus"},
			Modules: []string{"nexus"},
		},
	}

	model, report, err := Project(family, "feature-workflow")
	require.NoError(t, err)
	require.Equal(t, []string{"schedule", "ready"}, actionNames(model.Actions))
	require.ElementsMatch(t, []string{"schedule", "ready"}, report.EnvironmentActions)
}

func TestProjectRejectsOmittedActionWhichMutatesRetainedState(t *testing.T) {
	family := validModelFamily()
	family.Model.Actions[0].Effects = append(family.Model.Actions[0].Effects, Effect{
		Kind: SetStateEffect, Entity: "Workflow", Ref: "caller", State: "started",
	})
	family.Targets = []VerificationTarget{{
		Name:    "feature-workflow",
		Owners:  []CapabilityOwner{"workflow"},
		Modules: []string{"workflow"},
	}}

	_, _, err := Project(family, "feature-workflow")
	require.ErrorContains(t, err, `verification target "feature-workflow" omits action "schedule" which can affect retained state`)
}

func TestProjectReportsExplicitStutteringAction(t *testing.T) {
	family := validModelFamily()
	family.RefinementMaps = []RefinementMap{{
		Name:  "workflow-delivery",
		Owner: "workflow",
		Actions: []ActionRefinement{{
			Concrete: "schedule",
			Stutter:  true,
		}},
	}}
	family.Targets = []VerificationTarget{{
		Name:           "feature-workflow",
		Owners:         []CapabilityOwner{"workflow"},
		Modules:        []string{"workflow"},
		RefinementMaps: []string{"workflow-delivery"},
	}}

	model, report, err := Project(family, "feature-workflow")
	require.NoError(t, err)
	require.Empty(t, model.Actions)
	require.Equal(t, []string{"schedule"}, report.StutteringActions)
	require.Empty(t, report.OmittedActions)
}

func TestProjectRejectsStutteringActionWhichMutatesRetainedState(t *testing.T) {
	family := validModelFamily()
	family.Model.Actions[0].Effects = append(family.Model.Actions[0].Effects, Effect{
		Kind: SetStateEffect, Entity: "Workflow", Ref: "caller", State: "started",
	})
	family.RefinementMaps = []RefinementMap{{
		Name:  "workflow-delivery",
		Owner: "workflow",
		Actions: []ActionRefinement{{
			Concrete: "schedule",
			Stutter:  true,
		}},
	}}
	family.Targets = []VerificationTarget{{
		Name:           "feature-workflow",
		Owners:         []CapabilityOwner{"workflow"},
		Modules:        []string{"workflow"},
		RefinementMaps: []string{"workflow-delivery"},
	}}

	_, _, err := Project(family, "feature-workflow")
	require.ErrorContains(t, err, `verification target "feature-workflow" omits action "schedule" which can affect retained state`)
}

func TestProjectRejectsVacuousTargetBound(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:          "feature-workflow",
		Owners:        []CapabilityOwner{"workflow"},
		Modules:       []string{"workflow"},
		MinimumBounds: map[string]int{"Workflow": 2},
	}}

	_, _, err := Project(family, "feature-workflow")
	require.ErrorContains(t, err, `verification target "feature-workflow" requires at least 2 identities for entity "Workflow", got 1`)
}

func TestProjectAppliesIndependentTargetBounds(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:          "feature-nexus",
		Owners:        []CapabilityOwner{"nexus"},
		Modules:       []string{"nexus"},
		Bounds:        map[string]int{"NexusOperation": 1},
		MinimumBounds: map[string]int{"NexusOperation": 1},
	}}

	model, _, err := Project(family, "feature-nexus")
	require.NoError(t, err)
	var operationIDs []string
	for _, entity := range model.Entities {
		if entity.Name == "NexusOperation" {
			operationIDs = entity.IDs
		}
	}
	require.Equal(t, []string{"operation-0"}, operationIDs)
}

func TestProjectRejectsTargetBoundLargerThanSourcePool(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:    "feature-nexus",
		Owners:  []CapabilityOwner{"nexus"},
		Modules: []string{"nexus"},
		Bounds:  map[string]int{"NexusOperation": 3},
	}}

	_, _, err := Project(family, "feature-nexus")
	require.ErrorContains(t, err, `verification target "feature-nexus" requires 3 identities for entity "NexusOperation", source model provides 2`)
}

func TestProjectExpandsSelectedComposition(t *testing.T) {
	family := validModelFamily()
	family.Compositions = []Composition{{
		Name:    "nexus-workflow",
		Owners:  []CapabilityOwner{"nexus", "workflow"},
		Modules: []string{"nexus", "workflow"},
	}}
	family.Targets = []VerificationTarget{{
		Name:         "integration-nexus-workflow",
		Owners:       []CapabilityOwner{"nexus", "workflow"},
		Compositions: []string{"nexus-workflow"},
	}}

	model, report, err := Project(family, "integration-nexus-workflow")
	require.NoError(t, err)
	require.Equal(t, []string{"schedule"}, actionNames(model.Actions))
	require.Equal(t, []string{"schedule"}, report.RetainedActions)
}
