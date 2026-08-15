package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateModelFamilyRejectsModuleWithoutOwner(t *testing.T) {
	family := ModelFamily{
		Version: "test-family/v1",
		Model:   nexusModel(),
		Modules: []Module{{Name: "workflow", Actions: []string{"schedule"}}},
	}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `module "workflow" has no owner`)
}

func TestValidateModelFamilyRejectsUnknownImportedObligation(t *testing.T) {
	family := validModelFamily()
	family.Modules[0].Imports = []ObligationRef{{Interface: "delivery", Obligation: "missing"}}
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"workflow"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `module "workflow" imports unknown obligation "delivery.missing"`)
}

func TestValidateModelFamilyRejectsContractCycle(t *testing.T) {
	family := validModelFamily()
	family.Model.Properties = []Property{{Name: "workflow-valid", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}}
	family.Modules[0].Properties = []string{"workflow-valid"}
	family.Interfaces = []Interface{
		{
			Name:      "delivery",
			Provider:  "nexus",
			Consumers: []string{"workflow"},
			Obligations: []Obligation{{
				Name:        "accepted",
				Actions:     []string{"schedule"},
				Assumptions: []ObligationRef{{Interface: "feature", Obligation: "valid"}},
			}},
		},
		{
			Name:      "feature",
			Provider:  "workflow",
			Consumers: []string{"nexus"},
			Obligations: []Obligation{{
				Name:        "valid",
				Properties:  []string{"workflow-valid"},
				Assumptions: []ObligationRef{{Interface: "delivery", Obligation: "accepted"}},
			}},
		},
	}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `contract cycle: delivery.accepted -> feature.valid -> delivery.accepted`)
}

func TestValidateModelFamilyAcceptsContractCycleClosedByComposition(t *testing.T) {
	family := validModelFamily()
	family.Model.Properties = []Property{{Name: "workflow-valid", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}}
	family.Modules[0].Properties = []string{"workflow-valid"}
	family.Interfaces = []Interface{
		{
			Name:      "delivery",
			Provider:  "nexus",
			Consumers: []string{"workflow"},
			Obligations: []Obligation{{
				Name:        "accepted",
				Actions:     []string{"schedule"},
				Assumptions: []ObligationRef{{Interface: "feature", Obligation: "valid"}},
			}},
		},
		{
			Name:      "feature",
			Provider:  "workflow",
			Consumers: []string{"nexus"},
			Obligations: []Obligation{{
				Name:        "valid",
				Properties:  []string{"workflow-valid"},
				Assumptions: []ObligationRef{{Interface: "delivery", Obligation: "accepted"}},
			}},
		},
	}
	family.Compositions = []Composition{{
		Name:    "workflow-delivery",
		Owners:  []CapabilityOwner{"workflow", "nexus"},
		Modules: []string{"workflow", "nexus"},
		Closes: []ObligationRef{
			{Interface: "delivery", Obligation: "accepted"},
			{Interface: "feature", Obligation: "valid"},
		},
	}}
	family.Targets = []VerificationTarget{{
		Name:         "integration-workflow-delivery",
		Owners:       []CapabilityOwner{"workflow", "nexus"},
		Compositions: []string{"workflow-delivery"},
	}}

	require.NoError(t, ValidateModelFamily(family))
}

func TestValidateModelFamilyRejectsUnclosedCycleAfterClosedCycle(t *testing.T) {
	family := validModelFamily()
	family.Model.Properties = []Property{{Name: "workflow-valid", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}}
	family.Modules[0].Properties = []string{"workflow-valid"}
	family.Interfaces = []Interface{
		{
			Name:      "delivery",
			Provider:  "nexus",
			Consumers: []string{"workflow"},
			Obligations: []Obligation{
				{Name: "accepted-a", Actions: []string{"schedule"}, Assumptions: []ObligationRef{{Interface: "feature", Obligation: "valid-a"}}},
				{Name: "accepted-b", Actions: []string{"schedule"}, Assumptions: []ObligationRef{{Interface: "feature", Obligation: "valid-b"}}},
			},
		},
		{
			Name:      "feature",
			Provider:  "workflow",
			Consumers: []string{"nexus"},
			Obligations: []Obligation{
				{Name: "valid-a", Properties: []string{"workflow-valid"}, Assumptions: []ObligationRef{{Interface: "delivery", Obligation: "accepted-a"}}},
				{Name: "valid-b", Properties: []string{"workflow-valid"}, Assumptions: []ObligationRef{{Interface: "delivery", Obligation: "accepted-b"}}},
			},
		},
	}
	family.Compositions = []Composition{{
		Name:    "workflow-delivery-a",
		Owners:  []CapabilityOwner{"workflow", "nexus"},
		Modules: []string{"workflow", "nexus"},
		Closes: []ObligationRef{
			{Interface: "delivery", Obligation: "accepted-a"},
			{Interface: "feature", Obligation: "valid-a"},
		},
	}}
	family.Targets = []VerificationTarget{{
		Name:         "integration-workflow-delivery-a",
		Owners:       []CapabilityOwner{"workflow", "nexus"},
		Compositions: []string{"workflow-delivery-a"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `contract cycle: delivery.accepted-b -> feature.valid-b -> delivery.accepted-b`)
}

func TestValidateModelFamilyRejectsEmptyObligation(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:        "delivery",
		Provider:    "nexus",
		Consumers:   []string{"workflow"},
		Obligations: []Obligation{{Name: "accepted"}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `obligation "delivery.accepted" has no actions or properties`)
}

func TestValidateModelFamilyRejectsDuplicateGuaranteeAction(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"workflow"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule", "schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `obligation "delivery.accepted" contains action "schedule" more than once`)
}

func TestValidateModelFamilyRequiresProviderGuaranteeCheck(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"workflow"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `obligation "delivery.accepted" has no provider module or closing composition target`)
}

func TestValidateModelFamilyRejectsUnknownClosedObligation(t *testing.T) {
	family := validModelFamily()
	family.Compositions = []Composition{{
		Name:    "workflow-delivery",
		Owners:  []CapabilityOwner{"workflow", "nexus"},
		Modules: []string{"workflow", "nexus"},
		Closes:  []ObligationRef{{Interface: "delivery", Obligation: "missing"}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `composition "workflow-delivery" closes unknown obligation "delivery.missing"`)
}

func TestValidateModelFamilyRejectsUnownedDeclaration(t *testing.T) {
	family := validModelFamily()
	family.Modules[1].Actions = nil

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `action "schedule" is not owned by a module`)
}

func TestValidateModelFamilyRejectsUnknownInterfaceConsumer(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "nexus",
		Consumers: []string{"missing"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `interface "delivery" references unknown consumer module "missing"`)
}

func TestValidateModelFamilyRejectsUnknownInterfaceIdentitySort(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:       "delivery",
		Provider:   "nexus",
		Consumers:  []string{"workflow"},
		Identities: []string{"missing"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `interface "delivery" references unknown identity sort "missing"`)
}

func TestValidateModelFamilyRequiresAssumptionProviderAsConsumer(t *testing.T) {
	family := validModelFamily()
	family.Model.Properties = []Property{{Name: "workflow-valid", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}}
	family.Modules[0].Properties = []string{"workflow-valid"}
	family.Interfaces = []Interface{
		{
			Name:     "delivery",
			Provider: "nexus",
			Obligations: []Obligation{{
				Name:        "accepted",
				Actions:     []string{"schedule"},
				Assumptions: []ObligationRef{{Interface: "feature", Obligation: "valid"}},
			}},
		},
		{
			Name:     "feature",
			Provider: "workflow",
			Obligations: []Obligation{{
				Name:       "valid",
				Properties: []string{"workflow-valid"},
			}},
		},
	}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `obligation "delivery.accepted" assumes "feature.valid" but provider module "nexus" is not a declared consumer of interface "feature"`)
}

func TestValidateModelFamilyRejectsGuaranteeOwnedByAnotherModule(t *testing.T) {
	family := validModelFamily()
	family.Interfaces = []Interface{{
		Name:      "delivery",
		Provider:  "workflow",
		Consumers: []string{"nexus"},
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `obligation "delivery.accepted" action "schedule" is owned by module "nexus", not provider "workflow"`)
}

func TestValidateModelFamilyRejectsImportByUndeclaredConsumer(t *testing.T) {
	family := validModelFamily()
	family.Modules[0].Imports = []ObligationRef{{Interface: "delivery", Obligation: "accepted"}}
	family.Interfaces = []Interface{{
		Name:     "delivery",
		Provider: "nexus",
		Obligations: []Obligation{{
			Name:    "accepted",
			Actions: []string{"schedule"},
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `module "workflow" imports "delivery.accepted" but is not a declared consumer of interface "delivery"`)
}

func TestValidateModelFamilyRejectsOwnerlessComposition(t *testing.T) {
	family := validModelFamily()
	family.Compositions = []Composition{{
		Name:    "nexus-workflow",
		Modules: []string{"nexus", "workflow"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `composition "nexus-workflow" has no owners`)
}

func TestValidateModelFamilyRequiresEveryComposedModuleOwner(t *testing.T) {
	family := validModelFamily()
	family.Compositions = []Composition{{
		Name:    "nexus-workflow",
		Owners:  []CapabilityOwner{"nexus"},
		Modules: []string{"nexus", "workflow"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `composition "nexus-workflow" is missing owner "workflow" for module "workflow"`)
}

func TestValidateModelFamilyRejectsUnknownCompositionProperty(t *testing.T) {
	family := validModelFamily()
	family.Compositions = []Composition{{
		Name:       "nexus-workflow",
		Owners:     []CapabilityOwner{"nexus", "workflow"},
		Modules:    []string{"nexus", "workflow"},
		Properties: []string{"missing"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `composition "nexus-workflow" references unknown property "missing"`)
}

func TestValidateModelFamilyRejectsOwnerlessTarget(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:    "feature-workflow",
		Modules: []string{"workflow"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `verification target "feature-workflow" has no owners`)
}

func TestValidateModelFamilyRequiresEverySelectedModuleOwner(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:    "protocol-atomic",
		Owners:  []CapabilityOwner{"workflow"},
		Modules: []string{"workflow", "nexus"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `verification target "protocol-atomic" is missing owner "nexus" for module "nexus"`)
}

func TestValidateModelFamilyRequiresEverySelectedPropertyOwner(t *testing.T) {
	family := validModelFamily()
	family.Model.Properties = []Property{{Name: "workflow-safe", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}}
	family.Modules[0].Properties = []string{"workflow-safe"}
	family.Targets = []VerificationTarget{{
		Name:       "integration",
		Owners:     []CapabilityOwner{"nexus"},
		Properties: []string{"workflow-safe"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `verification target "integration" is missing owner "workflow" for property "workflow-safe"`)
}

func TestValidateModelFamilyRejectsDuplicateBackendRequirement(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:                "feature-workflow",
		Owners:              []CapabilityOwner{"workflow"},
		Modules:             []string{"workflow"},
		BackendRequirements: []string{"tla", "tla"},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `verification target "feature-workflow" has duplicate backend requirement "tla"`)
}

func TestValidateModelFamilyRejectsInvalidTargetBound(t *testing.T) {
	family := validModelFamily()
	family.Targets = []VerificationTarget{{
		Name:    "feature-workflow",
		Owners:  []CapabilityOwner{"workflow"},
		Modules: []string{"workflow"},
		Bounds:  map[string]int{"Workflow": 0},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `verification target "feature-workflow" bound for entity "Workflow" must be positive`)
}

func TestValidateModelFamilyRejectsAmbiguousActionRefinement(t *testing.T) {
	family := validModelFamily()
	family.RefinementMaps = []RefinementMap{{
		Name:  "workflow-delivery",
		Owner: "workflow",
		Actions: []ActionRefinement{{
			Concrete: "schedule",
			Abstract: "schedule",
			Stutter:  true,
		}},
	}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" action "schedule" must select exactly one abstract action or stuttering`)
}

func TestValidateModelFamilyRequiresCompleteContractRefinement(t *testing.T) {
	family := contractRefinementFamily()
	family.RefinementMaps[0].Actions = nil

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" does not classify concrete action "workflow-dispatch"`)

	family = contractRefinementFamily()
	family.RefinementMaps[0].Actions[0] = ActionRefinement{Concrete: "workflow-dispatch", Stutter: true}
	err = ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" does not refine imported action "schedule"`)
}

func TestValidateModelFamilyAcceptsCompleteContractRefinement(t *testing.T) {
	require.NoError(t, ValidateModelFamily(contractRefinementFamily()))
}

func TestValidateModelFamilyRequiresSharedPropertyForIncludedRule(t *testing.T) {
	family := validModelFamily()
	family.Model.Inventory = []InventoryItem{{Kind: "rule", Name: "deliveryRule", Included: true}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `included verification rule "deliveryRule" has no shared property`)

	family.Model.Properties = []Property{{
		Name: "delivery-safe", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr},
		Source: Provenance{Symbol: "deliveryRule"},
	}}
	family.Modules[1].Properties = []string{"delivery-safe"}
	require.NoError(t, ValidateModelFamily(family))
}

func TestValidateModelFamilyRequiresContractIdentityRefinement(t *testing.T) {
	family := contractRefinementFamily()
	family.Interfaces[0].Identities = []string{"NexusOperation"}
	family.RefinementMaps[0].Identities = []IdentityRefinement{{Concrete: "Workflow", Abstract: "NexusOperation"}}
	require.NoError(t, ValidateModelFamily(family))

	family.RefinementMaps[0].Identities = nil
	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" does not refine imported identity "NexusOperation"`)
}

func TestValidateModelFamilyRejectsRefinementOutsideImportedContract(t *testing.T) {
	family := contractRefinementFamily()
	family.RefinementMaps[0].Actions[0].Abstract = "finish"

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" maps to action "finish" outside imported interface "delivery"`)
}

func TestValidateModelFamilyRejectsIncorrectActionRefinement(t *testing.T) {
	family := contractRefinementFamily()
	family.Model.Actions[2].Effects = family.Model.Actions[2].Effects[:1]

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" concrete action "workflow-dispatch" omits effect 1 of abstract action "schedule"`)

	family = contractRefinementFamily()
	family.Model.Actions[2].Parameters[1].Binding = FreshBinding
	err = ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" concrete action "workflow-dispatch" has incompatible parameter "caller"`)
}

func TestValidateModelFamilyAcceptsRefinementParameterMapping(t *testing.T) {
	family := contractRefinementFamily()
	concrete := &family.Model.Actions[2]
	concrete.Parameters[0].Name = "delivery"
	concrete.Effects[0].Ref = "delivery"
	concrete.Effects[1].Source = "delivery"
	family.RefinementMaps[0].Actions[0].Parameters = []ParameterRefinement{{Concrete: "delivery", Abstract: "operation"}}

	require.NoError(t, ValidateModelFamily(family))
}

func TestValidateModelFamilyRejectsUnknownRefinementParameterMapping(t *testing.T) {
	family := contractRefinementFamily()
	family.RefinementMaps[0].Actions[0].Parameters = []ParameterRefinement{{Concrete: "missing", Abstract: "operation"}}

	err := ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery" action "workflow-dispatch" maps unknown concrete parameter "missing"`)
}

func TestHashModelFamilyIgnoresDeclarationOrder(t *testing.T) {
	left := validModelFamily()
	left.Targets = []VerificationTarget{{
		Name:                "protocol-atomic",
		Owners:              []CapabilityOwner{"workflow", "nexus"},
		Modules:             []string{"workflow", "nexus"},
		BackendRequirements: []string{"tla", "ivy"},
	}}
	right := left
	right.Modules = []Module{left.Modules[1], left.Modules[0]}
	right.Targets = []VerificationTarget{{
		Name:                "protocol-atomic",
		Owners:              []CapabilityOwner{"nexus", "workflow"},
		Modules:             []string{"nexus", "workflow"},
		BackendRequirements: []string{"ivy", "tla"},
	}}

	leftHash, err := HashModelFamily(left)
	require.NoError(t, err)
	rightHash, err := HashModelFamily(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)
	require.Equal(t, []CapabilityOwner{"workflow", "nexus"}, left.Targets[0].Owners)
}

func validModelFamily() ModelFamily {
	return ModelFamily{
		Version: "test-family/v1",
		Model:   nexusModel(),
		Modules: []Module{
			{Name: "workflow", Owner: "workflow", Entities: []string{"Workflow"}},
			{
				Name:       "nexus",
				Owner:      "nexus",
				Entities:   []string{"NexusOperation"},
				Relations:  []string{"nexus-child-of"},
				Actions:    []string{"schedule"},
				Properties: nil,
			},
		},
	}
}

func contractRefinementFamily() ModelFamily {
	family := validModelFamily()
	family.Model.Actions = append(family.Model.Actions,
		Action{
			Name:       "finish",
			Parameters: []Parameter{{Name: "operation", Type: "NexusOperation", Binding: InputBinding}},
			Guard:      StateIs("NexusOperation", "operation", "scheduled"),
			Effects:    []Effect{{Kind: SetStateEffect, Entity: "NexusOperation", Ref: "operation", State: "scheduled"}},
		},
		Action{
			Name: "workflow-dispatch",
			Parameters: []Parameter{
				{Name: "operation", Type: "NexusOperation", Binding: FreshBinding},
				{Name: "caller", Type: "Workflow", Binding: InputBinding},
			},
			Guard: StateIs("Workflow", "caller", "started"),
			Effects: []Effect{
				{Kind: CreateEffect, Entity: "NexusOperation", Ref: "operation", State: "scheduled"},
				{Kind: AddRelationEffect, Relation: "nexus-child-of", Source: "operation", Target: "caller"},
			},
		},
	)
	family.Modules[0].Actions = []string{"workflow-dispatch"}
	family.Modules[0].Imports = []ObligationRef{{Interface: "delivery", Obligation: "accepted"}}
	family.Modules[1].Actions = []string{"schedule", "finish"}
	family.Interfaces = []Interface{{
		Name: "delivery", Provider: "nexus", Consumers: []string{"workflow"},
		Obligations: []Obligation{{Name: "accepted", Actions: []string{"schedule"}}},
	}}
	family.RefinementMaps = []RefinementMap{{
		Name: "workflow-delivery", Owner: "workflow", Module: "workflow", Interface: "delivery",
		Actions: []ActionRefinement{{Concrete: "workflow-dispatch", Abstract: "schedule"}},
	}}
	family.Targets = []VerificationTarget{{
		Name: "integration", Owners: []CapabilityOwner{"workflow", "nexus"}, Modules: []string{"workflow", "nexus"}, RefinementMaps: []string{"workflow-delivery"},
	}}
	return family
}
