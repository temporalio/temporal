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
