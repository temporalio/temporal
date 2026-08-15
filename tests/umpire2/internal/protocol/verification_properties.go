package protocol

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// NexusActivityLinkConsistencyProperties is shared by the live rule and generated models.
func NexusActivityLinkConsistencyProperties() []verify.Property {
	source := verify.Provenance{Path: "tests/umpire2/rule/nexus_activity_link_consistency.go", Symbol: "NexusActivityLinkConsistencyRule"}
	return []verify.Property{
		{
			Name:   "NexusActivityForwardLinkConsistency",
			Kind:   verify.SafetyProperty,
			Expr:   reciprocalLinkProperty(NexusActivityRelation, ActivityNexusRelation),
			Source: source,
		},
		{
			Name:   "NexusActivityReverseLinkConsistency",
			Kind:   verify.SafetyProperty,
			Expr:   reciprocalLinkProperty(ActivityNexusRelation, NexusActivityRelation),
			Source: source,
		},
	}
}

func NexusActivityStrengtheningProperties() []verify.Property {
	return []verify.Property{{
		Name: "NexusActivityTerminalRefinement",
		Kind: verify.SafetyProperty,
		Expr: verify.Expr{
			Op:     verify.ForAllExpr,
			Entity: string(model.NexusOperationType),
			Var:    "operation",
			Args: []verify.Expr{{
				Op:     verify.ForAllExpr,
				Entity: string(model.ActivityType),
				Var:    "activity",
				Args: []verify.Expr{{
					Op: verify.ImpliesExpr,
					Args: []verify.Expr{
						{Op: verify.RelationHasExpr, Relation: string(NexusActivityRelation), Source: "operation", Target: "activity"},
						verify.And(
							verify.StateIs(string(model.NexusOperationType), "operation", model.NexusSucceeded),
							verify.StateIs(string(model.ActivityType), "activity", model.ActivityCompleted),
						),
					},
				}},
			}},
		},
		Strengthening: true,
		Source: verify.Provenance{
			Path:   "tests/umpire2/protocol/regress_domain.go",
			Symbol: "nexus.start_activity",
		},
	}}
}

func reciprocalLinkProperty(forward, reverseRelationType umpire.RelationType) verify.Expr {
	forwardRelation := string(forward)
	reverseRelation := string(reverseRelationType)
	forwardSource := string(model.NexusOperationType)
	forwardTarget := string(model.ActivityType)
	if forwardRelation == string(ActivityNexusRelation) {
		forwardSource, forwardTarget = forwardTarget, forwardSource
	}
	return verify.Expr{
		Op:     verify.ForAllExpr,
		Entity: forwardSource,
		Var:    "source",
		Args: []verify.Expr{{
			Op:     verify.ForAllExpr,
			Entity: forwardTarget,
			Var:    "target",
			Args: []verify.Expr{{
				Op: verify.ImpliesExpr,
				Args: []verify.Expr{
					{Op: verify.RelationHasExpr, Relation: forwardRelation, Source: "source", Target: "target"},
					{Op: verify.RelationHasExpr, Relation: reverseRelation, Source: "target", Target: "source"},
				},
			}},
		}},
	}
}
