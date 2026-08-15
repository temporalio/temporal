package protocol

import (
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
)

func featureDeliveryAuthorizationAction(
	name string,
	featureEntity string,
	featureFrom string,
	featureTo string,
	featureObligationRelation string,
	featureTaskRelation string,
	accept bool,
) verify.Action {
	foundationName := "delivery.authorize.reject"
	if accept {
		foundationName = "delivery.authorize.accept"
	}
	foundation := foundationAction(foundationName)
	action := foundation
	action.Name = name
	action.Parameters = append([]verify.Parameter{{Name: "entity", Type: featureEntity, Binding: verify.InputBinding}}, slices.Clone(foundation.Parameters)...)
	action.Guard = verify.And(
		verify.StateIs(featureEntity, "entity", featureFrom),
		verify.Expr{Op: verify.RelationHasExpr, Relation: featureObligationRelation, Source: "entity", Target: "obligation"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: featureTaskRelation, Source: "entity", Target: "task"},
		foundation.Guard,
	)
	if accept {
		action.Effects = append([]verify.Effect{{
			Kind: verify.SetStateEffect, Entity: featureEntity, Ref: "entity", State: featureTo,
		}}, slices.Clone(foundation.Effects)...)
	} else {
		action.Effects = slices.Clone(foundation.Effects)
	}
	action.Unrealized = true
	action.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_delivery_authorization.go", Symbol: name}
	return action
}

func featureAcceptedStartProperty(
	name string,
	featureEntity string,
	featureVariable string,
	acceptedState string,
	featureObligationRelation string,
) verify.Property {
	return verify.Property{
		Name: name,
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(featureEntity, featureVariable, deliveryImplies(
			verify.StateIs(featureEntity, featureVariable, acceptedState),
			deliveryExists(workObligationEntity, "obligation", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: featureObligationRelation, Source: featureVariable, Target: "obligation"},
				verify.StateIs(workObligationEntity, "obligation", "accepted"),
				deliveryExists(deliveryAttemptEntity, "attempt", verify.Expr{
					Op: verify.RelationHasExpr, Relation: deliveryAcceptedStartRelation, Source: "obligation", Target: "attempt",
				}),
			)),
		)),
		Source: verify.Provenance{Path: "tests/umpire2/protocol/verification_delivery_authorization.go", Symbol: name},
	}
}
