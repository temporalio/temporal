package protocol

import (
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

const (
	activityDeliveryTarget              = "integration-activity-delivery"
	activityDeliveryIntentModule        = "activity-delivery-intent"
	activityDeliveryAuthorizationModule = "activity-delivery-authorization"

	activityObligationRelation = "activity-obligation"
	activityDeliveryRelation   = "activity-delivery-task"
)

func activityDeliveryVerification() verificationFamilyFragment {
	activity := string(model.ActivityType)
	persist := foundationAction("delivery.persist.success")
	schedule := persist
	schedule.Name = "activity.delivery.persist"
	schedule.Parameters = append([]verify.Parameter{{Name: "entity", Type: activity, Binding: verify.FreshBinding}}, slices.Clone(persist.Parameters)...)
	schedule.Guard = verify.StateIs(activity, "entity", model.ActivityUnspecified)
	schedule.Effects = append([]verify.Effect{{
		Kind: verify.CreateEffect, Entity: activity, Ref: "entity", State: model.ActivityScheduled,
	}}, slices.Clone(persist.Effects)...)
	schedule.Effects = append(schedule.Effects, activityDeliveryLinkEffects()...)
	schedule.Unrealized = true
	schedule.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_activity_delivery.go", Symbol: schedule.Name}

	retry := persist
	retry.Name = "activity.delivery.retry"
	retry.Parameters = append([]verify.Parameter{
		{Name: "entity", Type: activity, Binding: verify.InputBinding},
		{Name: "previous-obligation", Type: workObligationEntity, Binding: verify.InputBinding},
		{Name: "previous-task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
	}, slices.Clone(persist.Parameters)...)
	retry.Guard = verify.And(
		verify.StateIs(activity, "entity", model.ActivityBackingOff),
		verify.Expr{Op: verify.RelationHasExpr, Relation: activityObligationRelation, Source: "entity", Target: "previous-obligation"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: activityDeliveryRelation, Source: "entity", Target: "previous-task"},
	)
	retry.Effects = []verify.Effect{
		{Kind: verify.SetStateEffect, Entity: activity, Ref: "entity", State: model.ActivityScheduled},
		{Kind: verify.RemoveRelationEffect, Relation: activityObligationRelation, Source: "entity", Target: "previous-obligation"},
		{Kind: verify.RemoveRelationEffect, Relation: activityDeliveryRelation, Source: "entity", Target: "previous-task"},
	}
	retry.Effects = append(retry.Effects, slices.Clone(persist.Effects)...)
	retry.Effects = append(retry.Effects, activityDeliveryLinkEffects()...)
	retry.Unrealized = true
	retry.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_activity_delivery.go", Symbol: retry.Name}

	resolve := foundationAction("delivery.resolve-persisted")
	resolve.Name = "activity.delivery.resolve-persisted"
	resolve.Unrealized = true
	resolve.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_activity_delivery.go", Symbol: resolve.Name}
	authorize := featureDeliveryAuthorizationAction(
		"activity.delivery.authorize", activity, model.ActivityScheduled, model.ActivityStarted,
		activityObligationRelation, activityDeliveryRelation, true,
	)
	reject := featureDeliveryAuthorizationAction(
		"activity.delivery.reject", activity, model.ActivityScheduled, model.ActivityScheduled,
		activityObligationRelation, activityDeliveryRelation, false,
	)

	property := verify.Property{
		Name: "activity.delivery.intent-correspondence",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(activity, "activity", deliveryImplies(
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(activity, "activity", model.ActivityScheduled),
				verify.StateIs(activity, "activity", model.ActivityStarted),
				verify.StateIs(activity, "activity", model.ActivityBackingOff),
			}},
			verify.And(
				deliveryExists(workObligationEntity, "obligation", verify.Expr{
					Op: verify.RelationHasExpr, Relation: activityObligationRelation, Source: "activity", Target: "obligation",
				}),
				deliveryExists(deliveryTaskEntity, "deliveryTask", verify.Expr{
					Op: verify.RelationHasExpr, Relation: activityDeliveryRelation, Source: "activity", Target: "deliveryTask",
				}),
			),
		)),
		Source: verify.Provenance{Path: "tests/umpire2/protocol/verification_activity_delivery.go", Symbol: "activity.delivery.intent-correspondence"},
	}
	authorizationProperty := featureAcceptedStartProperty(
		"activity.delivery.accepted-start-correspondence", activity, "activity", model.ActivityStarted,
		activityObligationRelation,
	)

	module := verify.Module{
		Name: activityDeliveryIntentModule, Owner: "activity",
		Relations:  []string{activityObligationRelation, activityDeliveryRelation},
		Actions:    []string{schedule.Name, retry.Name, resolve.Name},
		Properties: []string{property.Name},
		Imports:    []verify.ObligationRef{{Interface: "durable-intent", Obligation: "atomic-intent"}},
	}
	authorizationModule := verify.Module{
		Name: activityDeliveryAuthorizationModule, Owner: "activity",
		Actions:    []string{authorize.Name, reject.Name},
		Properties: []string{authorizationProperty.Name},
		Imports:    []verify.ObligationRef{{Interface: "start-authorization", Obligation: "single-acceptance"}},
	}
	contractRefinement := verify.RefinementMap{
		Name: "activity-delivery-intent", Owner: "activity", Module: module.Name, Interface: "durable-intent",
		Actions: []verify.ActionRefinement{
			{Concrete: schedule.Name, Abstract: persist.Name},
			{Concrete: retry.Name, Abstract: persist.Name},
			{Concrete: resolve.Name, Abstract: "delivery.resolve-persisted"},
		},
		Identities: []verify.IdentityRefinement{
			{Concrete: activity, Abstract: workObligationEntity},
			{Concrete: activity, Abstract: deliveryTaskEntity},
		},
	}
	lifecycleRefinement := verify.RefinementMap{
		Name: "activity-delivery-lifecycle", Owner: "activity",
		Actions: []verify.ActionRefinement{
			{Concrete: schedule.Name, Abstract: "Activity.unspecified.schedule.AnyHosting"},
			{Concrete: retry.Name, Abstract: "Activity.backing_off.schedule.AnyHosting"},
			{Concrete: resolve.Name, Stutter: true},
		},
	}
	authorizationRefinement := verify.RefinementMap{
		Name: "activity-delivery-authorization", Owner: "activity", Module: authorizationModule.Name, Interface: "start-authorization",
		Actions: []verify.ActionRefinement{
			{Concrete: authorize.Name, Abstract: "delivery.authorize.accept"},
			{Concrete: reject.Name, Abstract: "delivery.authorize.reject"},
		},
		Identities: []verify.IdentityRefinement{
			{Concrete: activity, Abstract: workObligationEntity},
			{Concrete: activity, Abstract: deliveryAttemptEntity},
		},
	}
	authorizationLifecycleRefinement := verify.RefinementMap{
		Name: "activity-delivery-authorization-lifecycle", Owner: "activity",
		Actions: []verify.ActionRefinement{
			{Concrete: authorize.Name, Abstract: "Activity.scheduled.start.AnyHosting"},
			{Concrete: reject.Name, Stutter: true},
		},
	}
	composition := verify.Composition{
		Name: "activity-delivery", Owners: []verify.CapabilityOwner{"activity", "history", "matching"},
		Modules:        []string{activityDeliveryIntentModule, activityDeliveryAuthorizationModule, "history-outbox", "matching-delivery", "history-authorization"},
		Properties:     append(foundationPropertyNames(foundationDeliveryProperties()), property.Name, authorizationProperty.Name),
		RefinementMaps: []string{contractRefinement.Name, lifecycleRefinement.Name, authorizationRefinement.Name, authorizationLifecycleRefinement.Name},
	}
	target := verify.VerificationTarget{
		Name: activityDeliveryTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: map[string]int{
			activity: 1, workObligationEntity: 2, deliveryTaskEntity: 2, deliveryAttemptEntity: 2, deliveryQueueEntity: 2, pollerEntity: 2,
		},
		MinimumBounds:       map[string]int{activity: 1, workObligationEntity: 2, deliveryTaskEntity: 2},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"activity-retry", "ambiguous-persistence", "lost-authorization-response"},
		Abstractions:        []string{"regression.nexus.start_activity"},
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Relations: []verify.Relation{
				{Name: activityObligationRelation, Source: activity, Target: workObligationEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
				{Name: activityDeliveryRelation, Source: activity, Target: deliveryTaskEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
			},
			Actions:    []verify.Action{schedule, retry, resolve, authorize, reject},
			Properties: []verify.Property{property, authorizationProperty},
			Abstractions: []verify.Abstraction{{
				Name: "regression.nexus.start_activity", Reason: "Nexus-linked Activity creation is checked by a Nexus and Activity composition",
				Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: "nexus.start_activity"},
			}},
			Refinements: []verify.Refinement{
				{Name: schedule.Name, Action: schedule.Name, LifecycleActions: []string{"Activity.unspecified.schedule.AnyHosting"}, Source: schedule.Source},
				{Name: retry.Name, Action: retry.Name, LifecycleActions: []string{"Activity.backing_off.schedule.AnyHosting"}, Source: retry.Source},
				{Name: resolve.Name, Action: resolve.Name, LifecycleActions: []string{"delivery.resolve-persisted"}, Source: resolve.Source},
				{Name: authorize.Name, Action: authorize.Name, LifecycleActions: []string{"Activity.scheduled.start.AnyHosting", "delivery.authorize.accept"}, Source: authorize.Source},
				{Name: reject.Name, Action: reject.Name, LifecycleActions: []string{"delivery.authorize.reject"}, Source: reject.Source},
			},
		},
		Modules: []verify.Module{module, authorizationModule},
		Refinements: []verify.RefinementMap{
			contractRefinement, lifecycleRefinement, authorizationRefinement, authorizationLifecycleRefinement,
		},
		Compositions: []verify.Composition{composition}, Targets: []verify.VerificationTarget{target},
	}
}

func activityDeliveryLinkEffects() []verify.Effect {
	return []verify.Effect{
		{Kind: verify.AddRelationEffect, Relation: activityObligationRelation, Source: "entity", Target: "obligation"},
		{Kind: verify.AddRelationEffect, Relation: activityDeliveryRelation, Source: "entity", Target: "task"},
	}
}
