package protocol

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire/verify"
)

const (
	foundationDeliveryTarget = "foundation-delivery-safety"

	workObligationEntity  = "WorkObligation"
	deliveryTaskEntity    = "DeliveryTask"
	deliveryAttemptEntity = "DeliveryAttempt"
	deliveryQueueEntity   = "DeliveryQueue"
	pollerEntity          = "Poller"

	deliveryTaskObligationRelation = "delivery-task-obligation"
	deliveryTaskQueueRelation      = "delivery-task-queue"
	deliveryAttemptTaskRelation    = "delivery-attempt-task"
	deliveryAttemptPollerRelation  = "delivery-attempt-poller"
	deliveryAcceptedStartRelation  = "delivery-accepted-start"
)

type verificationFamilyFragment struct {
	Model        verify.Model
	Modules      []verify.Module
	Interfaces   []verify.Interface
	Compositions []verify.Composition
	Targets      []verify.VerificationTarget
}

func foundationDeliveryVerification() verificationFamilyFragment {
	entities := []verify.EntityType{
		{
			Name:    workObligationEntity,
			IDs:     foundationIDs(workObligationEntity, 2),
			Initial: "unresolved",
			States: []verify.State{
				{Name: "unresolved", MustProgress: true},
				{Name: "valid", MustProgress: true},
				{Name: "accepted"},
				{Name: "terminal", Terminal: true},
			},
		},
		{
			Name:    deliveryTaskEntity,
			IDs:     foundationIDs(deliveryTaskEntity, 2),
			Initial: "pending",
			States: []verify.State{
				{Name: "pending", MustProgress: true},
				{Name: "sync-offered", MustProgress: true},
				{Name: "backlogged", MustProgress: true},
				{Name: "reserved", MustProgress: true},
				{Name: "authorized", MustProgress: true},
				{Name: "dispatched", MustProgress: true},
				{Name: "acknowledged"},
				{Name: "retired", Terminal: true},
			},
		},
		{
			Name:    deliveryAttemptEntity,
			IDs:     foundationIDs(deliveryAttemptEntity, 2),
			Initial: "reserved",
			States: []verify.State{
				{Name: "reserved", MustProgress: true},
				{Name: "accepted", MustProgress: true},
				{Name: "rejected", Terminal: true},
				{Name: "dispatched", MustProgress: true},
				{Name: "failed", Terminal: true},
				{Name: "completed", Terminal: true},
			},
		},
		{
			Name:            deliveryQueueEntity,
			IDs:             foundationIDs(deliveryQueueEntity, 2),
			InitiallyExists: foundationIDs(deliveryQueueEntity, 2),
			Initial:         "available",
			States:          []verify.State{{Name: "available"}},
		},
		{
			Name:            pollerEntity,
			IDs:             foundationIDs(pollerEntity, 2),
			InitiallyExists: foundationIDs(pollerEntity, 2),
			Initial:         "available",
			States:          []verify.State{{Name: "available"}},
		},
	}
	relations := []verify.Relation{
		{Name: deliveryTaskObligationRelation, Source: deliveryTaskEntity, Target: workObligationEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
		{Name: deliveryTaskQueueRelation, Source: deliveryTaskEntity, Target: deliveryQueueEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
		{Name: deliveryAttemptTaskRelation, Source: deliveryAttemptEntity, Target: deliveryTaskEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
		{Name: deliveryAttemptPollerRelation, Source: deliveryAttemptEntity, Target: pollerEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
		{Name: deliveryAcceptedStartRelation, Source: workObligationEntity, Target: deliveryAttemptEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
	}
	actions := foundationDeliveryActions()
	properties := foundationDeliveryProperties()
	model := verify.Model{
		Entities:   entities,
		Relations:  relations,
		Actions:    actions,
		Properties: properties,
	}
	for _, action := range actions {
		model.Abstractions = append(model.Abstractions, verify.Abstraction{
			Name:   action.Name,
			Reason: "abstract foundation delivery protocol",
		})
		model.Refinements = append(model.Refinements, verify.Refinement{
			Name:             action.Name + ".foundation",
			Action:           action.Name,
			LifecycleActions: []string{action.Name},
		})
	}
	modules := []verify.Module{
		{
			Name:       "history-outbox",
			Owner:      "history",
			Entities:   []string{workObligationEntity, deliveryTaskEntity},
			Relations:  []string{deliveryTaskObligationRelation},
			Actions:    []string{"delivery.persist.success", "delivery.persist.ambiguous", "delivery.resolve-persisted", "delivery.expire"},
			Properties: []string{"delivery.no-split-commit", "delivery.ambiguous-commit-resolved", "delivery.coarse-retirement-safety", "delivery.no-resurrection"},
		},
		{
			Name:       "matching-delivery",
			Owner:      "matching",
			Entities:   []string{deliveryAttemptEntity, deliveryQueueEntity, pollerEntity},
			Relations:  []string{deliveryTaskQueueRelation, deliveryAttemptTaskRelation, deliveryAttemptPollerRelation},
			Actions:    []string{"delivery.offer-sync", "delivery.spool", "delivery.reserve", "delivery.dispatch", "delivery.acknowledge", "delivery.retry", "delivery.retire"},
			Properties: []string{"delivery.no-phantom-dispatch", "delivery.failed-start-is-not-accepted", "delivery.retry-preserves-obligation", "delivery.path-equivalence", "delivery.destination-isolation"},
			Imports: []verify.ObligationRef{
				{Interface: "durable-intent", Obligation: "atomic-intent"},
				{Interface: "start-authorization", Obligation: "single-acceptance"},
			},
		},
		{
			Name:       "history-authorization",
			Owner:      "history",
			Relations:  []string{deliveryAcceptedStartRelation},
			Actions:    []string{"delivery.authorize.accept", "delivery.authorize.reject"},
			Properties: []string{"delivery.single-accepted-start"},
		},
	}
	interfaces := []verify.Interface{
		{
			Name:       "durable-intent",
			Provider:   "history-outbox",
			Consumers:  []string{"matching-delivery"},
			Identities: []string{workObligationEntity, deliveryTaskEntity},
			Obligations: []verify.Obligation{{
				Name:       "atomic-intent",
				Actions:    []string{"delivery.persist.success", "delivery.resolve-persisted"},
				Properties: []string{"delivery.no-split-commit"},
			}},
		},
		{
			Name:       "start-authorization",
			Provider:   "history-authorization",
			Consumers:  []string{"matching-delivery"},
			Identities: []string{workObligationEntity, deliveryAttemptEntity},
			Obligations: []verify.Obligation{{
				Name:       "single-acceptance",
				Actions:    []string{"delivery.authorize.accept", "delivery.authorize.reject"},
				Properties: []string{"delivery.single-accepted-start"},
			}},
		},
	}
	composition := verify.Composition{
		Name:       "foundation-delivery",
		Owners:     []verify.CapabilityOwner{"history", "matching"},
		Modules:    []string{"history-outbox", "matching-delivery", "history-authorization"},
		Properties: foundationPropertyNames(properties),
	}
	bounds := map[string]int{
		workObligationEntity:  2,
		deliveryTaskEntity:    2,
		deliveryAttemptEntity: 2,
		deliveryQueueEntity:   2,
		pollerEntity:          2,
	}
	target := verify.VerificationTarget{
		Name:                foundationDeliveryTarget,
		Owners:              []verify.CapabilityOwner{"history", "matching"},
		Modules:             []string{"history-outbox", "matching-delivery", "history-authorization"},
		Compositions:        []string{composition.Name},
		Bounds:              bounds,
		MinimumBounds:       bounds,
		BackendRequirements: []string{"ivy", "p", "tla"},
		FailurePolicy: []string{
			"ambiguous-persistence",
			"lost-authorization-response",
			"retry-after-dispatch-failure",
		},
	}
	return verificationFamilyFragment{
		Model:        model,
		Modules:      modules,
		Interfaces:   interfaces,
		Compositions: []verify.Composition{composition},
		Targets:      []verify.VerificationTarget{target},
	}
}

func foundationDeliveryActions() []verify.Action {
	newIntentParameters := []verify.Parameter{
		{Name: "obligation", Type: workObligationEntity, Binding: verify.FreshBinding},
		{Name: "task", Type: deliveryTaskEntity, Binding: verify.FreshBinding},
		{Name: "queue", Type: deliveryQueueEntity, Binding: verify.InputBinding},
	}
	newIntentEffects := func(state string) []verify.Effect {
		return []verify.Effect{
			{Kind: verify.CreateEffect, Entity: workObligationEntity, Ref: "obligation", State: state},
			{Kind: verify.CreateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "pending"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskQueueRelation, Source: "task", Target: "queue"},
		}
	}
	return []verify.Action{
		{Name: "delivery.persist.success", Parameters: newIntentParameters, Effects: newIntentEffects("valid"), Unrealized: true},
		{Name: "delivery.persist.ambiguous", Parameters: newIntentParameters, Effects: newIntentEffects("unresolved"), Unrealized: true},
		{
			Name: "delivery.resolve-persisted",
			Parameters: []verify.Parameter{
				{Name: "obligation", Type: workObligationEntity, Binding: verify.InputBinding},
				{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
			},
			Guard: verify.And(
				verify.StateIs(workObligationEntity, "obligation", "unresolved"),
				verify.StateIs(deliveryTaskEntity, "task", "pending"),
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
			),
			Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: workObligationEntity, Ref: "obligation", State: "valid"}},
			Unrealized: true,
		},
		deliveryTaskTransition("delivery.offer-sync", "pending", "sync-offered"),
		{
			Name:       "delivery.spool",
			Parameters: []verify.Parameter{{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding}},
			Guard: verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(deliveryTaskEntity, "task", "pending"),
				verify.StateIs(deliveryTaskEntity, "task", "sync-offered"),
			}},
			Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "backlogged"}},
			Unrealized: true,
		},
		{
			Name: "delivery.reserve",
			Parameters: []verify.Parameter{
				{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
				{Name: "attempt", Type: deliveryAttemptEntity, Binding: verify.FreshBinding},
				{Name: "poller", Type: pollerEntity, Binding: verify.InputBinding},
			},
			Guard: verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(deliveryTaskEntity, "task", "sync-offered"),
				verify.StateIs(deliveryTaskEntity, "task", "backlogged"),
			}},
			Effects: []verify.Effect{
				{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "reserved"},
				{Kind: verify.CreateEffect, Entity: deliveryAttemptEntity, Ref: "attempt", State: "reserved"},
				{Kind: verify.AddRelationEffect, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
				{Kind: verify.AddRelationEffect, Relation: deliveryAttemptPollerRelation, Source: "attempt", Target: "poller"},
			},
			Unrealized: true,
		},
		deliveryAuthorizationAction(true),
		deliveryAuthorizationAction(false),
		deliveryAttemptTransition("delivery.dispatch", "authorized", "accepted", "dispatched", "dispatched"),
		deliveryAttemptTransition("delivery.acknowledge", "dispatched", "dispatched", "acknowledged", "completed"),
		deliveryAttemptTransition("delivery.retry", "dispatched", "dispatched", "backlogged", "failed"),
		deliveryTaskTransition("delivery.retire", "acknowledged", "retired"),
		{
			Name: "delivery.expire",
			Parameters: []verify.Parameter{
				{Name: "obligation", Type: workObligationEntity, Binding: verify.InputBinding},
				{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
			},
			Guard: verify.And(
				verify.StateIs(workObligationEntity, "obligation", "valid"),
				verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
					verify.StateIs(deliveryTaskEntity, "task", "pending"),
					verify.StateIs(deliveryTaskEntity, "task", "sync-offered"),
					verify.StateIs(deliveryTaskEntity, "task", "backlogged"),
				}},
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
			),
			Effects: []verify.Effect{
				{Kind: verify.SetStateEffect, Entity: workObligationEntity, Ref: "obligation", State: "terminal"},
				{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "retired"},
			},
			Unrealized: true,
		},
	}
}

func deliveryAuthorizationAction(accept bool) verify.Action {
	name := "delivery.authorize.reject"
	obligationState := "valid"
	taskState := "backlogged"
	attemptState := "rejected"
	if accept {
		name = "delivery.authorize.accept"
		obligationState = "accepted"
		taskState = "authorized"
		attemptState = "accepted"
	}
	guard := verify.And(
		verify.StateIs(workObligationEntity, "obligation", "valid"),
		verify.StateIs(deliveryTaskEntity, "task", "reserved"),
		verify.StateIs(deliveryAttemptEntity, "attempt", "reserved"),
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
	)
	effects := []verify.Effect{
		{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: taskState},
		{Kind: verify.SetStateEffect, Entity: deliveryAttemptEntity, Ref: "attempt", State: attemptState},
	}
	if accept {
		effects = append(effects,
			verify.Effect{Kind: verify.SetStateEffect, Entity: workObligationEntity, Ref: "obligation", State: obligationState},
			verify.Effect{Kind: verify.AddRelationEffect, Relation: deliveryAcceptedStartRelation, Source: "obligation", Target: "attempt"},
		)
	}
	return verify.Action{
		Name: name,
		Parameters: []verify.Parameter{
			{Name: "obligation", Type: workObligationEntity, Binding: verify.InputBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
			{Name: "attempt", Type: deliveryAttemptEntity, Binding: verify.InputBinding},
		},
		Guard: guard, Effects: effects, Unrealized: true,
	}
}

func deliveryTaskTransition(name, from, to string) verify.Action {
	return verify.Action{
		Name:       name,
		Parameters: []verify.Parameter{{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding}},
		Guard:      verify.StateIs(deliveryTaskEntity, "task", from),
		Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: to}},
		Unrealized: true,
	}
}

func deliveryAttemptTransition(name, taskFrom, attemptFrom, taskTo, attemptTo string) verify.Action {
	return verify.Action{
		Name: name,
		Parameters: []verify.Parameter{
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
			{Name: "attempt", Type: deliveryAttemptEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(deliveryTaskEntity, "task", taskFrom),
			verify.StateIs(deliveryAttemptEntity, "attempt", attemptFrom),
			verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
		),
		Effects: []verify.Effect{
			{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: taskTo},
			{Kind: verify.SetStateEffect, Entity: deliveryAttemptEntity, Ref: "attempt", State: attemptTo},
		},
		Unrealized: true,
	}
}

func foundationDeliveryProperties() []verify.Property {
	return []verify.Property{
		{
			Name: "delivery.no-split-commit",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(workObligationEntity, "obligation", deliveryImplies(
				verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
					verify.StateIs(workObligationEntity, "obligation", "valid"),
					verify.StateIs(workObligationEntity, "obligation", "accepted"),
				}},
				deliveryExists(deliveryTaskEntity, "task", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"}),
			)),
		},
		{
			Name: "delivery.no-phantom-dispatch",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryAttemptEntity, "attempt", deliveryImplies(
				verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
					verify.StateIs(deliveryAttemptEntity, "attempt", "dispatched"),
					verify.StateIs(deliveryAttemptEntity, "attempt", "failed"),
					verify.StateIs(deliveryAttemptEntity, "attempt", "completed"),
				}},
				deliveryExists(deliveryTaskEntity, "task", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
					deliveryExists(workObligationEntity, "obligation", verify.And(
						verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
						verify.StateIs(workObligationEntity, "obligation", "accepted"),
					)),
				)),
			)),
		},
		{
			Name: "delivery.ambiguous-commit-resolved",
			Kind: verify.QuiescentProperty,
			Expr: deliveryForAll(workObligationEntity, "obligation",
				verify.Not(verify.StateIs(workObligationEntity, "obligation", "unresolved")),
			),
		},
		{
			Name: "delivery.single-accepted-start",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(workObligationEntity, "obligation", deliveryImplies(
				verify.StateIs(workObligationEntity, "obligation", "accepted"),
				deliveryExists(deliveryAttemptEntity, "attempt", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAcceptedStartRelation, Source: "obligation", Target: "attempt"}),
			)),
		},
		{
			Name: "delivery.failed-start-is-not-accepted",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryAttemptEntity, "attempt", deliveryImplies(
				verify.StateIs(deliveryAttemptEntity, "attempt", "rejected"),
				deliveryForAll(workObligationEntity, "obligation", verify.Not(verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAcceptedStartRelation, Source: "obligation", Target: "attempt"})),
			)),
		},
		{
			Name: "delivery.retry-preserves-obligation",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryAttemptEntity, "attempt",
				deliveryExists(deliveryTaskEntity, "task", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
					deliveryExists(workObligationEntity, "obligation", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"}),
				)),
			),
		},
		{
			Name: "delivery.destination-isolation",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryTaskEntity, "task",
				deliveryExists(deliveryQueueEntity, "queue", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskQueueRelation, Source: "task", Target: "queue"}),
			),
		},
		{
			Name: "delivery.path-equivalence",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryTaskEntity, "task", verify.And(
				deliveryExists(workObligationEntity, "obligation", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"}),
				deliveryExists(deliveryQueueEntity, "queue", verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskQueueRelation, Source: "task", Target: "queue"}),
			)),
		},
		{
			Name: "delivery.coarse-retirement-safety",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(deliveryTaskEntity, "task", deliveryImplies(
				verify.StateIs(deliveryTaskEntity, "task", "retired"),
				deliveryExists(workObligationEntity, "obligation", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
					verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
						verify.StateIs(workObligationEntity, "obligation", "accepted"),
						verify.StateIs(workObligationEntity, "obligation", "terminal"),
					}},
				)),
			)),
		},
		{
			Name: "delivery.no-resurrection",
			Kind: verify.SafetyProperty,
			Expr: deliveryForAll(workObligationEntity, "obligation", deliveryImplies(
				verify.StateIs(workObligationEntity, "obligation", "terminal"),
				deliveryForAll(deliveryTaskEntity, "task", deliveryImplies(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
					verify.StateIs(deliveryTaskEntity, "task", "retired"),
				)),
			)),
		},
	}
}

func deliveryForAll(entity, variable string, body verify.Expr) verify.Expr {
	return verify.Expr{Op: verify.ForAllExpr, Entity: entity, Var: variable, Args: []verify.Expr{body}}
}

func deliveryExists(entity, variable string, body verify.Expr) verify.Expr {
	return verify.Expr{Op: verify.ExistsExpr, Entity: entity, Var: variable, Args: []verify.Expr{body}}
}

func deliveryImplies(condition, consequence verify.Expr) verify.Expr {
	return verify.Expr{Op: verify.ImpliesExpr, Args: []verify.Expr{condition, consequence}}
}

func foundationIDs(entity string, count int) []string {
	result := make([]string, count)
	for index := range count {
		result[index] = fmt.Sprintf("%s#%d", entity, index)
	}
	return result
}

func foundationPropertyNames(properties []verify.Property) []string {
	result := make([]string, len(properties))
	for index, property := range properties {
		result[index] = property.Name
	}
	return result
}
