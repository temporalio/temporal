package protocol

import (
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
)

const (
	deliveryRoutingTarget   = "foundation-routing-isolation"
	ownershipFencingTarget  = "foundation-ownership-fencing"
	deliveryRoutingModule   = "matching-routing"
	historyOwnershipModule  = "history-ownership"
	routingPropertyModule   = "matching-routing-properties"
	ownershipPropertyModule = "cross-owner-fencing-properties"

	deliveryRouteEntity              = "DeliveryRouteClass"
	deliveryPartitionEntity          = "MatchingQueuePartition"
	deliveryOwnerGenerationEntity    = "MatchingOwnerGeneration"
	historyShardEntity               = "HistoryShard"
	historyOwnerGenerationEntity     = "HistoryOwnerGeneration"
	deliveryTaskRouteRelation        = "delivery-task-route"
	deliveryPollerRouteRelation      = "delivery-poller-route"
	deliveryPartitionRouteRelation   = "delivery-partition-route"
	deliveryPartitionOwnerRelation   = "delivery-partition-owner"
	deliveryTaskOwnerRelation        = "delivery-task-owner-generation"
	historyShardOwnerRelation        = "history-shard-owner"
	deliveryTaskHistoryRelation      = "delivery-task-history-shard"
	deliveryTaskHistoryOwnerRelation = "delivery-task-history-owner-generation"
)

func deliveryRoutingVerification() verificationFamilyFragment {
	historyBootstrap := verify.Action{
		Name: "routing.bootstrap-history-owner",
		Parameters: []verify.Parameter{
			{Name: "shard", Type: historyShardEntity, Binding: verify.FreshBinding},
			{Name: "historyGeneration", Type: historyOwnerGenerationEntity, Binding: verify.FreshBinding},
		},
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: historyShardEntity, Ref: "shard", State: "owned"},
			{Kind: verify.CreateEffect, Entity: historyOwnerGenerationEntity, Ref: "historyGeneration", State: "current"},
			{Kind: verify.AddRelationEffect, Relation: historyShardOwnerRelation, Source: "shard", Target: "historyGeneration"},
		},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.bootstrap-history-owner"),
	}
	bootstrap := verify.Action{
		Name: "routing.bootstrap",
		Parameters: []verify.Parameter{
			{Name: "route", Type: deliveryRouteEntity, Binding: verify.FreshBinding},
			{Name: "partition", Type: deliveryPartitionEntity, Binding: verify.FreshBinding},
			{Name: "generation", Type: deliveryOwnerGenerationEntity, Binding: verify.FreshBinding},
		},
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: deliveryRouteEntity, Ref: "route", State: "active"},
			{Kind: verify.CreateEffect, Entity: deliveryPartitionEntity, Ref: "partition", State: "owned"},
			{Kind: verify.CreateEffect, Entity: deliveryOwnerGenerationEntity, Ref: "generation", State: "current"},
			{Kind: verify.AddRelationEffect, Relation: deliveryPartitionRouteRelation, Source: "partition", Target: "route"},
			{Kind: verify.AddRelationEffect, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "generation"},
		},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.bootstrap"),
	}
	assign := verify.Action{
		Name: "routing.forward-to-matching",
		Parameters: []verify.Parameter{
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
			{Name: "route", Type: deliveryRouteEntity, Binding: verify.InputBinding},
			{Name: "partition", Type: deliveryPartitionEntity, Binding: verify.InputBinding},
			{Name: "generation", Type: deliveryOwnerGenerationEntity, Binding: verify.InputBinding},
			{Name: "shard", Type: historyShardEntity, Binding: verify.InputBinding},
			{Name: "historyGeneration", Type: historyOwnerGenerationEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(deliveryTaskEntity, "task", "pending"),
				verify.StateIs(deliveryTaskEntity, "task", "sync-offered"),
				verify.StateIs(deliveryTaskEntity, "task", "backlogged"),
			}},
			deliveryForAll(deliveryRouteEntity, "existingRoute", verify.Not(verify.Expr{
				Op: verify.RelationHasExpr, Relation: deliveryTaskRouteRelation, Source: "task", Target: "existingRoute",
			})),
			verify.StateIs(deliveryOwnerGenerationEntity, "generation", "current"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPartitionRouteRelation, Source: "partition", Target: "route"},
			verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "generation"},
			verify.StateIs(historyOwnerGenerationEntity, "historyGeneration", "current"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: historyShardOwnerRelation, Source: "shard", Target: "historyGeneration"},
		),
		Effects: []verify.Effect{
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskRouteRelation, Source: "task", Target: "route"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskOwnerRelation, Source: "task", Target: "generation"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskHistoryRelation, Source: "task", Target: "shard"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskHistoryOwnerRelation, Source: "task", Target: "historyGeneration"},
		},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.forward-to-matching"),
	}
	registerPoller := verify.Action{
		Name: "routing.register-poller",
		Parameters: []verify.Parameter{
			{Name: "poller", Type: pollerEntity, Binding: verify.InputBinding},
			{Name: "route", Type: deliveryRouteEntity, Binding: verify.InputBinding},
		},
		Guard: deliveryForAll(deliveryRouteEntity, "registeredRoute", verify.Not(verify.Expr{
			Op: verify.RelationHasExpr, Relation: deliveryPollerRouteRelation, Source: "poller", Target: "registeredRoute",
		})),
		Effects:    []verify.Effect{{Kind: verify.AddRelationEffect, Relation: deliveryPollerRouteRelation, Source: "poller", Target: "route"}},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.register-poller"),
	}
	foundationReserve := foundationAction("delivery.reserve")
	reserve := foundationReserve
	reserve.Name = "routing.reserve-compatible"
	reserve.Parameters = append(slices.Clone(foundationReserve.Parameters),
		verify.Parameter{Name: "route", Type: deliveryRouteEntity, Binding: verify.InputBinding},
		verify.Parameter{Name: "partition", Type: deliveryPartitionEntity, Binding: verify.InputBinding},
		verify.Parameter{Name: "generation", Type: deliveryOwnerGenerationEntity, Binding: verify.InputBinding},
	)
	reserve.Guard = verify.And(
		foundationReserve.Guard,
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskRouteRelation, Source: "task", Target: "route"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPollerRouteRelation, Source: "poller", Target: "route"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPartitionRouteRelation, Source: "partition", Target: "route"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "generation"},
		verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskOwnerRelation, Source: "task", Target: "generation"},
		verify.StateIs(deliveryOwnerGenerationEntity, "generation", "current"),
	)
	reserve.Source = deliveryRoutingSource(reserve.Name)
	handoff := verify.Action{
		Name: "routing.handoff",
		Parameters: []verify.Parameter{
			{Name: "partition", Type: deliveryPartitionEntity, Binding: verify.InputBinding},
			{Name: "oldGeneration", Type: deliveryOwnerGenerationEntity, Binding: verify.InputBinding},
			{Name: "newGeneration", Type: deliveryOwnerGenerationEntity, Binding: verify.FreshBinding},
		},
		Guard: verify.And(
			verify.StateIs(deliveryOwnerGenerationEntity, "oldGeneration", "current"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "oldGeneration"},
			deliveryForAll(deliveryTaskEntity, "task", deliveryImplies(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskOwnerRelation, Source: "task", Target: "oldGeneration"},
				deliveryTaskSafeForHandoff("task"),
			)),
		),
		Effects: []verify.Effect{
			{Kind: verify.SetStateEffect, Entity: deliveryOwnerGenerationEntity, Ref: "oldGeneration", State: "stale"},
			{Kind: verify.CreateEffect, Entity: deliveryOwnerGenerationEntity, Ref: "newGeneration", State: "current"},
			{Kind: verify.RemoveRelationEffect, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "oldGeneration"},
			{Kind: verify.AddRelationEffect, Relation: deliveryPartitionOwnerRelation, Source: "partition", Target: "newGeneration"},
		},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.handoff"),
	}
	historyHandoff := verify.Action{
		Name: "routing.handoff-history-owner",
		Parameters: []verify.Parameter{
			{Name: "shard", Type: historyShardEntity, Binding: verify.InputBinding},
			{Name: "oldHistoryGeneration", Type: historyOwnerGenerationEntity, Binding: verify.InputBinding},
			{Name: "newHistoryGeneration", Type: historyOwnerGenerationEntity, Binding: verify.FreshBinding},
		},
		Guard: verify.And(
			verify.StateIs(historyOwnerGenerationEntity, "oldHistoryGeneration", "current"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: historyShardOwnerRelation, Source: "shard", Target: "oldHistoryGeneration"},
			deliveryForAll(deliveryTaskEntity, "task", deliveryImplies(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskHistoryOwnerRelation, Source: "task", Target: "oldHistoryGeneration"},
				deliveryTaskSafeForHandoff("task"),
			)),
		),
		Effects: []verify.Effect{
			{Kind: verify.SetStateEffect, Entity: historyOwnerGenerationEntity, Ref: "oldHistoryGeneration", State: "stale"},
			{Kind: verify.CreateEffect, Entity: historyOwnerGenerationEntity, Ref: "newHistoryGeneration", State: "current"},
			{Kind: verify.RemoveRelationEffect, Relation: historyShardOwnerRelation, Source: "shard", Target: "oldHistoryGeneration"},
			{Kind: verify.AddRelationEffect, Relation: historyShardOwnerRelation, Source: "shard", Target: "newHistoryGeneration"},
		},
		Unrealized: true,
		Source:     deliveryRoutingSource("routing.handoff-history-owner"),
	}
	properties := []verify.Property{
		deliveryRouteIsolationProperty(),
		deliveryOwnerFencingProperty(),
	}
	actions := []verify.Action{historyBootstrap, bootstrap, assign, registerPoller, reserve, handoff, historyHandoff}
	module := verify.Module{
		Name: deliveryRoutingModule, Owner: "matching",
		Entities: []string{deliveryRouteEntity, deliveryPartitionEntity, deliveryOwnerGenerationEntity},
		Relations: []string{
			deliveryTaskRouteRelation, deliveryPollerRouteRelation, deliveryPartitionRouteRelation,
			deliveryPartitionOwnerRelation, deliveryTaskOwnerRelation,
		},
		Actions: []string{bootstrap.Name, assign.Name, registerPoller.Name, reserve.Name, handoff.Name},
	}
	historyModule := verify.Module{
		Name: historyOwnershipModule, Owner: "history",
		Entities:  []string{historyShardEntity, historyOwnerGenerationEntity},
		Relations: []string{historyShardOwnerRelation, deliveryTaskHistoryRelation, deliveryTaskHistoryOwnerRelation},
		Actions:   []string{historyBootstrap.Name, historyHandoff.Name},
	}
	routingProperties := verify.Module{Name: routingPropertyModule, Owner: "matching", Properties: []string{properties[0].Name}}
	ownershipProperties := verify.Module{Name: ownershipPropertyModule, Owner: "matching", Properties: []string{properties[1].Name}}
	refinement := verify.RefinementMap{
		Name: "routing-foundation-reservation", Owner: "matching",
		Actions: []verify.ActionRefinement{{Concrete: reserve.Name, Abstract: foundationReserve.Name}},
	}
	composition := verify.Composition{
		Name: "foundation-routing", Owners: []verify.CapabilityOwner{"history", "matching"},
		Modules:        []string{"history-outbox", "matching-delivery", "history-authorization", historyOwnershipModule, deliveryRoutingModule, routingPropertyModule, ownershipPropertyModule},
		Properties:     append(foundationPropertyNames(foundationDeliveryProperties()), properties[0].Name, properties[1].Name),
		RefinementMaps: []string{refinement.Name},
	}
	ownershipComposition := composition
	ownershipComposition.Name = "foundation-ownership"
	ownershipComposition.Modules = append(slices.Clone(composition.Modules[:len(composition.Modules)-2]), ownershipPropertyModule)
	ownershipComposition.Properties = append(foundationPropertyNames(foundationDeliveryProperties()), properties[1].Name)
	target := verify.VerificationTarget{
		Name: deliveryRoutingTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: map[string]int{
			workObligationEntity: 2, deliveryTaskEntity: 2, deliveryAttemptEntity: 2, deliveryQueueEntity: 2, pollerEntity: 2,
			deliveryRouteEntity: 2, deliveryPartitionEntity: 2, deliveryOwnerGenerationEntity: 2, historyShardEntity: 2, historyOwnerGenerationEntity: 2,
		},
		MinimumBounds: map[string]int{
			deliveryTaskEntity: 2, deliveryAttemptEntity: 2, pollerEntity: 2,
			deliveryRouteEntity: 2, deliveryPartitionEntity: 2, deliveryOwnerGenerationEntity: 2, historyShardEntity: 2, historyOwnerGenerationEntity: 2,
		},
		BackendRequirements: []string{"apalache", "fizz", "ivy", "p", "sany"},
		FailurePolicy:       []string{"owner-handoff", "route-crossing", "incompatible-poller"},
		Omissions: []verify.Abstraction{{
			Name: "workflow.task.create-speculative-direct", Reason: "checked by the speculative Workflow Task target",
			Source: workflowTaskSpeculativeSource("workflow.task.create-speculative-direct"),
		}},
	}
	ownershipTarget := target
	ownershipTarget.Name = ownershipFencingTarget
	ownershipTarget.Modules = slices.Clone(ownershipComposition.Modules)
	ownershipTarget.Compositions = []string{ownershipComposition.Name}
	ownershipTarget.Bounds = map[string]int{
		workObligationEntity: 1, deliveryTaskEntity: 1, deliveryAttemptEntity: 1, deliveryQueueEntity: 2, pollerEntity: 2,
		deliveryRouteEntity: 1, deliveryPartitionEntity: 1, deliveryOwnerGenerationEntity: 2, historyShardEntity: 1, historyOwnerGenerationEntity: 2,
	}
	ownershipTarget.MinimumBounds = map[string]int{
		deliveryTaskEntity: 1, deliveryAttemptEntity: 1, pollerEntity: 2,
		deliveryRouteEntity: 1, deliveryPartitionEntity: 1, deliveryOwnerGenerationEntity: 2, historyShardEntity: 1, historyOwnerGenerationEntity: 2,
	}
	ownershipTarget.BackendRequirements = []string{"fizz", "ivy", "tla"}
	ownershipTarget.FailurePolicy = []string{"history-owner-handoff", "matching-owner-handoff", "cross-owner-forwarding"}
	refinements := make([]verify.Refinement, len(actions))
	for index, action := range actions {
		lifecycle := []string{action.Name}
		if action.Name == reserve.Name {
			lifecycle = []string{foundationReserve.Name}
		}
		refinements[index] = verify.Refinement{Name: action.Name, Action: action.Name, LifecycleActions: lifecycle, Source: action.Source}
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Entities: []verify.EntityType{
				{Name: deliveryRouteEntity, IDs: foundationIDs(deliveryRouteEntity, 2), Initial: "inactive", States: []verify.State{{Name: "inactive"}, {Name: "active"}}, Source: deliveryRoutingSource(deliveryRouteEntity)},
				{Name: deliveryPartitionEntity, IDs: foundationIDs(deliveryPartitionEntity, 2), Initial: "unowned", States: []verify.State{{Name: "unowned"}, {Name: "owned"}}, Source: deliveryRoutingSource(deliveryPartitionEntity)},
				{Name: deliveryOwnerGenerationEntity, IDs: foundationIDs(deliveryOwnerGenerationEntity, 2), Initial: "unused", States: []verify.State{{Name: "unused"}, {Name: "current"}, {Name: "stale"}}, Source: deliveryRoutingSource(deliveryOwnerGenerationEntity)},
				{Name: historyShardEntity, IDs: foundationIDs(historyShardEntity, 2), Initial: "unowned", States: []verify.State{{Name: "unowned"}, {Name: "owned"}}, Source: deliveryRoutingSource(historyShardEntity)},
				{Name: historyOwnerGenerationEntity, IDs: foundationIDs(historyOwnerGenerationEntity, 2), Initial: "unused", States: []verify.State{{Name: "unused"}, {Name: "current"}, {Name: "stale"}}, Source: deliveryRoutingSource(historyOwnerGenerationEntity)},
			},
			Relations: []verify.Relation{
				{Name: deliveryTaskRouteRelation, Source: deliveryTaskEntity, Target: deliveryRouteEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryPollerRouteRelation, Source: pollerEntity, Target: deliveryRouteEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryPartitionRouteRelation, Source: deliveryPartitionEntity, Target: deliveryRouteEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryPartitionOwnerRelation, Source: deliveryPartitionEntity, Target: deliveryOwnerGenerationEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryTaskOwnerRelation, Source: deliveryTaskEntity, Target: deliveryOwnerGenerationEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: historyShardOwnerRelation, Source: historyShardEntity, Target: historyOwnerGenerationEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryTaskHistoryRelation, Source: deliveryTaskEntity, Target: historyShardEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: deliveryTaskHistoryOwnerRelation, Source: deliveryTaskEntity, Target: historyOwnerGenerationEntity, SourceCardinality: verify.One, TargetCardinality: verify.Many},
			},
			Actions: actions, Properties: properties, Refinements: refinements,
		},
		Modules: []verify.Module{historyModule, module, routingProperties, ownershipProperties}, Refinements: []verify.RefinementMap{refinement},
		Compositions: []verify.Composition{composition, ownershipComposition}, Targets: []verify.VerificationTarget{target, ownershipTarget},
	}
}

func deliveryRouteIsolationProperty() verify.Property {
	return verify.Property{
		Name: "delivery.routing-isolation",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(deliveryAttemptEntity, "attempt", deliveryForAll(deliveryTaskEntity, "task", deliveryForAll(pollerEntity, "poller", deliveryImplies(
			verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptPollerRelation, Source: "attempt", Target: "poller"},
			),
			deliveryExists(deliveryRouteEntity, "route", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskRouteRelation, Source: "task", Target: "route"},
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryPollerRouteRelation, Source: "poller", Target: "route"},
			)),
		)))),
		Source: deliveryRoutingSource("delivery.routing-isolation"),
	}
}

func deliveryOwnerFencingProperty() verify.Property {
	return verify.Property{
		Name: "delivery.owner-generation-fencing",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(deliveryAttemptEntity, "attempt", deliveryImplies(
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(deliveryAttemptEntity, "attempt", "reserved"),
				verify.StateIs(deliveryAttemptEntity, "attempt", "accepted"),
				verify.StateIs(deliveryAttemptEntity, "attempt", "dispatched"),
			}},
			deliveryExists(deliveryTaskEntity, "task", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryAttemptTaskRelation, Source: "attempt", Target: "task"},
				deliveryExists(deliveryOwnerGenerationEntity, "generation", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskOwnerRelation, Source: "task", Target: "generation"},
					verify.StateIs(deliveryOwnerGenerationEntity, "generation", "current"),
				)),
				deliveryExists(historyOwnerGenerationEntity, "historyGeneration", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryTaskHistoryOwnerRelation, Source: "task", Target: "historyGeneration"},
					verify.StateIs(historyOwnerGenerationEntity, "historyGeneration", "current"),
				)),
			)),
		)),
		Source: deliveryRoutingSource("delivery.owner-generation-fencing"),
	}
}

func deliveryTaskSafeForHandoff(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(deliveryTaskEntity, variable, "acknowledged"),
		verify.StateIs(deliveryTaskEntity, variable, "retired"),
	}}
}

func deliveryRoutingSource(symbol string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/protocol/verification_delivery_routing.go", Symbol: symbol}
}
