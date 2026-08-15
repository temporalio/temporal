package protocol

import (
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
)

const (
	deliveryBacklogTarget = "foundation-backlog-ack"
	deliveryBacklogModule = "matching-backlog"

	backlogPositionEntity           = "BacklogPosition"
	backlogPositionTaskRelation     = "backlog-position-task"
	backlogPositionPrecedesRelation = "backlog-position-precedes"
)

func deliveryBacklogVerification() verificationFamilyFragment {
	appendFirst := verify.Action{
		Name: "backlog.append-first",
		Parameters: []verify.Parameter{
			{Name: "position", Type: backlogPositionEntity, Binding: verify.FreshBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
		},
		Guard: verify.StateIs(deliveryTaskEntity, "task", "backlogged"),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: backlogPositionEntity, Ref: "position", State: "unread"},
			{Kind: verify.AddRelationEffect, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
		},
		Unrealized: true,
		Source:     deliveryBacklogSource("backlog.append-first"),
	}
	appendAfter := verify.Action{
		Name: "backlog.append-after",
		Parameters: []verify.Parameter{
			{Name: "position", Type: backlogPositionEntity, Binding: verify.FreshBinding},
			{Name: "previous", Type: backlogPositionEntity, Binding: verify.InputBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(deliveryTaskEntity, "task", "backlogged"),
			backlogPositionExists("previous"),
		),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: backlogPositionEntity, Ref: "position", State: "unread"},
			{Kind: verify.AddRelationEffect, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
			{Kind: verify.AddRelationEffect, Relation: backlogPositionPrecedesRelation, Source: "previous", Target: "position"},
		},
		Unrealized: true,
		Source:     deliveryBacklogSource("backlog.append-after"),
	}
	read := backlogPositionTransition("backlog.read", "unread", "read")
	acknowledge := verify.Action{
		Name: "backlog.ack",
		Parameters: []verify.Parameter{
			{Name: "position", Type: backlogPositionEntity, Binding: verify.InputBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(backlogPositionEntity, "position", "read"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(deliveryTaskEntity, "task", "acknowledged"),
				verify.StateIs(deliveryTaskEntity, "task", "retired"),
			}},
			backlogPredecessorsSettled("position"),
		),
		Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: backlogPositionEntity, Ref: "position", State: "acked"}},
		Unrealized: true,
		Source:     deliveryBacklogSource("backlog.ack"),
	}
	collect := verify.Action{
		Name: "backlog.gc",
		Parameters: []verify.Parameter{
			{Name: "position", Type: backlogPositionEntity, Binding: verify.InputBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(backlogPositionEntity, "position", "acked"),
			verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
			verify.StateIs(deliveryTaskEntity, "task", "retired"),
		),
		Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: backlogPositionEntity, Ref: "position", State: "gc"}},
		Unrealized: true,
		Source:     deliveryBacklogSource("backlog.gc"),
	}
	actions := []verify.Action{appendFirst, appendAfter, read, acknowledge, collect}
	properties := []verify.Property{
		backlogAcknowledgementProperty(),
		backlogGarbageCollectionProperty(),
		backlogPrefixProperty(),
		backlogProgressProperty(),
	}
	module := verify.Module{
		Name: deliveryBacklogModule, Owner: "matching",
		Entities:   []string{backlogPositionEntity},
		Relations:  []string{backlogPositionTaskRelation, backlogPositionPrecedesRelation},
		Actions:    []string{appendFirst.Name, appendAfter.Name, read.Name, acknowledge.Name, collect.Name},
		Properties: []string{properties[0].Name, properties[1].Name, properties[2].Name, properties[3].Name},
	}
	composition := verify.Composition{
		Name: "foundation-backlog", Owners: []verify.CapabilityOwner{"history", "matching"},
		Modules:    []string{"history-outbox", "matching-delivery", "history-authorization", deliveryBacklogModule},
		Properties: append(foundationPropertyNames(foundationDeliveryProperties()), properties[0].Name, properties[1].Name, properties[2].Name, properties[3].Name),
	}
	target := verify.VerificationTarget{
		Name: deliveryBacklogTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: map[string]int{
			workObligationEntity: 2, deliveryTaskEntity: 2, deliveryAttemptEntity: 2,
			deliveryQueueEntity: 2, pollerEntity: 2, backlogPositionEntity: 2,
		},
		MinimumBounds:       map[string]int{deliveryTaskEntity: 2, backlogPositionEntity: 2},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"premature-ack", "backlog-gc", "reader-fairness"},
		Omissions: []verify.Abstraction{{
			Name: "workflow.task.create-speculative-direct", Reason: "checked by the speculative Workflow Task target",
			Source: workflowTaskSpeculativeSource("workflow.task.create-speculative-direct"),
		}},
	}
	refinements := make([]verify.Refinement, len(actions))
	for index, action := range actions {
		refinements[index] = verify.Refinement{Name: action.Name, Action: action.Name, LifecycleActions: []string{action.Name}, Source: action.Source}
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Entities: []verify.EntityType{{
				Name: backlogPositionEntity, IDs: foundationIDs(backlogPositionEntity, 2), Initial: "unused",
				States: []verify.State{{Name: "unused"}, {Name: "unread"}, {Name: "read"}, {Name: "acked"}, {Name: "gc", Terminal: true}},
				Source: deliveryBacklogSource(backlogPositionEntity),
			}},
			Relations: []verify.Relation{
				{Name: backlogPositionTaskRelation, Source: backlogPositionEntity, Target: deliveryTaskEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
				{Name: backlogPositionPrecedesRelation, Source: backlogPositionEntity, Target: backlogPositionEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
			},
			Actions: actions, Properties: properties, Refinements: refinements,
		},
		Modules: []verify.Module{module}, Compositions: []verify.Composition{composition}, Targets: []verify.VerificationTarget{target},
	}
}

func backlogPositionTransition(name, from, to string) verify.Action {
	return verify.Action{
		Name:       name,
		Parameters: []verify.Parameter{{Name: "position", Type: backlogPositionEntity, Binding: verify.InputBinding}},
		Guard:      verify.StateIs(backlogPositionEntity, "position", from),
		Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: backlogPositionEntity, Ref: "position", State: to}},
		Unrealized: true,
		Source:     deliveryBacklogSource(name),
	}
}

func backlogPositionExists(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(backlogPositionEntity, variable, "unread"),
		verify.StateIs(backlogPositionEntity, variable, "read"),
		verify.StateIs(backlogPositionEntity, variable, "acked"),
		verify.StateIs(backlogPositionEntity, variable, "gc"),
	}}
}

func backlogPredecessorsSettled(position string) verify.Expr {
	return deliveryForAll(backlogPositionEntity, "previous", deliveryImplies(
		verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionPrecedesRelation, Source: "previous", Target: position},
		verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
			verify.StateIs(backlogPositionEntity, "previous", "acked"),
			verify.StateIs(backlogPositionEntity, "previous", "gc"),
		}},
	))
}

func backlogAcknowledgementProperty() verify.Property {
	return verify.Property{
		Name: "backlog.ack-after-dispatch",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(backlogPositionEntity, "position", deliveryImplies(
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(backlogPositionEntity, "position", "acked"),
				verify.StateIs(backlogPositionEntity, "position", "gc"),
			}},
			deliveryExists(deliveryTaskEntity, "task", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
				verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
					verify.StateIs(deliveryTaskEntity, "task", "acknowledged"),
					verify.StateIs(deliveryTaskEntity, "task", "retired"),
				}},
			)),
		)),
		Source: deliveryBacklogSource("backlog.ack-after-dispatch"),
	}
}

func backlogGarbageCollectionProperty() verify.Property {
	return verify.Property{
		Name: "backlog.gc-after-retirement",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(backlogPositionEntity, "position", deliveryImplies(
			verify.StateIs(backlogPositionEntity, "position", "gc"),
			deliveryExists(deliveryTaskEntity, "task", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionTaskRelation, Source: "position", Target: "task"},
				verify.StateIs(deliveryTaskEntity, "task", "retired"),
			)),
		)),
		Source: deliveryBacklogSource("backlog.gc-after-retirement"),
	}
}

func backlogPrefixProperty() verify.Property {
	return verify.Property{
		Name: "backlog.ack-prefix",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(backlogPositionEntity, "previous", deliveryForAll(backlogPositionEntity, "position", deliveryImplies(
			verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: backlogPositionPrecedesRelation, Source: "previous", Target: "position"},
				verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
					verify.StateIs(backlogPositionEntity, "position", "acked"),
					verify.StateIs(backlogPositionEntity, "position", "gc"),
				}},
			),
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(backlogPositionEntity, "previous", "acked"),
				verify.StateIs(backlogPositionEntity, "previous", "gc"),
			}},
		))),
		Source: deliveryBacklogSource("backlog.ack-prefix"),
	}
}

func backlogProgressProperty() verify.Property {
	return verify.Property{
		Name: "backlog.reader-progress",
		Kind: verify.ProgressProperty,
		Expr: deliveryForAll(backlogPositionEntity, "position", verify.Not(verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
			verify.StateIs(backlogPositionEntity, "position", "unread"),
			verify.StateIs(backlogPositionEntity, "position", "read"),
		}})),
		Fairness: []string{"compatible-reader-and-dispatch-remain-schedulable"},
		Source:   deliveryBacklogSource("backlog.reader-progress"),
	}
}

func deliveryBacklogSource(symbol string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/protocol/verification_delivery_backlog.go", Symbol: symbol}
}
