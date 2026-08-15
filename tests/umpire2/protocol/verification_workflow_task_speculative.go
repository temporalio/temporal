package protocol

import (
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

const (
	workflowTaskSpeculativeTarget = "feature-workflow-speculative-delivery"
	workflowTaskNormalModule      = "workflow-task-normal-intent"
	workflowTaskSpeculativeModule = "workflow-task-speculative-delivery"

	workflowTaskNormalRunRelation      = "workflow-task-normal-run"
	workflowTaskSpeculativeRunRelation = "workflow-task-speculative-run"
)

func workflowTaskSpeculativeVerification() verificationFamilyFragment {
	workflowRun := string(model.WorkflowRunType)
	workflowTask := string(model.WorkflowTaskType)
	persist := foundationAction("delivery.persist.success")
	normal := persist
	normal.Name = "workflow.task.create-normal"
	normal.Parameters = append([]verify.Parameter{
		{Name: "run", Type: workflowRun, Binding: verify.FreshBinding},
		{Name: "entity", Type: workflowTask, Binding: verify.FreshBinding},
	}, slices.Clone(persist.Parameters)...)
	normal.Guard = verify.And(
		verify.StateIs(workflowRun, "run", model.WorkflowRunCreated),
		verify.StateIs(workflowTask, "entity", model.TaskCreated),
	)
	normal.Effects = []verify.Effect{
		{Kind: verify.CreateEffect, Entity: workflowRun, Ref: "run", State: model.WorkflowRunStarted},
		{Kind: verify.CreateEffect, Entity: workflowTask, Ref: "entity", State: model.TaskAdded},
	}
	normal.Effects = append(normal.Effects, slices.Clone(persist.Effects)...)
	normal.Effects = append(normal.Effects,
		verify.Effect{Kind: verify.AddRelationEffect, Relation: workflowTaskObligationRelation, Source: "entity", Target: "obligation"},
		verify.Effect{Kind: verify.AddRelationEffect, Relation: workflowTaskDeliveryRelation, Source: "entity", Target: "task"},
		verify.Effect{Kind: verify.AddRelationEffect, Relation: workflowTaskNormalRunRelation, Source: "entity", Target: "run"},
	)
	normal.Unrealized = true
	normal.Source = workflowTaskSpeculativeSource(normal.Name)
	normalResolve := foundationAction("delivery.resolve-persisted")
	normalResolve.Name = "workflow.task.resolve-normal"
	normalResolve.Source = workflowTaskSpeculativeSource(normalResolve.Name)

	direct := verify.Action{
		Name: "workflow.task.create-speculative-direct",
		Parameters: []verify.Parameter{
			{Name: "run", Type: workflowRun, Binding: verify.InputBinding},
			{Name: "entity", Type: workflowTask, Binding: verify.FreshBinding},
			{Name: "obligation", Type: workObligationEntity, Binding: verify.FreshBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.FreshBinding},
			{Name: "queue", Type: deliveryQueueEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(workflowRun, "run", model.WorkflowRunStarted),
			verify.StateIs(workflowTask, "entity", model.TaskCreated),
			verify.Not(deliveryExists(workflowTask, "normalTask", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskNormalRunRelation, Source: "normalTask", Target: "run"},
				workflowTaskPending("normalTask"),
			))),
		),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: workflowTask, Ref: "entity", State: model.TaskAdded},
			{Kind: verify.CreateEffect, Entity: workObligationEntity, Ref: "obligation", State: "valid"},
			{Kind: verify.CreateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "sync-offered"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskObligationRelation, Source: "task", Target: "obligation"},
			{Kind: verify.AddRelationEffect, Relation: deliveryTaskQueueRelation, Source: "task", Target: "queue"},
			{Kind: verify.AddRelationEffect, Relation: workflowTaskObligationRelation, Source: "entity", Target: "obligation"},
			{Kind: verify.AddRelationEffect, Relation: workflowTaskDeliveryRelation, Source: "entity", Target: "task"},
			{Kind: verify.AddRelationEffect, Relation: workflowTaskSpeculativeRunRelation, Source: "entity", Target: "run"},
		},
		Unrealized: true,
		Source:     workflowTaskSpeculativeSource("workflow.task.create-speculative-direct"),
	}

	fallback := workflowTaskFallbackAction(workflowTask)
	cancelAdded := workflowTaskCancelAction("workflow.task.cancel-speculative-added", workflowTask, model.TaskAdded, "sync-offered")
	cancelStored := workflowTaskCancelAction("workflow.task.cancel-speculative-stored", workflowTask, model.TaskStored, "backlogged")
	properties := []verify.Property{
		speculativeTaskCreationProperty(workflowRun, workflowTask),
		workflowTaskStarvationProperty(workflowRun, workflowTask),
	}

	normalModule := verify.Module{
		Name: workflowTaskNormalModule, Owner: "workflow",
		Relations: []string{workflowTaskNormalRunRelation}, Actions: []string{normal.Name, normalResolve.Name},
		Imports: []verify.ObligationRef{{Interface: "durable-intent", Obligation: "atomic-intent"}},
	}
	speculativeModule := verify.Module{
		Name: workflowTaskSpeculativeModule, Owner: "workflow",
		Relations:  []string{workflowTaskSpeculativeRunRelation},
		Actions:    []string{direct.Name, fallback.Name, cancelAdded.Name, cancelStored.Name},
		Properties: []string{properties[0].Name, properties[1].Name},
	}
	normalContract := verify.RefinementMap{
		Name: "workflow-task-normal-intent", Owner: "workflow", Module: normalModule.Name, Interface: "durable-intent",
		Actions: []verify.ActionRefinement{
			{Concrete: normal.Name, Abstract: persist.Name},
			{Concrete: normalResolve.Name, Abstract: "delivery.resolve-persisted"},
		},
		Identities: []verify.IdentityRefinement{
			{Concrete: workflowTask, Abstract: workObligationEntity},
			{Concrete: workflowTask, Abstract: deliveryTaskEntity},
		},
	}
	workflowIntentRefinement := verify.RefinementMap{
		Name: "workflow-task-normal-feature-intent", Owner: "workflow",
		Actions: []verify.ActionRefinement{{Concrete: normal.Name, Abstract: "workflow.delivery.persist"}},
	}
	lifecycleRefinement := verify.RefinementMap{
		Name: "workflow-task-speculative-lifecycle", Owner: "workflow",
		Actions: []verify.ActionRefinement{
			{Concrete: normal.Name, Abstract: "WorkflowTask.created.add.AnyHosting"},
			{Concrete: normalResolve.Name, Stutter: true},
			{Concrete: direct.Name, Abstract: "WorkflowTask.created.add.AnyHosting"},
			{Concrete: fallback.Name, Abstract: "WorkflowTask.added.store.AnyHosting"},
			{Concrete: cancelAdded.Name, Abstract: "WorkflowTask.added.discard.AnyHosting"},
			{Concrete: cancelStored.Name, Abstract: "WorkflowTask.stored.discard.AnyHosting"},
		},
	}
	runRefinement := verify.RefinementMap{
		Name: "workflow-task-normal-run-lifecycle", Owner: "workflow",
		Actions: []verify.ActionRefinement{{
			Concrete: normal.Name, Abstract: "WorkflowRun.created.start.AnyHosting",
			Parameters: []verify.ParameterRefinement{{Concrete: "run", Abstract: "entity"}},
		}},
	}
	foundationRefinement := verify.RefinementMap{
		Name: "workflow-task-speculative-foundation", Owner: "workflow",
		Actions: []verify.ActionRefinement{
			{Concrete: fallback.Name, Abstract: "delivery.spool"},
			{Concrete: cancelAdded.Name, Abstract: "delivery.expire"},
			{Concrete: cancelStored.Name, Abstract: "delivery.expire"},
		},
	}
	composition := verify.Composition{
		Name: "workflow-task-speculative-delivery", Owners: []verify.CapabilityOwner{"history", "matching", "workflow"},
		Modules: []string{
			workflowTaskNormalModule, workflowTaskSpeculativeModule, workflowDeliveryAuthorizationModule,
			"history-outbox", "matching-delivery", "history-authorization",
		},
		Properties: append(
			foundationPropertyNames(foundationDeliveryProperties()),
			"workflow.delivery.accepted-start-correspondence",
			properties[0].Name,
			properties[1].Name,
		),
		RefinementMaps: []string{
			normalContract.Name, workflowIntentRefinement.Name, lifecycleRefinement.Name, runRefinement.Name, foundationRefinement.Name,
		},
	}
	target := verify.VerificationTarget{
		Name: workflowTaskSpeculativeTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: map[string]int{
			workflowRun: 1, workflowTask: 2, workObligationEntity: 2, deliveryTaskEntity: 2,
			deliveryAttemptEntity: 2, deliveryQueueEntity: 2, pollerEntity: 2,
		},
		MinimumBounds:       map[string]int{workflowRun: 1, workflowTask: 2, workObligationEntity: 2, deliveryTaskEntity: 2},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"speculative-fallback", "workflow-task-invalidation", "duplicate-start"},
		Abstractions:        []string{"workflow.delivery.persist"},
	}
	actions := []verify.Action{normal, normalResolve, direct, fallback, cancelAdded, cancelStored}
	return verificationFamilyFragment{
		Model: verify.Model{
			Relations: []verify.Relation{
				{Name: workflowTaskNormalRunRelation, Source: workflowTask, Target: workflowRun, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: workflowTaskSpeculativeRunRelation, Source: workflowTask, Target: workflowRun, SourceCardinality: verify.One, TargetCardinality: verify.Many},
			},
			Actions: actions, Properties: properties,
			Abstractions: []verify.Abstraction{{
				Name: direct.Name, Reason: "the speculative target checks the atomic direct-delivery adapter",
				Source: direct.Source,
			}},
			Refinements: []verify.Refinement{
				{Name: normal.Name, Action: normal.Name, LifecycleActions: []string{"WorkflowRun.created.start.AnyHosting", "WorkflowTask.created.add.AnyHosting", "delivery.persist.success"}, Source: normal.Source},
				{Name: normalResolve.Name, Action: normalResolve.Name, LifecycleActions: []string{"delivery.resolve-persisted"}, Source: normalResolve.Source},
				{Name: direct.Name, Action: direct.Name, LifecycleActions: []string{"WorkflowTask.created.add.AnyHosting", "delivery.offer-sync"}, Source: direct.Source},
				{Name: fallback.Name, Action: fallback.Name, LifecycleActions: []string{"WorkflowTask.added.store.AnyHosting", "delivery.spool"}, Source: fallback.Source},
				{Name: cancelAdded.Name, Action: cancelAdded.Name, LifecycleActions: []string{"WorkflowTask.added.discard.AnyHosting", "delivery.expire"}, Source: cancelAdded.Source},
				{Name: cancelStored.Name, Action: cancelStored.Name, LifecycleActions: []string{"WorkflowTask.stored.discard.AnyHosting", "delivery.expire"}, Source: cancelStored.Source},
			},
		},
		Modules: []verify.Module{normalModule, speculativeModule},
		Refinements: []verify.RefinementMap{
			normalContract, workflowIntentRefinement, lifecycleRefinement, runRefinement, foundationRefinement,
		},
		Compositions: []verify.Composition{composition}, Targets: []verify.VerificationTarget{target},
	}
}

func workflowTaskFallbackAction(workflowTask string) verify.Action {
	spool := foundationAction("delivery.spool")
	return verify.Action{
		Name: "workflow.task.speculative-fallback",
		Parameters: []verify.Parameter{
			{Name: "entity", Type: workflowTask, Binding: verify.InputBinding},
			{Name: "task", Type: deliveryTaskEntity, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(workflowTask, "entity", model.TaskAdded),
			verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskDeliveryRelation, Source: "entity", Target: "task"},
			verify.StateIs(deliveryTaskEntity, "task", "sync-offered"),
			spool.Guard,
		),
		Effects: []verify.Effect{
			{Kind: verify.SetStateEffect, Entity: workflowTask, Ref: "entity", State: model.TaskStored},
			{Kind: verify.SetStateEffect, Entity: deliveryTaskEntity, Ref: "task", State: "backlogged"},
		},
		Unrealized: true,
		Source:     workflowTaskSpeculativeSource("workflow.task.speculative-fallback"),
	}
}

func workflowTaskCancelAction(name, workflowTask, taskState, deliveryState string) verify.Action {
	expire := foundationAction("delivery.expire")
	return verify.Action{
		Name:       name,
		Parameters: append([]verify.Parameter{{Name: "entity", Type: workflowTask, Binding: verify.InputBinding}}, slices.Clone(expire.Parameters)...),
		Guard: verify.And(
			verify.StateIs(workflowTask, "entity", taskState),
			verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskObligationRelation, Source: "entity", Target: "obligation"},
			verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskDeliveryRelation, Source: "entity", Target: "task"},
			verify.StateIs(deliveryTaskEntity, "task", deliveryState),
			expire.Guard,
		),
		Effects: append([]verify.Effect{{
			Kind: verify.SetStateEffect, Entity: workflowTask, Ref: "entity", State: model.TaskDiscarded,
		}}, slices.Clone(expire.Effects)...),
		Unrealized: true,
		Source:     workflowTaskSpeculativeSource(name),
	}
}

func speculativeTaskCreationProperty(workflowRun, workflowTask string) verify.Property {
	return verify.Property{
		Name: "SpeculativeTaskCreation",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(workflowRun, "run", verify.Not(
			deliveryExists(workflowTask, "normalTask", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskNormalRunRelation, Source: "normalTask", Target: "run"},
				workflowTaskPending("normalTask"),
				deliveryExists(workflowTask, "speculativeTask", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskSpeculativeRunRelation, Source: "speculativeTask", Target: "run"},
					workflowTaskPending("speculativeTask"),
				)),
			)),
		)),
		Source: workflowTaskRuleSource("SpeculativeTaskCreationRule", "speculative_task_creation.go"),
	}
}

func workflowTaskStarvationProperty(workflowRun, workflowTask string) verify.Property {
	return verify.Property{
		Name: "WorkflowTaskStarvation",
		Kind: verify.ProgressProperty,
		Expr: deliveryForAll(workflowTask, "workflowTask", deliveryForAll(workflowRun, "run", deliveryImplies(
			verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: workflowTaskNormalRunRelation, Source: "workflowTask", Target: "run"},
				verify.StateIs(workflowRun, "run", model.WorkflowRunStarted),
			),
			verify.Not(workflowTaskPending("workflowTask")),
		))),
		Fairness: []string{"compatible-poller-and-run-close-remain-schedulable"},
		Source:   workflowTaskRuleSource("WorkflowTaskStarvationRule", "workflow_task_starvation.go"),
	}
}

func workflowTaskPending(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(string(model.WorkflowTaskType), variable, model.TaskAdded),
		verify.StateIs(string(model.WorkflowTaskType), variable, model.TaskStored),
	}}
}

func workflowTaskSpeculativeSource(symbol string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/protocol/verification_workflow_task_speculative.go", Symbol: symbol}
}

func workflowTaskRuleSource(symbol, file string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/rule/" + file, Symbol: symbol}
}
