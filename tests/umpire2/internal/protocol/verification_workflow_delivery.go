package protocol

import (
	"fmt"
	"slices"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

const (
	workflowDeliveryTarget              = "integration-workflow-delivery"
	workflowDeliveryIntentModule        = "workflow-delivery-intent"
	workflowDeliveryAuthorizationModule = "workflow-delivery-authorization"

	workflowTaskObligationRelation = "workflow-task-obligation"
	workflowTaskDeliveryRelation   = "workflow-task-delivery-task"
)

func workflowDeliveryVerification() verificationFamilyFragment {
	workflowTask := string(model.WorkflowTaskType)
	persist := foundationAction("delivery.persist.success")
	adapter := persist
	adapter.Name = "workflow.delivery.persist"
	adapter.Parameters = append([]verify.Parameter{{Name: "entity", Type: workflowTask, Binding: verify.FreshBinding}}, slices.Clone(persist.Parameters)...)
	adapter.Guard = verify.StateIs(workflowTask, "entity", model.TaskCreated)
	adapter.Effects = append([]verify.Effect{{
		Kind: verify.CreateEffect, Entity: workflowTask, Ref: "entity", State: model.TaskAdded,
	}}, slices.Clone(persist.Effects)...)
	adapter.Effects = append(adapter.Effects,
		verify.Effect{Kind: verify.AddRelationEffect, Relation: workflowTaskObligationRelation, Source: "entity", Target: "obligation"},
		verify.Effect{Kind: verify.AddRelationEffect, Relation: workflowTaskDeliveryRelation, Source: "entity", Target: "task"},
	)
	adapter.Unrealized = true
	adapter.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_workflow_delivery.go", Symbol: adapter.Name}
	resolve := foundationAction("delivery.resolve-persisted")
	resolve.Name = "workflow.delivery.resolve-persisted"
	resolve.Unrealized = true
	resolve.Source = verify.Provenance{Path: "tests/umpire2/protocol/verification_workflow_delivery.go", Symbol: resolve.Name}
	acceptAdded := featureDeliveryAuthorizationAction(
		"workflow.delivery.authorize-added", workflowTask, model.TaskAdded, model.TaskPolled,
		workflowTaskObligationRelation, workflowTaskDeliveryRelation, true,
	)
	acceptStored := featureDeliveryAuthorizationAction(
		"workflow.delivery.authorize-stored", workflowTask, model.TaskStored, model.TaskPolled,
		workflowTaskObligationRelation, workflowTaskDeliveryRelation, true,
	)
	rejectAdded := featureDeliveryAuthorizationAction(
		"workflow.delivery.reject-added", workflowTask, model.TaskAdded, model.TaskAdded,
		workflowTaskObligationRelation, workflowTaskDeliveryRelation, false,
	)
	rejectStored := featureDeliveryAuthorizationAction(
		"workflow.delivery.reject-stored", workflowTask, model.TaskStored, model.TaskStored,
		workflowTaskObligationRelation, workflowTaskDeliveryRelation, false,
	)

	property := verify.Property{
		Name: "workflow.delivery.intent-correspondence",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(workflowTask, "workflowTask", deliveryImplies(
			verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
				verify.StateIs(workflowTask, "workflowTask", model.TaskAdded),
				verify.StateIs(workflowTask, "workflowTask", model.TaskStored),
				verify.StateIs(workflowTask, "workflowTask", model.TaskPolled),
			}},
			verify.And(
				deliveryExists(workObligationEntity, "obligation", verify.Expr{
					Op: verify.RelationHasExpr, Relation: workflowTaskObligationRelation, Source: "workflowTask", Target: "obligation",
				}),
				deliveryExists(deliveryTaskEntity, "deliveryTask", verify.Expr{
					Op: verify.RelationHasExpr, Relation: workflowTaskDeliveryRelation, Source: "workflowTask", Target: "deliveryTask",
				}),
			),
		)),
		Source: verify.Provenance{Path: "tests/umpire2/rule/workflow_task_starvation.go", Symbol: "WorkflowTaskStarvationRule"},
	}
	authorizationProperty := featureAcceptedStartProperty(
		"workflow.delivery.accepted-start-correspondence", workflowTask, "workflowTask", model.TaskPolled,
		workflowTaskObligationRelation,
	)

	module := verify.Module{
		Name: workflowDeliveryIntentModule, Owner: "workflow",
		Relations:  []string{workflowTaskObligationRelation, workflowTaskDeliveryRelation},
		Actions:    []string{adapter.Name, resolve.Name},
		Properties: []string{property.Name},
		Imports:    []verify.ObligationRef{{Interface: "durable-intent", Obligation: "atomic-intent"}},
	}
	authorizationModule := verify.Module{
		Name: workflowDeliveryAuthorizationModule, Owner: "workflow",
		Actions:    []string{acceptAdded.Name, acceptStored.Name, rejectAdded.Name, rejectStored.Name},
		Properties: []string{authorizationProperty.Name},
		Imports:    []verify.ObligationRef{{Interface: "start-authorization", Obligation: "single-acceptance"}},
	}
	contractRefinement := verify.RefinementMap{
		Name: "workflow-delivery-intent", Owner: "workflow", Module: module.Name, Interface: "durable-intent",
		Actions: []verify.ActionRefinement{
			{Concrete: adapter.Name, Abstract: persist.Name},
			{Concrete: resolve.Name, Abstract: "delivery.resolve-persisted"},
		},
		Identities: []verify.IdentityRefinement{
			{Concrete: workflowTask, Abstract: workObligationEntity},
			{Concrete: workflowTask, Abstract: deliveryTaskEntity},
		},
	}
	lifecycleRefinement := verify.RefinementMap{
		Name: "workflow-delivery-lifecycle", Owner: "workflow",
		Actions: []verify.ActionRefinement{
			{Concrete: adapter.Name, Abstract: "WorkflowTask.created.add.AnyHosting"},
			{Concrete: resolve.Name, Stutter: true},
		},
	}
	authorizationRefinement := verify.RefinementMap{
		Name: "workflow-delivery-authorization", Owner: "workflow", Module: authorizationModule.Name, Interface: "start-authorization",
		Actions: []verify.ActionRefinement{
			{Concrete: acceptAdded.Name, Abstract: "delivery.authorize.accept"},
			{Concrete: acceptStored.Name, Abstract: "delivery.authorize.accept"},
			{Concrete: rejectAdded.Name, Abstract: "delivery.authorize.reject"},
			{Concrete: rejectStored.Name, Abstract: "delivery.authorize.reject"},
		},
		Identities: []verify.IdentityRefinement{
			{Concrete: workflowTask, Abstract: workObligationEntity},
			{Concrete: workflowTask, Abstract: deliveryAttemptEntity},
		},
	}
	authorizationLifecycleRefinement := verify.RefinementMap{
		Name: "workflow-delivery-authorization-lifecycle", Owner: "workflow",
		Actions: []verify.ActionRefinement{
			{Concrete: acceptAdded.Name, Abstract: "WorkflowTask.added.poll.AnyHosting"},
			{Concrete: acceptStored.Name, Abstract: "WorkflowTask.stored.poll.AnyHosting"},
			{Concrete: rejectAdded.Name, Stutter: true},
			{Concrete: rejectStored.Name, Stutter: true},
		},
	}
	composition := verify.Composition{
		Name: "workflow-delivery", Owners: []verify.CapabilityOwner{"history", "matching", "workflow"},
		Modules:        []string{"workflow", workflowDeliveryIntentModule, workflowDeliveryAuthorizationModule, "history-outbox", "matching-delivery", "history-authorization"},
		Properties:     append(foundationPropertyNames(foundationDeliveryProperties()), property.Name, authorizationProperty.Name),
		RefinementMaps: []string{contractRefinement.Name, lifecycleRefinement.Name, authorizationRefinement.Name, authorizationLifecycleRefinement.Name},
	}
	bounds := map[string]int{
		string(model.WorkflowType):    1,
		string(model.WorkflowRunType): 1,
		workflowTask:                  1,
		workObligationEntity:          2,
		deliveryTaskEntity:            2,
		deliveryAttemptEntity:         2,
		deliveryQueueEntity:           2,
		pollerEntity:                  2,
	}
	target := verify.VerificationTarget{
		Name: workflowDeliveryTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: bounds, MinimumBounds: map[string]int{workflowTask: 1, workObligationEntity: 1, deliveryTaskEntity: 1},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"ambiguous-persistence", "lost-authorization-response", "workflow-task-invalidation"},
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Relations: []verify.Relation{
				{Name: workflowTaskObligationRelation, Source: workflowTask, Target: workObligationEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
				{Name: workflowTaskDeliveryRelation, Source: workflowTask, Target: deliveryTaskEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
			},
			Actions:    []verify.Action{adapter, resolve, acceptAdded, acceptStored, rejectAdded, rejectStored},
			Properties: []verify.Property{property, authorizationProperty},
			Abstractions: []verify.Abstraction{{
				Name: adapter.Name, Reason: "the speculative Workflow Task target checks its own normal-intent adapter",
				Source: adapter.Source,
			}},
			Refinements: []verify.Refinement{
				{Name: adapter.Name, Action: adapter.Name, LifecycleActions: []string{"WorkflowTask.created.add.AnyHosting"}, Source: adapter.Source},
				{Name: resolve.Name, Action: resolve.Name, LifecycleActions: []string{"delivery.resolve-persisted"}, Source: resolve.Source},
				{Name: acceptAdded.Name, Action: acceptAdded.Name, LifecycleActions: []string{"WorkflowTask.added.poll.AnyHosting", "delivery.authorize.accept"}, Source: acceptAdded.Source},
				{Name: acceptStored.Name, Action: acceptStored.Name, LifecycleActions: []string{"WorkflowTask.stored.poll.AnyHosting", "delivery.authorize.accept"}, Source: acceptStored.Source},
				{Name: rejectAdded.Name, Action: rejectAdded.Name, LifecycleActions: []string{"delivery.authorize.reject"}, Source: rejectAdded.Source},
				{Name: rejectStored.Name, Action: rejectStored.Name, LifecycleActions: []string{"delivery.authorize.reject"}, Source: rejectStored.Source},
			},
		},
		Modules: []verify.Module{module, authorizationModule},
		Refinements: []verify.RefinementMap{
			contractRefinement, lifecycleRefinement, authorizationRefinement, authorizationLifecycleRefinement,
		},
		Compositions: []verify.Composition{composition}, Targets: []verify.VerificationTarget{target},
	}
}

func foundationAction(name string) verify.Action {
	for _, action := range foundationDeliveryActions() {
		if action.Name == name {
			return action
		}
	}
	panic(fmt.Sprintf("foundation delivery action %q is not declared", name))
}
