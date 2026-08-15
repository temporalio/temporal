package protocol

import (
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

const (
	callbackNexusTarget     = "integration-callback-nexus"
	callbackWorkflowTarget  = "integration-callback-workflow"
	callbackReferenceModule = "callback-reference"
	callbackDeliveryModule  = "callback-delivery"
	callbackLifetimeModule  = "callback-handler-lifetime"

	nexusHandlerRunRelation  = "nexus-operation-handler-run"
	callbackDeliveryRelation = "callback-delivery"
	deliveryResponseRelation = "callback-delivery-response"
	callbackDeliveryEntity   = "CallbackDelivery"
	callbackResponseEntity   = "CallbackResponse"
)

func callbackVerification(canonical verify.Model) verificationFamilyFragment {
	callback := string(model.CallbackType)
	operation := string(model.NexusOperationType)
	workflowRun := string(model.WorkflowRunType)
	attach := verify.Action{
		Name: "callback.attach-reference",
		Parameters: []verify.Parameter{
			{Name: "callback", Type: callback, Binding: verify.FreshBinding},
			{Name: "operation", Type: operation, Binding: verify.InputBinding},
			{Name: "handlerRun", Type: workflowRun, Binding: verify.InputBinding},
		},
		Guard: verify.And(
			verify.StateIs(workflowRun, "handlerRun", model.WorkflowRunStarted),
			deliveryForAll(workflowRun, "existingHandlerRun", verify.Not(verify.Expr{
				Op: verify.RelationHasExpr, Relation: nexusHandlerRunRelation, Source: "operation", Target: "existingHandlerRun",
			})),
		),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: callback, Ref: "callback", State: "unobserved"},
			{Kind: verify.AddRelationEffect, Relation: string(CallbackOperationRelation), Source: "callback", Target: "operation"},
			{Kind: verify.AddRelationEffect, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: "handlerRun"},
			{Kind: verify.AddRelationEffect, Relation: nexusHandlerRunRelation, Source: "operation", Target: "handlerRun"},
		},
		Unrealized: true,
		Source:     callbackVerificationSource("callback.attach-reference"),
	}
	attachHandler := verify.Action{
		Name: "callback.attach-handler",
		Parameters: []verify.Parameter{
			{Name: "callback", Type: callback, Binding: verify.FreshBinding},
			{Name: "handlerRun", Type: workflowRun, Binding: verify.InputBinding},
		},
		Guard: verify.StateIs(workflowRun, "handlerRun", model.WorkflowRunStarted),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: callback, Ref: "callback", State: "unobserved"},
			{Kind: verify.AddRelationEffect, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: "handlerRun"},
		},
		Unrealized: true,
		Source:     callbackVerificationSource("callback.attach-handler"),
	}
	enqueue := verify.Action{
		Name: "callback.delivery.enqueue",
		Parameters: []verify.Parameter{
			{Name: "callback", Type: callback, Binding: verify.InputBinding},
			{Name: "delivery", Type: callbackDeliveryEntity, Binding: verify.FreshBinding},
		},
		Guard: deliveryForAll(workflowRun, "handlerRun", deliveryImplies(
			verify.Expr{Op: verify.RelationHasExpr, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: "handlerRun"},
			verify.Not(workflowRunTerminal("handlerRun")),
		)),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: callbackDeliveryEntity, Ref: "delivery", State: "pending"},
			{Kind: verify.AddRelationEffect, Relation: callbackDeliveryRelation, Source: "callback", Target: "delivery"},
		},
		Unrealized: true,
		Source:     callbackVerificationSource("callback.delivery.enqueue"),
	}
	deliver := callbackDeliveryTransition("callback.delivery.deliver", "pending", "delivered")
	failPending := callbackDeliveryTransition("callback.delivery.fail-pending", "pending", "failed")
	failDelivered := callbackDeliveryTransition("callback.delivery.fail-delivered", "delivered", "failed")
	retry := callbackDeliveryTransition("callback.delivery.retry", "failed", "pending")
	acknowledge := verify.Action{
		Name: "callback.delivery.acknowledge",
		Parameters: []verify.Parameter{
			{Name: "delivery", Type: callbackDeliveryEntity, Binding: verify.InputBinding},
			{Name: "response", Type: callbackResponseEntity, Binding: verify.FreshBinding},
		},
		Guard: verify.StateIs(callbackDeliveryEntity, "delivery", "delivered"),
		Effects: []verify.Effect{
			{Kind: verify.SetStateEffect, Entity: callbackDeliveryEntity, Ref: "delivery", State: "acknowledged"},
			{Kind: verify.CreateEffect, Entity: callbackResponseEntity, Ref: "response", State: "accepted"},
			{Kind: verify.AddRelationEffect, Relation: deliveryResponseRelation, Source: "delivery", Target: "response"},
		},
		Unrealized: true,
		Source:     callbackVerificationSource("callback.delivery.acknowledge"),
	}

	handlerStart := verificationActionByName(canonical, "WorkflowRun.created.start.AnyHosting")
	handlerStart.Name = "callback.handler.start"
	handlerStart.Unrealized = true
	handlerStart.Source = callbackVerificationSource(handlerStart.Name)
	lifetimeActions := []verify.Action{attachHandler, handlerStart}
	lifetimeRefinements := []verify.ActionRefinement{{Concrete: handlerStart.Name, Abstract: "WorkflowRun.created.start.AnyHosting"}}
	refinementInventory := []verify.Refinement{{
		Name: handlerStart.Name, Action: handlerStart.Name,
		LifecycleActions: []string{"WorkflowRun.created.start.AnyHosting"}, Source: handlerStart.Source,
	}}
	for _, raw := range canonical.Actions {
		if !strings.HasPrefix(raw.Name, "WorkflowRun.started.") {
			continue
		}
		event := strings.Split(raw.Name, ".")[2]
		closeAction := raw
		closeAction.Name = "callback.handler.close." + event
		closeAction.Guard = verify.And(raw.Guard, callbackHandlerCanClose(callback, raw.Parameters[0].Name))
		closeAction.Unrealized = true
		closeAction.Source = callbackVerificationSource(closeAction.Name)
		lifetimeActions = append(lifetimeActions, closeAction)
		lifetimeRefinements = append(lifetimeRefinements, verify.ActionRefinement{Concrete: closeAction.Name, Abstract: raw.Name})
		refinementInventory = append(refinementInventory, verify.Refinement{
			Name: closeAction.Name, Action: closeAction.Name, LifecycleActions: []string{raw.Name}, Source: closeAction.Source,
		})
	}

	properties := []verify.Property{
		callbackReferenceProperty(callback, operation, workflowRun),
		callbackResponseProperty(),
		callbackLifetimeProperty(callback, workflowRun),
	}
	referenceModule := verify.Module{
		Name: callbackReferenceModule, Owner: "callback",
		Relations: []string{nexusHandlerRunRelation}, Actions: []string{attach.Name}, Properties: []string{properties[0].Name},
	}
	deliveryModule := verify.Module{
		Name: callbackDeliveryModule, Owner: "callback",
		Entities:   []string{callbackDeliveryEntity, callbackResponseEntity},
		Relations:  []string{callbackDeliveryRelation, deliveryResponseRelation},
		Actions:    []string{enqueue.Name, deliver.Name, failPending.Name, failDelivered.Name, retry.Name, acknowledge.Name},
		Properties: []string{properties[1].Name},
	}
	var lifetimeActionNames []string
	for _, action := range lifetimeActions {
		lifetimeActionNames = append(lifetimeActionNames, action.Name)
	}
	lifetimeModule := verify.Module{
		Name: callbackLifetimeModule, Owner: "workflow",
		Actions: lifetimeActionNames, Properties: []string{properties[2].Name},
	}
	lifetimeRefinement := verify.RefinementMap{
		Name: "callback-handler-lifecycle", Owner: "workflow", Actions: lifetimeRefinements,
	}
	nexusComposition := verify.Composition{
		Name: "callback-nexus", Owners: []verify.CapabilityOwner{"callback", "nexus", "workflow"},
		Modules:        []string{callbackReferenceModule, callbackDeliveryModule, callbackLifetimeModule},
		Properties:     []string{properties[0].Name, properties[1].Name, properties[2].Name},
		RefinementMaps: []string{lifetimeRefinement.Name},
	}
	workflowComposition := verify.Composition{
		Name: "callback-workflow", Owners: []verify.CapabilityOwner{"callback", "workflow"},
		Modules:        []string{callbackDeliveryModule, callbackLifetimeModule},
		Properties:     []string{properties[1].Name, properties[2].Name},
		RefinementMaps: []string{lifetimeRefinement.Name},
	}
	commonBounds := map[string]int{
		callback: 2, workflowRun: 2, callbackDeliveryEntity: 2, callbackResponseEntity: 2,
	}
	nexusBounds := cloneBounds(commonBounds)
	nexusBounds[operation] = 2
	nexusTarget := verify.VerificationTarget{
		Name: callbackNexusTarget, Owners: slices.Clone(nexusComposition.Owners),
		Modules: slices.Clone(nexusComposition.Modules), Compositions: []string{nexusComposition.Name},
		Bounds: nexusBounds, MinimumBounds: cloneBounds(nexusBounds),
		BackendRequirements: []string{"fizz", "ivy", "tla"},
		FailurePolicy:       []string{"callback-retry", "conflicting-response", "wrong-reference"},
		Abstractions:        []string{"regression.nexus.start_activity"},
	}
	workflowTarget := verify.VerificationTarget{
		Name: callbackWorkflowTarget, Owners: slices.Clone(workflowComposition.Owners),
		Modules: slices.Clone(workflowComposition.Modules), Compositions: []string{workflowComposition.Name},
		Bounds: cloneBounds(commonBounds), MinimumBounds: cloneBounds(commonBounds),
		BackendRequirements: []string{"fizz", "ivy", "tla"},
		FailurePolicy:       []string{"callback-retry", "handler-close-race"},
		Omissions: []verify.Abstraction{{
			Name: attach.Name, Reason: "the Nexus integration target checks operation-reference attachment", Source: attach.Source,
		}},
	}
	actions := []verify.Action{attach, enqueue, deliver, failPending, failDelivered, retry, acknowledge}
	actions = append(actions, lifetimeActions...)
	refinedActions := make(map[string]struct{}, len(refinementInventory))
	for _, refinement := range refinementInventory {
		refinedActions[refinement.Action] = struct{}{}
	}
	for _, action := range actions {
		if _, refined := refinedActions[action.Name]; refined {
			continue
		}
		refinementInventory = append(refinementInventory, verify.Refinement{
			Name: action.Name, Action: action.Name, LifecycleActions: []string{action.Name}, Source: action.Source,
		})
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Entities: []verify.EntityType{
				{Name: callbackDeliveryEntity, IDs: foundationIDs(callbackDeliveryEntity, 2), Initial: "pending", States: []verify.State{{Name: "pending"}, {Name: "delivered"}, {Name: "failed"}, {Name: "acknowledged", Terminal: true}}, Source: callbackVerificationSource(callbackDeliveryEntity)},
				{Name: callbackResponseEntity, IDs: foundationIDs(callbackResponseEntity, 2), Initial: "unobserved", States: []verify.State{{Name: "unobserved"}, {Name: "accepted"}, {Name: "conflicting"}}, Source: callbackVerificationSource(callbackResponseEntity)},
			},
			Relations: []verify.Relation{
				{Name: nexusHandlerRunRelation, Source: operation, Target: workflowRun, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: callbackDeliveryRelation, Source: callback, Target: callbackDeliveryEntity, SourceCardinality: verify.Many, TargetCardinality: verify.One},
				{Name: deliveryResponseRelation, Source: callbackDeliveryEntity, Target: callbackResponseEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
			},
			Actions: actions, Properties: properties, Refinements: refinementInventory,
		},
		Modules:      []verify.Module{referenceModule, deliveryModule, lifetimeModule},
		Refinements:  []verify.RefinementMap{lifetimeRefinement},
		Compositions: []verify.Composition{nexusComposition, workflowComposition},
		Targets:      []verify.VerificationTarget{nexusTarget, workflowTarget},
	}
}

func callbackDeliveryTransition(name, from, to string) verify.Action {
	return verify.Action{
		Name:       name,
		Parameters: []verify.Parameter{{Name: "delivery", Type: callbackDeliveryEntity, Binding: verify.InputBinding}},
		Guard:      verify.StateIs(callbackDeliveryEntity, "delivery", from),
		Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: callbackDeliveryEntity, Ref: "delivery", State: to}},
		Unrealized: true,
		Source:     callbackVerificationSource(name),
	}
}

func callbackReferenceProperty(callback, operation, workflowRun string) verify.Property {
	return verify.Property{
		Name: "CallbackReferenceConsistency",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(callback, "callback", deliveryForAll(operation, "operation", deliveryForAll(workflowRun, "handlerRun", deliveryImplies(
			verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: string(CallbackOperationRelation), Source: "callback", Target: "operation"},
				verify.Expr{Op: verify.RelationHasExpr, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: "handlerRun"},
			),
			verify.Expr{Op: verify.RelationHasExpr, Relation: nexusHandlerRunRelation, Source: "operation", Target: "handlerRun"},
		)))),
		Source: verify.Provenance{Path: "tests/umpire2/rule/callback_reference_consistency.go", Symbol: "CallbackReferenceConsistencyRule"},
	}
}

func callbackResponseProperty() verify.Property {
	return verify.Property{
		Name: "CallbackResponseConsistency",
		Kind: verify.SafetyProperty,
		Expr: verify.And(
			deliveryForAll(callbackDeliveryEntity, "delivery", deliveryImplies(
				verify.StateIs(callbackDeliveryEntity, "delivery", "acknowledged"),
				deliveryExists(callbackResponseEntity, "response", verify.And(
					verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryResponseRelation, Source: "delivery", Target: "response"},
					verify.StateIs(callbackResponseEntity, "response", "accepted"),
				)),
			)),
			deliveryForAll(callbackDeliveryEntity, "delivery", deliveryForAll(callbackResponseEntity, "response", deliveryImplies(
				verify.Expr{Op: verify.RelationHasExpr, Relation: deliveryResponseRelation, Source: "delivery", Target: "response"},
				verify.And(
					verify.StateIs(callbackDeliveryEntity, "delivery", "acknowledged"),
					verify.StateIs(callbackResponseEntity, "response", "accepted"),
				),
			))),
		),
		Source: verify.Provenance{Path: "tests/umpire2/rule/callback_response_consistency.go", Symbol: "CallbackResponseConsistencyRule"},
	}
}

func callbackLifetimeProperty(callback, workflowRun string) verify.Property {
	return verify.Property{
		Name: "CallbackHandlerLifetime",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(workflowRun, "handlerRun", deliveryImplies(
			workflowRunTerminal("handlerRun"),
			deliveryForAll(callback, "callback", deliveryImplies(
				verify.Expr{Op: verify.RelationHasExpr, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: "handlerRun"},
				deliveryForAll(callbackDeliveryEntity, "delivery", deliveryImplies(
					verify.Expr{Op: verify.RelationHasExpr, Relation: callbackDeliveryRelation, Source: "callback", Target: "delivery"},
					verify.StateIs(callbackDeliveryEntity, "delivery", "acknowledged"),
				)),
			)),
		)),
		Source: callbackVerificationSource("CallbackHandlerLifetime"),
	}
}

func callbackHandlerCanClose(callback, handlerRun string) verify.Expr {
	return deliveryForAll(callback, "callback", deliveryImplies(
		verify.Expr{Op: verify.RelationHasExpr, Relation: string(CallbackHandlerRunRelation), Source: "callback", Target: handlerRun},
		deliveryForAll(callbackDeliveryEntity, "delivery", deliveryImplies(
			verify.Expr{Op: verify.RelationHasExpr, Relation: callbackDeliveryRelation, Source: "callback", Target: "delivery"},
			verify.StateIs(callbackDeliveryEntity, "delivery", "acknowledged"),
		)),
	))
}

func workflowRunTerminal(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunCompleted),
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunFailed),
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunCanceled),
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunTerminated),
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunTimedOut),
		verify.StateIs(string(model.WorkflowRunType), variable, model.WorkflowRunContinuedAsNew),
	}}
}

func cloneBounds(bounds map[string]int) map[string]int {
	result := make(map[string]int, len(bounds))
	for name, bound := range bounds {
		result[name] = bound
	}
	return result
}

func callbackVerificationSource(symbol string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/protocol/verification_callback.go", Symbol: symbol}
}
