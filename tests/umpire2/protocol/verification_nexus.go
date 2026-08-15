package protocol

import (
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

const (
	nexusFeatureTarget             = "feature-nexus"
	nexusActivityIntegrationTarget = "integration-nexus-activity"
	nexusClosureModule             = "nexus-workflow-closure"
	nexusTimeoutModule             = "nexus-timeout-semantics"

	nexusOperationWorkflowRelation = "nexus-operation-workflow"
	nexusTimeoutEvidenceRelation   = "nexus-timeout-evidence"
	nexusTimeoutEvidenceEntity     = "NexusTimeoutEvidence"
)

func nexusVerification(canonical verify.Model) verificationFamilyFragment {
	workflow := string(model.WorkflowType)
	operation := string(model.NexusOperationType)
	workflowStart := verificationActionByName(canonical, "Workflow.created.start.Standalone")
	start := workflowStart
	start.Name = "nexus.workflow.start"
	start.Unrealized = true
	start.Source = nexusVerificationSource(start.Name)

	var actions []verify.Action
	var lifecycleRefinements []verify.ActionRefinement
	var refinementInventory []verify.Refinement
	actions = append(actions, start)
	lifecycleRefinements = append(lifecycleRefinements, verify.ActionRefinement{Concrete: start.Name, Abstract: workflowStart.Name})
	refinementInventory = append(refinementInventory, verify.Refinement{
		Name: start.Name, Action: start.Name, LifecycleActions: []string{workflowStart.Name}, Source: start.Source,
	})

	for _, raw := range canonical.Actions {
		if strings.HasPrefix(raw.Name, "NexusOperation.unspecified.schedule.") {
			hosting := raw.Name[strings.LastIndexByte(raw.Name, '.')+1:]
			schedule := raw
			schedule.Name = "nexus.operation.schedule." + hosting
			schedule.Parameters = append(slices.Clone(raw.Parameters), verify.Parameter{Name: "workflow", Type: workflow, Binding: verify.InputBinding})
			schedule.Guard = verify.And(
				verify.StateIs(workflow, "workflow", model.WorkflowStarted),
				raw.Guard,
			)
			schedule.Effects = append(slices.Clone(raw.Effects), verify.Effect{
				Kind: verify.AddRelationEffect, Relation: nexusOperationWorkflowRelation, Source: raw.Parameters[0].Name, Target: "workflow",
			})
			schedule.Unrealized = true
			schedule.Source = nexusVerificationSource(schedule.Name)
			actions = append(actions, schedule)
			lifecycleRefinements = append(lifecycleRefinements, verify.ActionRefinement{Concrete: schedule.Name, Abstract: raw.Name})
			refinementInventory = append(refinementInventory, verify.Refinement{
				Name: schedule.Name, Action: schedule.Name, LifecycleActions: []string{raw.Name}, Source: schedule.Source,
			})
		}
	}

	for _, raw := range canonical.Actions {
		if !strings.HasPrefix(raw.Name, "Workflow.started.") {
			continue
		}
		event := strings.Split(raw.Name, ".")[2]
		closeAction := raw
		closeAction.Name = "nexus.workflow.close." + event
		closeAction.Guard = verify.And(raw.Guard, deliveryForAll(operation, "operation", deliveryImplies(
			verify.Expr{Op: verify.RelationHasExpr, Relation: nexusOperationWorkflowRelation, Source: "operation", Target: raw.Parameters[0].Name},
			nexusOperationTerminal("operation"),
		)))
		closeAction.Unrealized = true
		closeAction.Source = nexusVerificationSource(closeAction.Name)
		actions = append(actions, closeAction)
		lifecycleRefinements = append(lifecycleRefinements, verify.ActionRefinement{Concrete: closeAction.Name, Abstract: raw.Name})
		refinementInventory = append(refinementInventory, verify.Refinement{
			Name: closeAction.Name, Action: closeAction.Name, LifecycleActions: []string{raw.Name}, Source: closeAction.Source,
		})
	}

	var timeoutActions []string
	var timeoutRefinements []verify.ActionRefinement
	for _, raw := range canonical.Actions {
		if !strings.HasPrefix(raw.Name, "NexusOperation.") || !strings.Contains(raw.Name, ".timeout.") {
			continue
		}
		parts := strings.Split(raw.Name, ".")
		timeout := raw
		timeout.Name = "nexus.timeout." + parts[1] + "." + parts[3]
		timeout.Parameters = append(slices.Clone(raw.Parameters), verify.Parameter{
			Name: "timeoutEvidence", Type: nexusTimeoutEvidenceEntity, Binding: verify.FreshBinding,
		})
		timeout.Effects = append(slices.Clone(raw.Effects),
			verify.Effect{Kind: verify.CreateEffect, Entity: nexusTimeoutEvidenceEntity, Ref: "timeoutEvidence", State: "valid"},
			verify.Effect{Kind: verify.AddRelationEffect, Relation: nexusTimeoutEvidenceRelation, Source: raw.Parameters[0].Name, Target: "timeoutEvidence"},
		)
		timeout.Unrealized = true
		timeout.Source = nexusVerificationSource(timeout.Name)
		actions = append(actions, timeout)
		timeoutActions = append(timeoutActions, timeout.Name)
		timeoutRefinements = append(timeoutRefinements, verify.ActionRefinement{Concrete: timeout.Name, Abstract: raw.Name})
		refinementInventory = append(refinementInventory, verify.Refinement{
			Name: timeout.Name, Action: timeout.Name, LifecycleActions: []string{raw.Name}, Source: timeout.Source,
		})
	}

	properties := []verify.Property{
		nexusOperationClosureProperty(workflow, operation),
		nexusOperationTimeoutProperty(operation),
	}
	closureActions := make([]string, 0, len(actions)-len(timeoutActions))
	for _, action := range actions[:len(actions)-len(timeoutActions)] {
		closureActions = append(closureActions, action.Name)
	}
	closureModule := verify.Module{
		Name: nexusClosureModule, Owner: "nexus",
		Relations: []string{nexusOperationWorkflowRelation}, Actions: closureActions,
		Properties: []string{properties[0].Name},
	}
	timeoutModule := verify.Module{
		Name: nexusTimeoutModule, Owner: "nexus",
		Entities: []string{nexusTimeoutEvidenceEntity}, Relations: []string{nexusTimeoutEvidenceRelation},
		Actions: timeoutActions, Properties: []string{properties[1].Name},
	}
	lifecycleRefinement := verify.RefinementMap{
		Name: "nexus-closure-lifecycle", Owner: "nexus", Actions: lifecycleRefinements,
	}
	timeoutRefinement := verify.RefinementMap{
		Name: "nexus-timeout-observation", Owner: "nexus", Actions: timeoutRefinements,
	}
	composition := verify.Composition{
		Name: "nexus-feature", Owners: []verify.CapabilityOwner{"nexus", "workflow"},
		Modules:        []string{nexusClosureModule, nexusTimeoutModule},
		Properties:     []string{properties[0].Name, properties[1].Name},
		RefinementMaps: []string{lifecycleRefinement.Name, timeoutRefinement.Name},
	}
	target := verify.VerificationTarget{
		Name: nexusFeatureTarget, Owners: slices.Clone(composition.Owners),
		Modules: slices.Clone(composition.Modules), Compositions: []string{composition.Name},
		Bounds: map[string]int{
			workflow: 1, operation: 2, nexusTimeoutEvidenceEntity: 2,
		},
		MinimumBounds:       map[string]int{workflow: 1, operation: 2, nexusTimeoutEvidenceEntity: 1},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"workflow-close-race", "nexus-timeout"},
		Abstractions:        []string{"regression.nexus.start_activity"},
	}
	nexusActivityComposition := verify.Composition{
		Name: "nexus-activity", Owners: []verify.CapabilityOwner{"activity", "nexus"},
		Modules: []string{"nexus", "activity"},
		Properties: []string{
			"NexusActivityForwardLinkConsistency",
			"NexusActivityReverseLinkConsistency",
			"NexusActivityTerminalRefinement",
		},
	}
	nexusActivityTarget := verify.VerificationTarget{
		Name: nexusActivityIntegrationTarget, Owners: slices.Clone(nexusActivityComposition.Owners),
		Compositions:        []string{nexusActivityComposition.Name},
		Bounds:              map[string]int{operation: 2, string(model.ActivityType): 2},
		MinimumBounds:       map[string]int{operation: 2, string(model.ActivityType): 2},
		BackendRequirements: []string{"fizz", "ivy", "p", "tla"},
		FailurePolicy:       []string{"wrong-link-identity", "terminal-refinement"},
	}
	return verificationFamilyFragment{
		Model: verify.Model{
			Entities: []verify.EntityType{{
				Name: nexusTimeoutEvidenceEntity, IDs: foundationIDs(nexusTimeoutEvidenceEntity, 2), Initial: "unobserved",
				States: []verify.State{{Name: "unobserved"}, {Name: "valid"}, {Name: "invalid"}},
				Source: nexusVerificationSource(nexusTimeoutEvidenceEntity),
			}},
			Relations: []verify.Relation{
				{Name: nexusOperationWorkflowRelation, Source: operation, Target: workflow, SourceCardinality: verify.One, TargetCardinality: verify.Many},
				{Name: nexusTimeoutEvidenceRelation, Source: operation, Target: nexusTimeoutEvidenceEntity, SourceCardinality: verify.One, TargetCardinality: verify.One},
			},
			Actions: actions, Properties: properties, Refinements: refinementInventory,
		},
		Modules:      []verify.Module{closureModule, timeoutModule},
		Refinements:  []verify.RefinementMap{lifecycleRefinement, timeoutRefinement},
		Compositions: []verify.Composition{composition, nexusActivityComposition},
		Targets:      []verify.VerificationTarget{target, nexusActivityTarget},
	}
}

func nexusOperationClosureProperty(workflow, operation string) verify.Property {
	return verify.Property{
		Name: "NexusOperationClosure",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(operation, "operation", deliveryForAll(workflow, "workflow", deliveryImplies(
			verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: nexusOperationWorkflowRelation, Source: "operation", Target: "workflow"},
				workflowTerminal("workflow"),
			),
			nexusOperationTerminal("operation"),
		))),
		Strengthening: true,
		Source:        verify.Provenance{Path: "tests/umpire2/rule/nexus_operation_closure.go", Symbol: "NexusOperationClosureRule"},
	}
}

func nexusOperationTimeoutProperty(operation string) verify.Property {
	return verify.Property{
		Name: "NexusOperationTimeoutSemantics",
		Kind: verify.SafetyProperty,
		Expr: deliveryForAll(operation, "operation", deliveryImplies(
			verify.StateIs(operation, "operation", model.NexusTimedOut),
			deliveryExists(nexusTimeoutEvidenceEntity, "timeoutEvidence", verify.And(
				verify.Expr{Op: verify.RelationHasExpr, Relation: nexusTimeoutEvidenceRelation, Source: "operation", Target: "timeoutEvidence"},
				verify.StateIs(nexusTimeoutEvidenceEntity, "timeoutEvidence", "valid"),
			)),
		)),
		Source: verify.Provenance{Path: "tests/umpire2/rule/nexus_operation_timeout_semantics.go", Symbol: "NexusOperationTimeoutSemanticsRule"},
	}
}

func workflowTerminal(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(string(model.WorkflowType), variable, model.WorkflowCompleted),
		verify.StateIs(string(model.WorkflowType), variable, model.WorkflowFailed),
		verify.StateIs(string(model.WorkflowType), variable, model.WorkflowCanceled),
		verify.StateIs(string(model.WorkflowType), variable, model.WorkflowTerminated),
		verify.StateIs(string(model.WorkflowType), variable, model.WorkflowTimedOut),
	}}
}

func nexusOperationTerminal(variable string) verify.Expr {
	return verify.Expr{Op: verify.OrExpr, Args: []verify.Expr{
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusSucceeded),
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusFailed),
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusCanceled),
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusTimedOut),
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusTerminated),
		verify.StateIs(string(model.NexusOperationType), variable, model.NexusRejected),
	}}
}

func verificationActionByName(canonical verify.Model, name string) verify.Action {
	for _, action := range canonical.Actions {
		if action.Name == name {
			return action
		}
	}
	panic(fmt.Sprintf("canonical verification action %q is not declared", name))
}

func nexusVerificationSource(symbol string) verify.Provenance {
	return verify.Provenance{Path: "tests/umpire2/protocol/verification_nexus.go", Symbol: symbol}
}
