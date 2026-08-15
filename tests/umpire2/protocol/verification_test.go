package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/common/testing/umpire/verify"
	protocolmodel "go.temporal.io/server/tests/umpire2/model"
)

func TestVerificationModelLowersCanonicalProtocol(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{
		DefaultBound: 1,
		Bounds:       map[umpire.EntityType]int{protocolmodel.NexusOperationType: 2},
	})
	require.NoError(t, err)
	require.NoError(t, verify.Validate(verification))

	nexus := requireEntity(t, verification, string(protocolmodel.NexusOperationType))
	require.Equal(t, []string{"NexusOperation#0", "NexusOperation#1"}, nexus.IDs)
	require.Equal(t, protocolmodel.NexusUnspecified, nexus.Initial)
	require.Contains(t, nexus.States, verify.State{Name: protocolmodel.NexusScheduled, MustProgress: true})

	callback := requireEntity(t, verification, string(protocolmodel.CallbackType))
	require.Equal(t, "unobserved", callback.Initial)
	require.Equal(t, []verify.State{{Name: "unobserved"}}, callback.States)

	relation := requireRelation(t, verification, string(CallbackOperationRelation))
	require.Equal(t, string(protocolmodel.CallbackType), relation.Source)
	require.Equal(t, string(protocolmodel.NexusOperationType), relation.Target)
	require.Equal(t, verify.One, relation.SourceCardinality)
	require.Equal(t, verify.Many, relation.TargetCardinality)
}

func TestVerificationFamilyProjectsProtocolAtomicWithoutSemanticDrift(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	options := VerificationOptions{DefaultBound: 1}

	want, err := protocol.VerificationModel(options)
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(options)
	require.NoError(t, err)
	require.NoError(t, verify.ValidateModelFamily(family))
	require.Equal(t, map[string]int{
		"Activity":       1,
		"Callback":       1,
		"NexusOperation": 1,
		"TaskQueue":      1,
		"Workflow":       1,
		"WorkflowRun":    1,
		"WorkflowTask":   1,
	}, family.Targets[0].Bounds)

	projection, err := verify.Project(family, "protocol-atomic")
	require.NoError(t, err)
	got, report := projection.Model, projection.Closure
	wantJSON, err := verify.MarshalModel(want)
	require.NoError(t, err)
	gotJSON, err := verify.MarshalModel(got)
	require.NoError(t, err)
	require.JSONEq(t, string(wantJSON), string(gotJSON))
	require.Len(t, report.RetainedActions, len(want.Actions))
	require.Empty(t, report.EnvironmentActions)
	require.Empty(t, report.StutteringActions)
	require.NotEmpty(t, report.OmittedActions)
	require.Contains(t, report.OmittedActions, "workflow.delivery.resolve-persisted")
	require.Contains(t, report.RefinedActions, "workflow.delivery.persist")
}

func TestVerificationFamilyDeclaresFoundationDeliverySafetyTarget(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	target := requireVerificationTarget(t, family, "foundation-delivery-safety")
	require.Equal(t, []verify.CapabilityOwner{"history", "matching"}, target.Owners)
	require.Equal(t, map[string]int{
		"DeliveryAttempt": 2,
		"DeliveryQueue":   2,
		"DeliveryTask":    2,
		"Poller":          2,
		"WorkObligation":  2,
	}, target.Bounds)
	require.Equal(t, map[string]int{
		"DeliveryAttempt": 2,
		"DeliveryQueue":   2,
		"DeliveryTask":    2,
		"Poller":          2,
		"WorkObligation":  2,
	}, target.MinimumBounds)

	projection, err := verify.Project(family, target.Name)
	require.NoError(t, err)
	model, report := projection.Model, projection.Closure
	require.NoError(t, verify.Validate(model))
	require.NotEmpty(t, report.RetainedActions)
	require.Empty(t, report.EnvironmentActions)
	for _, property := range []string{
		"delivery.no-split-commit",
		"delivery.ambiguous-commit-resolved",
		"delivery.no-phantom-dispatch",
		"delivery.single-accepted-start",
		"delivery.failed-start-is-not-accepted",
		"delivery.destination-isolation",
		"delivery.path-equivalence",
		"delivery.coarse-retirement-safety",
		"delivery.no-resurrection",
	} {
		requireProperty(t, model, property)
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(2)
	require.NoError(t, err)
	require.Empty(t, exploration.Violations)
}

func TestFoundationDeliveryNoSplitCommitDetectsMutation(t *testing.T) {
	model := foundationDeliveryModel(t)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "delivery.persist.success" {
			continue
		}
		var effects []verify.Effect
		for _, effect := range model.Actions[actionIndex].Effects {
			if effect.Entity == "DeliveryTask" || effect.Relation == "delivery-task-obligation" || effect.Relation == "delivery-task-queue" {
				continue
			}
			effects = append(effects, effect)
		}
		model.Actions[actionIndex].Effects = effects
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.Contains(t, violationProperties(exploration.Violations), "delivery.no-split-commit")
}

func TestFoundationDeliveryDoesNotAdvanceAnUnresolvedIntent(t *testing.T) {
	model := foundationDeliveryModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	states, err := interpreter.Replay([]verify.TraceStep{{
		Action: "delivery.persist.ambiguous",
		Bindings: verify.Bindings{
			"obligation": "WorkObligation#0", "task": "DeliveryTask#0", "queue": "DeliveryQueue#0",
		},
	}})
	require.NoError(t, err)
	require.Len(t, states, 1)

	_, err = interpreter.Step(states[0], "delivery.offer-sync", verify.Bindings{"task": "DeliveryTask#0"})
	require.ErrorContains(t, err, "is not enabled")
	resolved, err := interpreter.Step(states[0], "delivery.resolve-persisted", verify.Bindings{
		"obligation": "WorkObligation#0", "task": "DeliveryTask#0",
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	_, err = interpreter.Step(resolved[0], "delivery.offer-sync", verify.Bindings{"task": "DeliveryTask#0"})
	require.NoError(t, err)
}

func TestFoundationDeliveryRetryIdentityMutationIsDetected(t *testing.T) {
	model := foundationDeliveryModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name == "delivery.retry" {
			model.Actions[actionIndex].Effects = append(model.Actions[actionIndex].Effects, verify.Effect{
				Kind: verify.RemoveRelationEffect, Relation: "delivery-attempt-task", Source: "attempt", Target: "task",
			})
		}
	}
	state := replayFoundationTrace(t, model,
		foundationPersistTrace(),
		[]verify.TraceStep{
			{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
			{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
			{Action: "delivery.authorize.accept", Bindings: verify.Bindings{"obligation": "WorkObligation#0", "task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
			{Action: "delivery.dispatch", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
			{Action: "delivery.retry", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
		},
	)
	require.Empty(t, state.Relations["delivery-attempt-task"])
	requireFoundationPropertyFails(t, model, state, "delivery.retry-preserves-obligation")
}

func TestFoundationDeliveryPrematureRetirementMutationIsDetected(t *testing.T) {
	model := foundationDeliveryModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name == "delivery.retire" {
			model.Actions[actionIndex].Guard = verify.StateIs("DeliveryTask", "task", "pending")
		}
	}
	state := replayFoundationTrace(t, model,
		foundationPersistTrace(),
		[]verify.TraceStep{{Action: "delivery.retire", Bindings: verify.Bindings{"task": "DeliveryTask#0"}}},
	)
	require.Equal(t, "retired", state.Entities["DeliveryTask"]["DeliveryTask#0"])
	require.Equal(t, "valid", state.Entities["WorkObligation"]["WorkObligation#0"])
	requireFoundationPropertyFails(t, model, state, "delivery.coarse-retirement-safety")
}

func TestFoundationDeliveryFailedStartMutationIsDetected(t *testing.T) {
	model := foundationDeliveryModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name == "delivery.authorize.reject" {
			model.Actions[actionIndex].Effects = append(model.Actions[actionIndex].Effects, verify.Effect{
				Kind: verify.SetStateEffect, Entity: "WorkObligation", Ref: "obligation", State: "accepted",
			})
		}
	}
	state := replayFoundationTrace(t, model,
		foundationPersistTrace(),
		[]verify.TraceStep{
			{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
			{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
			{Action: "delivery.authorize.reject", Bindings: verify.Bindings{"obligation": "WorkObligation#0", "task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
		},
	)
	require.Equal(t, "accepted", state.Entities["WorkObligation"]["WorkObligation#0"])
	require.Empty(t, state.Relations["delivery-accepted-start"])
	requireFoundationPropertyFails(t, model, state, "delivery.single-accepted-start")
}

func TestFoundationDeliveryDuplicateStartMutationIsDetected(t *testing.T) {
	model := foundationDeliveryModel(t)
	model.Refinements = nil
	model.Actions = append(model.Actions, verify.Action{
		Name: "delivery.seed-duplicate-start",
		Parameters: []verify.Parameter{
			{Name: "obligation", Type: "WorkObligation", Binding: verify.InputBinding},
			{Name: "attempt", Type: "DeliveryAttempt", Binding: verify.FreshBinding},
		},
		Guard: verify.StateIs("WorkObligation", "obligation", "accepted"),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: "DeliveryAttempt", Ref: "attempt", State: "accepted"},
			{Kind: verify.AddRelationEffect, Relation: "delivery-accepted-start", Source: "obligation", Target: "attempt"},
		},
	})
	state := replayFoundationTrace(t, model,
		foundationPersistTrace(),
		[]verify.TraceStep{
			{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
			{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
			{Action: "delivery.authorize.accept", Bindings: verify.Bindings{"obligation": "WorkObligation#0", "task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
			{Action: "delivery.seed-duplicate-start", Bindings: verify.Bindings{"obligation": "WorkObligation#0", "attempt": "DeliveryAttempt#1"}},
		},
	)
	violations, err := verify.CheckState(model, state, false)
	require.NoError(t, err)
	require.Contains(t, violationProperties(violations), "relation delivery-accepted-start source cardinality")
}

func TestVerificationFamilyDeclaresWorkflowDeliveryTarget(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 2})
	require.NoError(t, err)

	target := requireVerificationTarget(t, family, workflowDeliveryTarget)
	require.Equal(t, []verify.CapabilityOwner{"history", "matching", "workflow"}, target.Owners)
	projection, err := verify.Project(family, target.Name)
	require.NoError(t, err)
	requireAction(t, projection.Model, "workflow.delivery.persist")
	requireAction(t, projection.Model, "workflow.delivery.authorize-added")
	requireAction(t, projection.Model, "workflow.delivery.authorize-stored")
	requireProperty(t, projection.Model, "workflow.delivery.intent-correspondence")
	requireProperty(t, projection.Model, "workflow.delivery.accepted-start-correspondence")
	require.NotContains(t, projection.Closure.RetainedActions, "WorkflowTask.created.add.AnyHosting")
	require.Contains(t, projection.Closure.RefinedActions, "WorkflowTask.created.add.AnyHosting")
	require.Contains(t, projection.Closure.RefinedActions, "delivery.persist.success")

	interpreter, err := verify.NewInterpreter(projection.Model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.Empty(t, exploration.Violations)
}

func TestWorkflowDeliveryIntentMutationIsDetected(t *testing.T) {
	model := workflowDeliveryModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "workflow.delivery.persist" {
			continue
		}
		for effectIndex, effect := range model.Actions[actionIndex].Effects {
			if effect.Relation == workflowTaskObligationRelation {
				model.Actions[actionIndex].Effects = append(model.Actions[actionIndex].Effects[:effectIndex], model.Actions[actionIndex].Effects[effectIndex+1:]...)
				break
			}
		}
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.Contains(t, violationProperties(exploration.Violations), "workflow.delivery.intent-correspondence")
}

func TestWorkflowDeliveryRefinementRejectsMissingFoundationEffect(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 2})
	require.NoError(t, err)
	for actionIndex := range family.Model.Actions {
		if family.Model.Actions[actionIndex].Name == "workflow.delivery.persist" {
			family.Model.Actions[actionIndex].Effects = family.Model.Actions[actionIndex].Effects[:1]
			break
		}
	}

	err = verify.ValidateModelFamily(family)
	require.ErrorContains(t, err, `refinement map "workflow-delivery-intent" concrete action "workflow.delivery.persist" omits effect`)
}

func TestVerificationFamilyDeclaresActivityDeliveryTarget(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	target := requireVerificationTarget(t, family, activityDeliveryTarget)
	require.Equal(t, []verify.CapabilityOwner{"activity", "history", "matching"}, target.Owners)
	projection, err := verify.Project(family, target.Name)
	require.NoError(t, err)
	requireAction(t, projection.Model, "activity.delivery.persist")
	requireAction(t, projection.Model, "activity.delivery.retry")
	requireAction(t, projection.Model, "activity.delivery.authorize")
	requireProperty(t, projection.Model, "activity.delivery.intent-correspondence")
	requireProperty(t, projection.Model, "activity.delivery.accepted-start-correspondence")
	for _, action := range []string{
		"Activity.unspecified.schedule.AnyHosting",
		"Activity.backing_off.schedule.AnyHosting",
		"delivery.persist.success",
	} {
		require.Contains(t, projection.Closure.RefinedActions, action)
	}

	interpreter, err := verify.NewInterpreter(projection.Model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.Empty(t, exploration.Violations)
}

func TestActivityDeliveryIntentMutationIsDetected(t *testing.T) {
	model := activityDeliveryModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "activity.delivery.persist" {
			continue
		}
		for effectIndex, effect := range model.Actions[actionIndex].Effects {
			if effect.Relation == activityObligationRelation {
				model.Actions[actionIndex].Effects = append(model.Actions[actionIndex].Effects[:effectIndex], model.Actions[actionIndex].Effects[effectIndex+1:]...)
				break
			}
		}
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.Contains(t, violationProperties(exploration.Violations), "activity.delivery.intent-correspondence")
}

func TestActivityDeliveryRetryRebindsFreshIntent(t *testing.T) {
	model := activityDeliveryModel(t)
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{
			Action: "activity.delivery.persist",
			Bindings: verify.Bindings{
				"entity":     "Activity#0",
				"obligation": "WorkObligation#0",
				"task":       "DeliveryTask#0",
				"queue":      "DeliveryQueue#0",
			},
		},
		{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
		{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
		{
			Action: "activity.delivery.authorize",
			Bindings: verify.Bindings{
				"entity":     "Activity#0",
				"obligation": "WorkObligation#0",
				"task":       "DeliveryTask#0",
				"attempt":    "DeliveryAttempt#0",
			},
		},
		{Action: "Activity.started.attempt_failed.AnyHosting", Bindings: verify.Bindings{"entity": "Activity#0"}},
		{
			Action: "activity.delivery.retry",
			Bindings: verify.Bindings{
				"entity":              "Activity#0",
				"previous-obligation": "WorkObligation#0",
				"previous-task":       "DeliveryTask#0",
				"obligation":          "WorkObligation#1",
				"task":                "DeliveryTask#1",
				"queue":               "DeliveryQueue#0",
			},
		},
	})

	require.Equal(t, protocolmodel.ActivityScheduled, state.Entities["Activity"]["Activity#0"])
	require.Equal(t, "accepted", state.Entities[workObligationEntity]["WorkObligation#0"])
	require.Equal(t, "valid", state.Entities[workObligationEntity]["WorkObligation#1"])
	require.Equal(t, "authorized", state.Entities[deliveryTaskEntity]["DeliveryTask#0"])
	require.Equal(t, "pending", state.Entities[deliveryTaskEntity]["DeliveryTask#1"])
	require.Equal(t, []verify.RelationTuple{{Source: "Activity#0", Target: "WorkObligation#1"}}, state.Relations[activityObligationRelation])
	require.Equal(t, []verify.RelationTuple{{Source: "Activity#0", Target: "DeliveryTask#1"}}, state.Relations[activityDeliveryRelation])

	property := requireProperty(t, model, "activity.delivery.intent-correspondence")
	holds, err := verify.EvaluateExpr(model, state, property.Expr, nil)
	require.NoError(t, err)
	require.True(t, holds)
}

func TestWorkflowDeliveryRejectsAuthorizationForAnotherIntent(t *testing.T) {
	model := workflowDeliveryModel(t)
	removeActionGuardRelation(t, &model, "workflow.delivery.authorize-added", workflowTaskObligationRelation)
	removeActionGuardRelation(t, &model, "workflow.delivery.authorize-added", workflowTaskDeliveryRelation)
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{
			Action: "workflow.delivery.persist",
			Bindings: verify.Bindings{
				"entity":     "WorkflowTask#0",
				"obligation": "WorkObligation#0",
				"task":       "DeliveryTask#0",
				"queue":      "DeliveryQueue#0",
			},
		},
		{
			Action: "delivery.persist.ambiguous",
			Bindings: verify.Bindings{
				"obligation": "WorkObligation#1",
				"task":       "DeliveryTask#1",
				"queue":      "DeliveryQueue#0",
			},
		},
		{Action: "workflow.delivery.resolve-persisted", Bindings: verify.Bindings{"obligation": "WorkObligation#1", "task": "DeliveryTask#1"}},
		{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#1"}},
		{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#1", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
		{
			Action: "workflow.delivery.authorize-added",
			Bindings: verify.Bindings{
				"entity":     "WorkflowTask#0",
				"obligation": "WorkObligation#1",
				"task":       "DeliveryTask#1",
				"attempt":    "DeliveryAttempt#0",
			},
		},
	})

	requireFoundationPropertyFails(t, model, state, "workflow.delivery.accepted-start-correspondence")
}

func TestActivityDeliveryRejectsAuthorizationForAnotherIntent(t *testing.T) {
	model := activityDeliveryModel(t)
	removeActionGuardRelation(t, &model, "activity.delivery.authorize", activityObligationRelation)
	removeActionGuardRelation(t, &model, "activity.delivery.authorize", activityDeliveryRelation)
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{
			Action: "activity.delivery.persist",
			Bindings: verify.Bindings{
				"entity":     "Activity#0",
				"obligation": "WorkObligation#0",
				"task":       "DeliveryTask#0",
				"queue":      "DeliveryQueue#0",
			},
		},
		{
			Action: "delivery.persist.ambiguous",
			Bindings: verify.Bindings{
				"obligation": "WorkObligation#1",
				"task":       "DeliveryTask#1",
				"queue":      "DeliveryQueue#0",
			},
		},
		{Action: "activity.delivery.resolve-persisted", Bindings: verify.Bindings{"obligation": "WorkObligation#1", "task": "DeliveryTask#1"}},
		{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#1"}},
		{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#1", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
		{
			Action: "activity.delivery.authorize",
			Bindings: verify.Bindings{
				"entity":     "Activity#0",
				"obligation": "WorkObligation#1",
				"task":       "DeliveryTask#1",
				"attempt":    "DeliveryAttempt#0",
			},
		},
	})

	requireFoundationPropertyFails(t, model, state, "activity.delivery.accepted-start-correspondence")
}

func TestVerificationFamilyDeclaresSpeculativeWorkflowTaskTarget(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	target := requireVerificationTarget(t, family, workflowTaskSpeculativeTarget)
	require.Equal(t, 2, target.MinimumBounds[string(protocolmodel.WorkflowTaskType)])
	projection, err := verify.Project(family, target.Name)
	require.NoError(t, err)
	for _, action := range []string{
		"workflow.task.create-normal",
		"workflow.task.create-speculative-direct",
		"workflow.task.speculative-fallback",
		"workflow.task.cancel-speculative-added",
		"workflow.delivery.authorize-added",
	} {
		requireAction(t, projection.Model, action)
	}
	requireProperty(t, projection.Model, "SpeculativeTaskCreation")
	starvation := requireProperty(t, projection.Model, "WorkflowTaskStarvation")
	require.Equal(t, verify.ProgressProperty, starvation.Kind)
	require.NotEmpty(t, starvation.Fairness)
	for _, action := range []string{
		"WorkflowTask.created.add.AnyHosting",
		"WorkflowTask.added.store.AnyHosting",
		"delivery.persist.success",
		"delivery.spool",
	} {
		require.Contains(t, projection.Closure.RefinedActions, action)
	}
}

func TestSpeculativeWorkflowTaskCreationMutationIsDetected(t *testing.T) {
	model := workflowTaskSpeculativeModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(workflowTaskDuplicateCreationTrace())
	require.ErrorContains(t, err, `action "workflow.task.create-speculative-direct" is not enabled`)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "workflow.task.create-speculative-direct" {
			continue
		}
		var guards []verify.Expr
		for _, guard := range model.Actions[actionIndex].Guard.Args {
			if guard.Op != verify.NotExpr {
				guards = append(guards, guard)
			}
		}
		model.Actions[actionIndex].Guard.Args = guards
		break
	}
	state := replayFoundationTrace(t, model, workflowTaskDuplicateCreationTrace())
	requireFoundationPropertyFails(t, model, state, "SpeculativeTaskCreation")
}

func TestVerificationFamilyDeclaresNexusTargets(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	feature, err := verify.Project(family, nexusFeatureTarget)
	require.NoError(t, err)
	requireAction(t, feature.Model, "nexus.workflow.start")
	requireAction(t, feature.Model, "nexus.operation.schedule.Standalone")
	requireAction(t, feature.Model, "nexus.timeout.scheduled.Standalone")
	requireProperty(t, feature.Model, "NexusOperationClosure")
	timeout := requireProperty(t, feature.Model, "NexusOperationTimeoutSemantics")
	require.Equal(t, "NexusOperationTimeoutSemanticsRule", timeout.Source.Symbol)

	integration, err := verify.Project(family, nexusActivityIntegrationTarget)
	require.NoError(t, err)
	requireAction(t, integration.Model, "regression.nexus.start_activity")
	requireProperty(t, integration.Model, "NexusActivityForwardLinkConsistency")
	requireProperty(t, integration.Model, "NexusActivityReverseLinkConsistency")
	requireProperty(t, integration.Model, "NexusActivityTerminalRefinement")
}

func TestNexusOperationClosureMutationIsDetected(t *testing.T) {
	model := nexusFeatureModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(nexusCloseWhileActiveTrace())
	require.ErrorContains(t, err, `action "nexus.workflow.close.complete" is not enabled`)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "nexus.workflow.close.complete" {
			continue
		}
		model.Actions[actionIndex].Guard = model.Actions[actionIndex].Guard.Args[0]
		break
	}
	state := replayFoundationTrace(t, model, nexusCloseWhileActiveTrace())
	requireFoundationPropertyFails(t, model, state, "NexusOperationClosure")
}

func TestNexusOperationTimeoutMetadataMutationIsDetected(t *testing.T) {
	model := nexusFeatureModel(t)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "nexus.timeout.scheduled.Standalone" {
			continue
		}
		model.Actions[actionIndex].Effects = model.Actions[actionIndex].Effects[:2]
		break
	}
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{Action: "nexus.workflow.start", Bindings: verify.Bindings{"wf": "Workflow#0"}},
		{Action: "nexus.operation.schedule.Standalone", Bindings: verify.Bindings{"op": "NexusOperation#0", "workflow": "Workflow#0"}},
		{Action: "nexus.timeout.scheduled.Standalone", Bindings: verify.Bindings{"op": "NexusOperation#0", "timeoutEvidence": "NexusTimeoutEvidence#0"}},
	})
	requireFoundationPropertyFails(t, model, state, "NexusOperationTimeoutSemantics")
}

func TestVerificationFamilyDeclaresCallbackTargets(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	nexus, err := verify.Project(family, callbackNexusTarget)
	require.NoError(t, err)
	require.Equal(t, []string{"fizz", "ivy", "tla"}, nexus.Target.BackendRequirements)
	requireAction(t, nexus.Model, "callback.attach-reference")
	requireProperty(t, nexus.Model, "CallbackReferenceConsistency")
	requireProperty(t, nexus.Model, "CallbackResponseConsistency")

	workflow, err := verify.Project(family, callbackWorkflowTarget)
	require.NoError(t, err)
	require.Equal(t, []string{"fizz", "ivy", "tla"}, workflow.Target.BackendRequirements)
	requireAction(t, workflow.Model, "callback.attach-handler")
	requireAction(t, workflow.Model, "callback.delivery.retry")
	requireProperty(t, workflow.Model, "CallbackHandlerLifetime")
}

func TestCallbackReferenceIdentityMutationIsDetected(t *testing.T) {
	model := callbackTargetModel(t, callbackNexusTarget)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "callback.attach-reference" {
			continue
		}
		model.Actions[actionIndex].Effects = model.Actions[actionIndex].Effects[:3]
		break
	}
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "NexusOperation.unspecified.schedule.Standalone", Bindings: verify.Bindings{"op": "NexusOperation#0"}},
		{Action: "callback.attach-reference", Bindings: verify.Bindings{"callback": "Callback#0", "operation": "NexusOperation#0", "handlerRun": "WorkflowRun#0"}},
	})
	requireFoundationPropertyFails(t, model, state, "CallbackReferenceConsistency")
}

func TestCallbackReferenceRejectsOperationReattachment(t *testing.T) {
	model := callbackTargetModel(t, callbackNexusTarget)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay([]verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#1"}},
		{Action: "NexusOperation.unspecified.schedule.Standalone", Bindings: verify.Bindings{"op": "NexusOperation#0"}},
		{Action: "callback.attach-reference", Bindings: verify.Bindings{"callback": "Callback#0", "operation": "NexusOperation#0", "handlerRun": "WorkflowRun#0"}},
		{Action: "callback.attach-reference", Bindings: verify.Bindings{"callback": "Callback#1", "operation": "NexusOperation#0", "handlerRun": "WorkflowRun#1"}},
	})
	require.ErrorContains(t, err, `action "callback.attach-reference" is not enabled`)
}

func TestCallbackDeliveryRetryPreservesIdentity(t *testing.T) {
	model := callbackTargetModel(t, callbackWorkflowTarget)
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.attach-handler", Bindings: verify.Bindings{"callback": "Callback#0", "handlerRun": "WorkflowRun#0"}},
		{Action: "callback.delivery.enqueue", Bindings: verify.Bindings{"callback": "Callback#0", "delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.fail-pending", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.retry", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.deliver", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.acknowledge", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0", "response": "CallbackResponse#0"}},
	})
	require.Equal(t, "acknowledged", state.Entities[callbackDeliveryEntity]["CallbackDelivery#0"])
	require.Equal(t, []verify.RelationTuple{{Source: "Callback#0", Target: "CallbackDelivery#0"}}, state.Relations[callbackDeliveryRelation])
	property := requireProperty(t, model, "CallbackResponseConsistency")
	holds, err := verify.EvaluateExpr(model, state, property.Expr, nil)
	require.NoError(t, err)
	require.True(t, holds)
}

func TestCallbackResponseRequiresAcknowledgedDelivery(t *testing.T) {
	model := callbackTargetModel(t, callbackWorkflowTarget)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name == "callback.delivery.acknowledge" {
			model.Actions[actionIndex].Effects = model.Actions[actionIndex].Effects[1:]
			break
		}
	}
	state := replayFoundationTrace(t, model, []verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.attach-handler", Bindings: verify.Bindings{"callback": "Callback#0", "handlerRun": "WorkflowRun#0"}},
		{Action: "callback.delivery.enqueue", Bindings: verify.Bindings{"callback": "Callback#0", "delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.deliver", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0"}},
		{Action: "callback.delivery.acknowledge", Bindings: verify.Bindings{"delivery": "CallbackDelivery#0", "response": "CallbackResponse#0"}},
	})
	requireFoundationPropertyFails(t, model, state, "CallbackResponseConsistency")
}

func TestCallbackHandlerLifetimeMutationIsDetected(t *testing.T) {
	model := callbackTargetModel(t, callbackWorkflowTarget)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(callbackCloseWithPendingDeliveryTrace())
	require.ErrorContains(t, err, `action "callback.handler.close.complete" is not enabled`)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name == "callback.handler.close.complete" {
			model.Actions[actionIndex].Guard = model.Actions[actionIndex].Guard.Args[0]
			break
		}
	}
	state := replayFoundationTrace(t, model, callbackCloseWithPendingDeliveryTrace())
	requireFoundationPropertyFails(t, model, state, "CallbackHandlerLifetime")
}

func TestCallbackDeliveryRejectsEnqueueAfterHandlerCloses(t *testing.T) {
	model := callbackTargetModel(t, callbackWorkflowTarget)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay([]verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.attach-handler", Bindings: verify.Bindings{"callback": "Callback#0", "handlerRun": "WorkflowRun#0"}},
		{Action: "callback.handler.close.complete", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.delivery.enqueue", Bindings: verify.Bindings{"callback": "Callback#0", "delivery": "CallbackDelivery#0"}},
	})
	require.ErrorContains(t, err, `action "callback.delivery.enqueue" is not enabled`)
}

func TestVerificationFamilyDeclaresRoutingTarget(t *testing.T) {
	model := deliveryRoutingModel(t)
	requireAction(t, model, "routing.bootstrap-history-owner")
	requireAction(t, model, "routing.bootstrap")
	requireAction(t, model, "routing.reserve-compatible")
	requireAction(t, model, "routing.handoff")
	requireAction(t, model, "routing.handoff-history-owner")
	requireProperty(t, model, "delivery.routing-isolation")
	requireProperty(t, model, "delivery.owner-generation-fencing")

	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	ownership, err := verify.Project(family, ownershipFencingTarget)
	require.NoError(t, err)
	require.Equal(t, []string{"fizz", "ivy", "tla"}, ownership.Target.BackendRequirements)
	requireProperty(t, ownership.Model, "delivery.owner-generation-fencing")
	require.Equal(t, 2, ownership.Target.MinimumBounds[deliveryOwnerGenerationEntity])
	require.Equal(t, 2, ownership.Target.MinimumBounds[historyOwnerGenerationEntity])
	require.Equal(t, 1, ownership.Target.Bounds[workObligationEntity])
	for _, property := range ownership.Model.Properties {
		require.NotEqual(t, "delivery.routing-isolation", property.Name)
	}
	routing, err := verify.Project(family, deliveryRoutingTarget)
	require.NoError(t, err)
	require.Equal(t, []string{"apalache", "fizz", "ivy", "p", "sany"}, routing.Target.BackendRequirements)
	ownershipHash, err := verify.HashModel(ownership.Model)
	require.NoError(t, err)
	routingHash, err := verify.HashModel(routing.Model)
	require.NoError(t, err)
	require.NotEqual(t, routingHash, ownershipHash)
}

func TestDeliveryRoutingRejectsIncompatiblePollerMutation(t *testing.T) {
	model := deliveryRoutingModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(deliveryWrongRouteTrace())
	require.ErrorContains(t, err, `action "routing.reserve-compatible" is not enabled`)

	removeActionGuardRelation(t, &model, "routing.reserve-compatible", deliveryPollerRouteRelation)
	state := replayFoundationTrace(t, model, deliveryWrongRouteTrace())
	requireFoundationPropertyFails(t, model, state, "delivery.routing-isolation")
}

func TestDeliveryRoutingRejectsPollerReregistration(t *testing.T) {
	model := deliveryRoutingModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay([]verify.TraceStep{
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#1", "partition": "MatchingQueuePartition#1", "generation": "MatchingOwnerGeneration#1"}},
		{Action: "routing.register-poller", Bindings: verify.Bindings{"poller": "Poller#0", "route": "DeliveryRouteClass#0"}},
		{Action: "routing.register-poller", Bindings: verify.Bindings{"poller": "Poller#0", "route": "DeliveryRouteClass#1"}},
	})
	require.ErrorContains(t, err, `action "routing.register-poller" is not enabled`)
}

func TestDeliveryRoutingRejectsTaskRerouting(t *testing.T) {
	model := deliveryRoutingModel(t)
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay([]verify.TraceStep{
		{Action: "routing.bootstrap-history-owner", Bindings: verify.Bindings{"shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#1", "partition": "MatchingQueuePartition#1", "generation": "MatchingOwnerGeneration#1"}},
		foundationPersistTrace()[0],
		{Action: "routing.forward-to-matching", Bindings: verify.Bindings{"task": "DeliveryTask#0", "route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0", "shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.forward-to-matching", Bindings: verify.Bindings{"task": "DeliveryTask#0", "route": "DeliveryRouteClass#1", "partition": "MatchingQueuePartition#1", "generation": "MatchingOwnerGeneration#1", "shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
	})
	require.ErrorContains(t, err, `action "routing.forward-to-matching" is not enabled`)
}

func TestDeliveryRoutingRejectsStaleOwnerMutation(t *testing.T) {
	model := deliveryRoutingModel(t)
	trace := append(deliveryCompatibleReservationTrace(), verify.TraceStep{
		Action: "routing.handoff",
		Bindings: verify.Bindings{
			"partition": "MatchingQueuePartition#0", "oldGeneration": "MatchingOwnerGeneration#0", "newGeneration": "MatchingOwnerGeneration#1",
		},
	})
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(trace)
	require.ErrorContains(t, err, `action "routing.handoff" is not enabled`)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "routing.handoff" {
			continue
		}
		var guards []verify.Expr
		for _, guard := range model.Actions[actionIndex].Guard.Args {
			if guard.Op != verify.ForAllExpr {
				guards = append(guards, guard)
			}
		}
		model.Actions[actionIndex].Guard.Args = guards
		break
	}
	state := replayFoundationTrace(t, model, trace)
	requireFoundationPropertyFails(t, model, state, "delivery.owner-generation-fencing")
}

func TestDeliveryRoutingBlocksHistoryHandoffWithLiveForwardedTask(t *testing.T) {
	model := deliveryRoutingModel(t)
	trace := deliveryCompatibleReservationTrace()
	trace = append(trace[:len(trace)-1], verify.TraceStep{
		Action: "routing.handoff-history-owner",
		Bindings: verify.Bindings{
			"shard": "HistoryShard#0", "oldHistoryGeneration": "HistoryOwnerGeneration#0", "newHistoryGeneration": "HistoryOwnerGeneration#1",
		},
	})
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(trace)
	require.ErrorContains(t, err, `action "routing.handoff-history-owner" is not enabled`)
}

func TestVerificationFamilyDeclaresBacklogTarget(t *testing.T) {
	model := deliveryBacklogModel(t)
	requireAction(t, model, "backlog.append-first")
	requireAction(t, model, "backlog.ack")
	requireAction(t, model, "backlog.gc")
	requireProperty(t, model, "backlog.ack-after-dispatch")
	requireProperty(t, model, "backlog.gc-after-retirement")
	requireProperty(t, model, "backlog.ack-prefix")
	progress := requireProperty(t, model, "backlog.reader-progress")
	require.Equal(t, verify.ProgressProperty, progress.Kind)
	require.NotEmpty(t, progress.Fairness)
}

func TestBacklogPrematureAcknowledgementMutationIsDetected(t *testing.T) {
	model := deliveryBacklogModel(t)
	trace := []verify.TraceStep{
		foundationPersistTrace()[0],
		{Action: "delivery.spool", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
		{Action: "backlog.append-first", Bindings: verify.Bindings{"position": "BacklogPosition#0", "task": "DeliveryTask#0"}},
		{Action: "backlog.read", Bindings: verify.Bindings{"position": "BacklogPosition#0"}},
		{Action: "backlog.ack", Bindings: verify.Bindings{"position": "BacklogPosition#0", "task": "DeliveryTask#0"}},
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(trace)
	require.ErrorContains(t, err, `action "backlog.ack" is not enabled`)

	removeActionGuardOp(t, &model, "backlog.ack", verify.OrExpr)
	state := replayFoundationTrace(t, model, trace)
	requireFoundationPropertyFails(t, model, state, "backlog.ack-after-dispatch")
}

func TestBacklogPrematureGarbageCollectionMutationIsDetected(t *testing.T) {
	model := deliveryBacklogModel(t)
	trace := append(backlogAcknowledgedTrace(), verify.TraceStep{
		Action: "backlog.gc", Bindings: verify.Bindings{"position": "BacklogPosition#0", "task": "DeliveryTask#0"},
	})
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	_, err = interpreter.Replay(trace)
	require.ErrorContains(t, err, `action "backlog.gc" is not enabled`)

	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != "backlog.gc" {
			continue
		}
		var guards []verify.Expr
		for _, guard := range model.Actions[actionIndex].Guard.Args {
			if guard.Op != verify.StateIsExpr || guard.Entity != deliveryTaskEntity {
				guards = append(guards, guard)
			}
		}
		model.Actions[actionIndex].Guard.Args = guards
		break
	}
	state := replayFoundationTrace(t, model, trace)
	requireFoundationPropertyFails(t, model, state, "backlog.gc-after-retirement")
}

func TestVerificationModelMakesLifecycleContextPartOfEveryAction(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	schedule := requireAction(t, verification, "NexusOperation.unspecified.schedule.Standalone")
	require.Equal(t, verify.StateIs("NexusOperation", "op", protocolmodel.NexusUnspecified), schedule.Guard)
	require.Equal(t, []verify.Effect{{
		Kind:   verify.CreateEffect,
		Entity: "NexusOperation",
		Ref:    "op",
		State:  protocolmodel.NexusScheduled,
	}}, schedule.Effects)
	require.Equal(t, []string{umpire.RPCDrive.String()}, schedule.Capabilities)
	require.False(t, schedule.Unrealized)

	timeoutGap := requireAction(t, verification, "NexusOperation.started.timeout.Standalone")
	require.True(t, timeoutGap.Unrealized)
	require.Equal(t, verify.StateIs("NexusOperation", "entity", protocolmodel.NexusStarted), timeoutGap.Guard)
	require.Equal(t, []verify.Effect{{
		Kind:   verify.SetStateEffect,
		Entity: "NexusOperation",
		Ref:    "entity",
		State:  protocolmodel.NexusTimedOut,
	}}, timeoutGap.Effects)
	require.Equal(t, []string{umpire.Faults.String()}, timeoutGap.Capabilities)

	activitySchedule := requireAction(t, verification, "Activity.unspecified.schedule.AnyHosting")
	require.Equal(t, verify.FreshBinding, activitySchedule.Parameters[0].Binding)
	require.Equal(t, verify.CreateEffect, activitySchedule.Effects[0].Kind)
}

func TestVerificationModelDerivesQuiescentProgressProperties(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	property := requireProperty(t, verification, "NexusOperation.scheduled.quiescent-progress")
	require.Equal(t, verify.QuiescentProperty, property.Kind)
	require.Empty(t, property.Fairness)
	require.Equal(t, verify.Expr{
		Op:     verify.ForAllExpr,
		Entity: "NexusOperation",
		Var:    "entity",
		Args: []verify.Expr{{
			Op: verify.NotExpr,
			Args: []verify.Expr{
				verify.StateIs("NexusOperation", "entity", protocolmodel.NexusScheduled),
			},
		}},
	}, property.Expr)
}

func TestVerificationModelIncludesRuntimeCrossEntityProperties(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	forward := requireProperty(t, verification, "NexusActivityForwardLinkConsistency")
	require.Equal(t, verify.SafetyProperty, forward.Kind)
	require.Equal(t, "NexusActivityLinkConsistencyRule", forward.Source.Symbol)
	requireProperty(t, verification, "NexusActivityReverseLinkConsistency")
	strengthening := requireProperty(t, verification, "NexusActivityTerminalRefinement")
	require.True(t, strengthening.Strengthening)
}

func TestVerificationModelIncludesSelectedRegressionRefinementAndInventory(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	action := requireAction(t, verification, "regression.nexus.start_activity")
	require.Equal(t, []verify.Parameter{
		{Name: "activity", Type: "Activity", Binding: verify.FreshBinding},
		{Name: "operation", Type: "NexusOperation", Binding: verify.InputBinding},
	}, action.Parameters)
	require.Contains(t, action.Effects, verify.Effect{Kind: verify.AddRelationEffect, Relation: string(NexusActivityRelation), Source: "operation", Target: "activity"})
	require.Contains(t, action.Effects, verify.Effect{Kind: verify.AddRelationEffect, Relation: string(ActivityNexusRelation), Source: "activity", Target: "operation"})

	refinement := requireRefinement(t, verification, action.Name)
	require.Equal(t, []string{"nexus.start_activity"}, refinement.RegressionActions)
	require.Equal(t, []string{"NexusOperation.scheduled.succeed.Embedded"}, refinement.LifecycleActions)
	require.True(t, requireInventoryItem(t, verification, "regression-action", "nexus.start_activity").Included)
	require.True(t, requireInventoryItem(t, verification, "regression-action", "nexus.schedule_default").Included)
	require.True(t, requireInventoryItem(t, verification, "regression-action", "workflow.complete").Included)
	excluded := requireInventoryItem(t, verification, "regression-action", "nexus.timeout")
	require.False(t, excluded.Included)
	require.Contains(t, excluded.Reason, "permanent exclusion:")
	require.Contains(t, requireRefinement(t, verification, "NexusOperation.unspecified.schedule.Standalone").RegressionActions, "nexus.schedule_default")
}

func TestLowerRegressionStartActivityRejectsSemanticDrift(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	var selected regress.ActionCapability
	for _, action := range protocol.regression.Snapshot().Actions {
		if action.Schema.Name == "nexus.start_activity" {
			selected = action
			break
		}
	}
	require.NotEmpty(t, selected.Schema.Name)
	selected.Effects = selected.Effects[:len(selected.Effects)-1]

	_, err = lowerRegressionStartActivity(selected, map[umpire.EntityType]*umpire.Lifecycle{
		protocolmodel.NexusOperationType: protocol.mustLifecycle(t, protocolmodel.NexusOperationType),
		protocolmodel.ActivityType:       protocol.mustLifecycle(t, protocolmodel.ActivityType),
	})
	require.ErrorContains(t, err, `regression action "nexus.start_activity" no longer matches its verification refinement`)
}

func TestVerificationModelIsExhaustivelyInterpretableAtSmokeBounds(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	interpreter, err := verify.NewInterpreter(verification)
	require.NoError(t, err)

	exploration, err := interpreter.Explore(100)
	require.NoError(t, err)
	require.True(t, exploration.Complete)
	require.NotEmpty(t, exploration.States)
	require.Empty(t, exploration.Violations)
}

func requireEntity(t *testing.T, verification verify.Model, name string) verify.EntityType {
	t.Helper()
	for _, entity := range verification.Entities {
		if entity.Name == name {
			return entity
		}
	}
	require.FailNow(t, "verification entity is missing", name)
	return verify.EntityType{}
}

func requireRelation(t *testing.T, verification verify.Model, name string) verify.Relation {
	t.Helper()
	for _, relation := range verification.Relations {
		if relation.Name == name {
			return relation
		}
	}
	require.FailNow(t, "verification relation is missing", name)
	return verify.Relation{}
}

func requireAction(t *testing.T, verification verify.Model, name string) verify.Action {
	t.Helper()
	for _, action := range verification.Actions {
		if action.Name == name {
			return action
		}
	}
	require.FailNow(t, "verification action is missing", name)
	return verify.Action{}
}

func requireProperty(t *testing.T, verification verify.Model, name string) verify.Property {
	t.Helper()
	for _, property := range verification.Properties {
		if property.Name == name {
			return property
		}
	}
	require.FailNow(t, "verification property is missing", name)
	return verify.Property{}
}

func requireRefinement(t *testing.T, verification verify.Model, action string) verify.Refinement {
	t.Helper()
	for _, refinement := range verification.Refinements {
		if refinement.Action == action {
			return refinement
		}
	}
	require.FailNow(t, "verification refinement is missing", action)
	return verify.Refinement{}
}

func requireInventoryItem(t *testing.T, verification verify.Model, kind, name string) verify.InventoryItem {
	t.Helper()
	for _, item := range verification.Inventory {
		if item.Kind == kind && item.Name == name {
			return item
		}
	}
	require.FailNow(t, "verification inventory item is missing", kind+":"+name)
	return verify.InventoryItem{}
}

func requireVerificationTarget(t *testing.T, family verify.ModelFamily, name string) verify.VerificationTarget {
	t.Helper()
	for _, target := range family.Targets {
		if target.Name == name {
			return target
		}
	}
	require.FailNow(t, "verification target is missing", name)
	return verify.VerificationTarget{}
}

func violationProperties(violations []verify.PropertyViolation) []string {
	result := make([]string, len(violations))
	for index, violation := range violations {
		result[index] = violation.Property
	}
	return result
}

func foundationDeliveryModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, foundationDeliveryTarget)
	require.NoError(t, err)
	return projection.Model
}

func workflowDeliveryModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 2})
	require.NoError(t, err)
	projection, err := verify.Project(family, workflowDeliveryTarget)
	require.NoError(t, err)
	return projection.Model
}

func activityDeliveryModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, activityDeliveryTarget)
	require.NoError(t, err)
	return projection.Model
}

func workflowTaskSpeculativeModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, workflowTaskSpeculativeTarget)
	require.NoError(t, err)
	return projection.Model
}

func nexusFeatureModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, nexusFeatureTarget)
	require.NoError(t, err)
	return projection.Model
}

func callbackTargetModel(t *testing.T, target string) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, target)
	require.NoError(t, err)
	return projection.Model
}

func deliveryRoutingModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, deliveryRoutingTarget)
	require.NoError(t, err)
	return projection.Model
}

func deliveryBacklogModel(t *testing.T) verify.Model {
	t.Helper()
	protocol, err := Default()
	require.NoError(t, err)
	family, err := protocol.VerificationFamily(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)
	projection, err := verify.Project(family, deliveryBacklogTarget)
	require.NoError(t, err)
	return projection.Model
}

func backlogAcknowledgedTrace() []verify.TraceStep {
	return []verify.TraceStep{
		foundationPersistTrace()[0],
		{Action: "delivery.spool", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
		{Action: "backlog.append-first", Bindings: verify.Bindings{"position": "BacklogPosition#0", "task": "DeliveryTask#0"}},
		{Action: "backlog.read", Bindings: verify.Bindings{"position": "BacklogPosition#0"}},
		{Action: "delivery.reserve", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0"}},
		{Action: "delivery.authorize.accept", Bindings: verify.Bindings{"obligation": "WorkObligation#0", "task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
		{Action: "delivery.dispatch", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
		{Action: "delivery.acknowledge", Bindings: verify.Bindings{"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0"}},
		{Action: "backlog.ack", Bindings: verify.Bindings{"position": "BacklogPosition#0", "task": "DeliveryTask#0"}},
	}
}

func deliveryWrongRouteTrace() []verify.TraceStep {
	return []verify.TraceStep{
		{Action: "routing.bootstrap-history-owner", Bindings: verify.Bindings{"shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#1", "partition": "MatchingQueuePartition#1", "generation": "MatchingOwnerGeneration#1"}},
		foundationPersistTrace()[0],
		{Action: "routing.forward-to-matching", Bindings: verify.Bindings{"task": "DeliveryTask#0", "route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0", "shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.register-poller", Bindings: verify.Bindings{"poller": "Poller#0", "route": "DeliveryRouteClass#1"}},
		{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
		{Action: "routing.reserve-compatible", Bindings: verify.Bindings{
			"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0",
			"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0",
		}},
	}
}

func deliveryCompatibleReservationTrace() []verify.TraceStep {
	return []verify.TraceStep{
		{Action: "routing.bootstrap-history-owner", Bindings: verify.Bindings{"shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.bootstrap", Bindings: verify.Bindings{"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0"}},
		foundationPersistTrace()[0],
		{Action: "routing.forward-to-matching", Bindings: verify.Bindings{"task": "DeliveryTask#0", "route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0", "shard": "HistoryShard#0", "historyGeneration": "HistoryOwnerGeneration#0"}},
		{Action: "routing.register-poller", Bindings: verify.Bindings{"poller": "Poller#0", "route": "DeliveryRouteClass#0"}},
		{Action: "delivery.offer-sync", Bindings: verify.Bindings{"task": "DeliveryTask#0"}},
		{Action: "routing.reserve-compatible", Bindings: verify.Bindings{
			"task": "DeliveryTask#0", "attempt": "DeliveryAttempt#0", "poller": "Poller#0",
			"route": "DeliveryRouteClass#0", "partition": "MatchingQueuePartition#0", "generation": "MatchingOwnerGeneration#0",
		}},
	}
}

func callbackCloseWithPendingDeliveryTrace() []verify.TraceStep {
	return []verify.TraceStep{
		{Action: "callback.handler.start", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
		{Action: "callback.attach-handler", Bindings: verify.Bindings{"callback": "Callback#0", "handlerRun": "WorkflowRun#0"}},
		{Action: "callback.delivery.enqueue", Bindings: verify.Bindings{"callback": "Callback#0", "delivery": "CallbackDelivery#0"}},
		{Action: "callback.handler.close.complete", Bindings: verify.Bindings{"entity": "WorkflowRun#0"}},
	}
}

func nexusCloseWhileActiveTrace() []verify.TraceStep {
	return []verify.TraceStep{
		{Action: "nexus.workflow.start", Bindings: verify.Bindings{"wf": "Workflow#0"}},
		{Action: "nexus.operation.schedule.Standalone", Bindings: verify.Bindings{"op": "NexusOperation#0", "workflow": "Workflow#0"}},
		{Action: "nexus.workflow.close.complete", Bindings: verify.Bindings{"wf": "Workflow#0"}},
	}
}

func workflowTaskDuplicateCreationTrace() []verify.TraceStep {
	return []verify.TraceStep{
		{
			Action: "workflow.task.create-normal",
			Bindings: verify.Bindings{
				"run":        "WorkflowRun#0",
				"entity":     "WorkflowTask#0",
				"obligation": "WorkObligation#0",
				"task":       "DeliveryTask#0",
				"queue":      "DeliveryQueue#0",
			},
		},
		{
			Action: "workflow.task.create-speculative-direct",
			Bindings: verify.Bindings{
				"run":        "WorkflowRun#0",
				"entity":     "WorkflowTask#1",
				"obligation": "WorkObligation#1",
				"task":       "DeliveryTask#1",
				"queue":      "DeliveryQueue#0",
			},
		},
	}
}

func foundationPersistTrace() []verify.TraceStep {
	return []verify.TraceStep{{
		Action: "delivery.persist.success",
		Bindings: verify.Bindings{
			"obligation": "WorkObligation#0",
			"task":       "DeliveryTask#0",
			"queue":      "DeliveryQueue#0",
		},
	}}
}

func replayFoundationTrace(t *testing.T, model verify.Model, traces ...[]verify.TraceStep) verify.ModelState {
	t.Helper()
	var trace []verify.TraceStep
	for _, steps := range traces {
		trace = append(trace, steps...)
	}
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	states, err := interpreter.Replay(trace)
	require.NoError(t, err)
	require.Len(t, states, 1)
	return states[0]
}

func requireFoundationPropertyFails(t *testing.T, model verify.Model, state verify.ModelState, name string) {
	t.Helper()
	property := requireProperty(t, model, name)
	holds, err := verify.EvaluateExpr(model, state, property.Expr, nil)
	require.NoError(t, err)
	require.False(t, holds)
}

func removeActionGuardRelation(t *testing.T, model *verify.Model, actionName, relation string) {
	t.Helper()
	action := requireAction(t, *model, actionName)
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != action.Name {
			continue
		}
		var guards []verify.Expr
		for _, guard := range model.Actions[actionIndex].Guard.Args {
			if guard.Op != verify.RelationHasExpr || guard.Relation != relation {
				guards = append(guards, guard)
			}
		}
		model.Actions[actionIndex].Guard.Args = guards
		return
	}
	require.FailNow(t, "verification action is missing", actionName)
}

func removeActionGuardOp(t *testing.T, model *verify.Model, actionName string, operation verify.ExprOp) {
	t.Helper()
	for actionIndex := range model.Actions {
		if model.Actions[actionIndex].Name != actionName {
			continue
		}
		var guards []verify.Expr
		for _, guard := range model.Actions[actionIndex].Guard.Args {
			if guard.Op != operation {
				guards = append(guards, guard)
			}
		}
		model.Actions[actionIndex].Guard.Args = guards
		return
	}
	require.FailNow(t, "verification action is missing", actionName)
}

func (p *Protocol) mustLifecycle(t *testing.T, entityType umpire.EntityType) *umpire.Lifecycle {
	t.Helper()
	lifecycle, found := p.Lifecycle(entityType)
	require.True(t, found)
	return lifecycle
}
