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
	require.Empty(t, callback.Initial)
	require.Empty(t, callback.States)

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
	require.Len(t, report.OmittedActions, len(foundationDeliveryVerification().Model.Actions))
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
	require.False(t, requireInventoryItem(t, verification, "regression-action", "nexus.timeout").Included)
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

func (p *Protocol) mustLifecycle(t *testing.T, entityType umpire.EntityType) *umpire.Lifecycle {
	t.Helper()
	lifecycle, found := p.Lifecycle(entityType)
	require.True(t, found)
	return lifecycle
}
