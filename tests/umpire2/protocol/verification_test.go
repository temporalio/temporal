package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

func TestVerificationModelLowersCanonicalProtocol(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{
		DefaultBound: 1,
		Bounds:       map[umpire.EntityType]int{model.NexusOperationType: 2},
	})
	require.NoError(t, err)
	require.NoError(t, verify.Validate(verification))

	nexus := requireEntity(t, verification, string(model.NexusOperationType))
	require.Equal(t, []string{"NexusOperation#0", "NexusOperation#1"}, nexus.IDs)
	require.Equal(t, model.NexusUnspecified, nexus.Initial)
	require.Contains(t, nexus.States, verify.State{Name: model.NexusScheduled, MustProgress: true})

	callback := requireEntity(t, verification, string(model.CallbackType))
	require.Empty(t, callback.Initial)
	require.Empty(t, callback.States)

	relation := requireRelation(t, verification, string(CallbackOperationRelation))
	require.Equal(t, string(model.CallbackType), relation.Source)
	require.Equal(t, string(model.NexusOperationType), relation.Target)
	require.Equal(t, verify.One, relation.SourceCardinality)
	require.Equal(t, verify.Many, relation.TargetCardinality)
}

func TestVerificationModelMakesLifecycleContextPartOfEveryAction(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	verification, err := protocol.VerificationModel(VerificationOptions{DefaultBound: 1})
	require.NoError(t, err)

	schedule := requireAction(t, verification, "NexusOperation.unspecified.schedule.Standalone")
	require.Equal(t, verify.StateIs("NexusOperation", "op", model.NexusUnspecified), schedule.Guard)
	require.Equal(t, []verify.Effect{{
		Kind:   verify.CreateEffect,
		Entity: "NexusOperation",
		Ref:    "op",
		State:  model.NexusScheduled,
	}}, schedule.Effects)
	require.Equal(t, []string{umpire.RPCDrive.String()}, schedule.Capabilities)
	require.False(t, schedule.Unrealized)

	timeoutGap := requireAction(t, verification, "NexusOperation.started.timeout.Standalone")
	require.True(t, timeoutGap.Unrealized)
	require.Equal(t, verify.StateIs("NexusOperation", "entity", model.NexusStarted), timeoutGap.Guard)
	require.Equal(t, []verify.Effect{{
		Kind:   verify.SetStateEffect,
		Entity: "NexusOperation",
		Ref:    "entity",
		State:  model.NexusTimedOut,
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
				verify.StateIs("NexusOperation", "entity", model.NexusScheduled),
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
		model.NexusOperationType: protocol.mustLifecycle(t, model.NexusOperationType),
		model.ActivityType:       protocol.mustLifecycle(t, model.ActivityType),
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

func (p *Protocol) mustLifecycle(t *testing.T, entityType umpire.EntityType) *umpire.Lifecycle {
	t.Helper()
	lifecycle, found := p.Lifecycle(entityType)
	require.True(t, found)
	return lifecycle
}
