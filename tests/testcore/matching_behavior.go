package testcore

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/matching"
)

// MatchingBehavior describes a test scenario for matching service behavior.
type MatchingBehavior struct {
	ForceTaskForward bool
	ForcePollForward bool
	ForceAsync       bool
}

type hookInjector interface {
	InjectHook(hook testhooks.Hook) (cleanup func())
	T() *testing.T
	NamespaceID() namespace.ID
	GetTestCluster() *TestCluster
}

// Name returns a descriptive name for this behavior combination.
func (b MatchingBehavior) Name() string {
	name := "NoTaskForward"
	if b.ForceTaskForward {
		name = "ForceTaskForward"
	}
	if b.ForcePollForward {
		name += "ForcePollForward"
	} else {
		name += "NoPollForward"
	}
	if b.ForceAsync {
		name += "ForceAsync"
	} else {
		name += "AllowSync"
	}
	return name
}

// Options returns the TestOptions to configure matching behavior.
func (b MatchingBehavior) Options() []TestOption {
	opts := []TestOption{
		WithDynamicConfig(dynamicconfig.MatchingForwarderMaxChildrenPerNode, 3),
	}
	if b.ForceTaskForward || b.ForcePollForward {
		opts = append(opts,
			WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13),
			WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13),
		)
	} else {
		opts = append(opts,
			WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
			WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		)
	}
	return opts
}

// InjectHooks injects the test hooks for this matching behavior.
func (b MatchingBehavior) InjectHooks(env hookInjector) {
	if b.ForceTaskForward {
		env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, 11))
	} else {
		env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, 0))
	}
	if b.ForcePollForward {
		env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, 5))
	} else {
		env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, 0))
	}
	if b.ForceAsync {
		// The tests of the matching behaviors don't all dispatch a task that history adds, such as the
		// Nexus ones, so the failpoint may not fire.
		InjectFailpointIn(env.T(), env.GetTestCluster(),
			matching.SkipSyncMatchKey.ForScope(env.NamespaceID().String()), MayNotFire()).On()
	}
}

// AllMatchingBehaviors returns all 8 combinations of matching behaviors for testing.
func AllMatchingBehaviors() []MatchingBehavior {
	var behaviors []MatchingBehavior
	for _, forcePollForward := range []bool{false, true} {
		for _, forceTaskForward := range []bool{false, true} {
			for _, forceAsync := range []bool{false, true} {
				behaviors = append(behaviors, MatchingBehavior{
					ForceTaskForward: forceTaskForward,
					ForcePollForward: forcePollForward,
					ForceAsync:       forceAsync,
				})
			}
		}
	}
	return behaviors
}
