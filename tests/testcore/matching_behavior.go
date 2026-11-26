package testcore

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/testx/failpoint"
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
	// Without ForceAsync the failpoint doesn't fire, but its registration keeps BUGGIFY from
	// skipping the sync match.
	registry := FailpointRegistry(env.GetTestCluster())
	key := matching.SkipSyncMatchKey.ForScope(env.NamespaceID().String())
	fp := failpoint.NewFailpoint(key)
	if b.ForceAsync {
		fp.On()
	}
	registry.Set(fp)
	env.T().Cleanup(func() {
		registry.Delete(key)
		fp.Disable()
	})
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
