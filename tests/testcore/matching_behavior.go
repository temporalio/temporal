package testcore

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testhooks"
)

// MatchingBehavior describes a test scenario for matching service behavior.
type MatchingBehavior struct {
	ForceTaskForward bool
	ForcePollForward bool
	ForceAsync       bool
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
	var opts []TestOption
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
func (b MatchingBehavior) InjectHooks(env Env) {
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
		env.InjectHook(testhooks.NewHook(testhooks.MatchingDisableSyncMatch, true))
	} else {
		env.InjectHook(testhooks.NewHook(testhooks.MatchingDisableSyncMatch, false))
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
