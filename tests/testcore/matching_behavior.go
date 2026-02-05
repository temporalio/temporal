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

// Apply applies the behavior's dynamic config and test hooks to the environment.
func (b MatchingBehavior) Apply(env Env) {
	// Apply dynamic config
	if b.ForceTaskForward || b.ForcePollForward {
		env.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
		env.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
	} else {
		env.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
		env.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	}

	// Inject test hooks
	if b.ForceTaskForward {
		env.InjectHook(testhooks.MatchingLBForceWritePartition, 11)
	} else {
		env.InjectHook(testhooks.MatchingLBForceWritePartition, 0)
	}
	if b.ForcePollForward {
		env.InjectHook(testhooks.MatchingLBForceReadPartition, 5)
	} else {
		env.InjectHook(testhooks.MatchingLBForceReadPartition, 0)
	}
	if b.ForceAsync {
		env.InjectHook(testhooks.MatchingDisableSyncMatch, true)
	} else {
		env.InjectHook(testhooks.MatchingDisableSyncMatch, false)
	}
}

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
