package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

// appliedSizeUnknown forces the first decision to resize.
const appliedSizeUnknown = -1

var (
	_ chasm.Component                              = (*ScalingGroup)(nil)
	_ chasm.StateMachine[wcipb.ScalingGroupStatus] = (*ScalingGroup)(nil)
)

// ScalingGroup holds one group's runtime state; its spec lives on the root.
type ScalingGroup struct {
	chasm.UnimplementedComponent

	*wcipb.ScalingGroupState

	Root chasm.ParentPtr[*WorkerControllerInstance]
}

func NewScalingGroup(id string) *ScalingGroup {
	return &ScalingGroup{
		ScalingGroupState: &wcipb.ScalingGroupState{
			Id:          id,
			Status:      wcipb.SCALING_GROUP_STATUS_IDLE,
			AppliedSize: appliedSizeUnknown,
		},
	}
}

// rootRunning reports whether the root still runs scaling; without a tree (unit tests) there is no
// parent to check. Closed roots already invalidate child tasks in the framework.
func (g *ScalingGroup) rootRunning(ctx chasm.Context) bool {
	root, ok := g.Root.TryGet(ctx)
	return !ok || root.Status == wcipb.WCI_STATUS_RUNNING
}

// LifecycleState is always running: groups live as long as the root.
func (g *ScalingGroup) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (g *ScalingGroup) StateMachineState() wcipb.ScalingGroupStatus {
	return g.Status
}

func (g *ScalingGroup) SetStateMachineState(status wcipb.ScalingGroupStatus) {
	g.Status = status
}

// Decide records an UpdateWorkerSetSize decision; only an IDLE group starts a resize, a busy one
// picks the new size up when its resize completes.
func (g *ScalingGroup) Decide(ctx chasm.MutableContext, desiredSize int32, providerType string) error {
	if g.Status != wcipb.SCALING_GROUP_STATUS_IDLE {
		g.DesiredSize = desiredSize
		return nil
	}
	return TransitionScaleDecided.Apply(g, ctx, EventScaleDecided{DesiredSize: desiredSize, ProviderType: providerType})
}
