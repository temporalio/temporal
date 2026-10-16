package wci

import (
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---- Root ----

type EventMigrateToWorkflow struct{}

var TransitionMigrateToWorkflow = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING},
	wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW,
	func(w *WorkerControllerInstance, ctx chasm.MutableContext, _ EventMigrateToWorkflow) error {
		ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.MigrateToWorkflowTask{})
		return nil
	},
)

type EventWorkflowStarted struct{}

var TransitionWorkflowStarted = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW},
	wcipb.WCI_STATUS_MIGRATED,
	func(*WorkerControllerInstance, chasm.MutableContext, EventWorkflowStarted) error { return nil },
)

// EventDelete covers the Delete RPC and an Update that leaves zero groups.
type EventDelete struct {
	Identity string
}

var TransitionDelete = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING},
	wcipb.WCI_STATUS_DELETED,
	func(w *WorkerControllerInstance, _ chasm.MutableContext, event EventDelete) error {
		w.LastModifierIdentity = event.Identity
		return nil
	},
)

// ---- ScalingGroup (UpdateWorkerSetSize only) ----

type EventScaleDecided struct {
	DesiredSize  int32
	ProviderType string
}

var TransitionScaleDecided = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_IDLE},
	wcipb.SCALING_GROUP_STATUS_SCALING,
	func(g *ScalingGroup, ctx chasm.MutableContext, event EventScaleDecided) error {
		g.DesiredSize = event.DesiredSize
		g.Attempt = 1
		g.scheduleResize(ctx, event.ProviderType)
		return nil
	},
)

type EventResizeSucceeded struct {
	AppliedSize int32
}

var TransitionResizeSucceeded = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_SCALING},
	wcipb.SCALING_GROUP_STATUS_IDLE,
	func(g *ScalingGroup, _ chasm.MutableContext, event EventResizeSucceeded) error {
		g.AppliedSize = event.AppliedSize
		g.Attempt = 0
		g.LastFailure = nil
		return nil
	},
)

// EventResizeSucceededStale means the resize landed but desired_size moved meanwhile.
type EventResizeSucceededStale struct {
	AppliedSize  int32
	ProviderType string
}

var TransitionResizeSucceededStale = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_SCALING},
	wcipb.SCALING_GROUP_STATUS_SCALING,
	func(g *ScalingGroup, ctx chasm.MutableContext, event EventResizeSucceededStale) error {
		g.AppliedSize = event.AppliedSize
		g.Attempt++
		g.scheduleResize(ctx, event.ProviderType)
		return nil
	},
)

type EventResizeRetryable struct {
	Failure         *failurepb.Failure
	NextAttemptTime time.Time
}

var TransitionResizeRetryable = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_SCALING},
	wcipb.SCALING_GROUP_STATUS_BACKING_OFF,
	func(g *ScalingGroup, ctx chasm.MutableContext, event EventResizeRetryable) error {
		g.LastFailure = event.Failure
		g.NextAttemptTime = timestamppb.New(event.NextAttemptTime)
		ctx.AddTask(g, chasm.TaskAttributes{ScheduledTime: event.NextAttemptTime}, &wcipb.ScalingGroupBackoffTask{Attempt: g.Attempt})
		return nil
	},
)

type EventBackoffElapsed struct {
	ProviderType string
}

var TransitionBackoffElapsed = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_BACKING_OFF},
	wcipb.SCALING_GROUP_STATUS_SCALING,
	func(g *ScalingGroup, ctx chasm.MutableContext, event EventBackoffElapsed) error {
		g.NextAttemptTime = nil
		g.Attempt++
		g.scheduleResize(ctx, event.ProviderType)
		return nil
	},
)

// EventResizeFailed is non-retryable; the caller also marks validation failed on the root.
type EventResizeFailed struct {
	Failure *failurepb.Failure
}

var TransitionResizeFailed = chasm.NewTransition(
	[]wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_SCALING},
	wcipb.SCALING_GROUP_STATUS_IDLE,
	func(g *ScalingGroup, _ chasm.MutableContext, event EventResizeFailed) error {
		g.LastFailure = event.Failure
		g.Attempt = 0
		return nil
	},
)

// CompleteResize picks Succeeded or SucceededStale by comparing desired with applied.
func (g *ScalingGroup) CompleteResize(ctx chasm.MutableContext, appliedSize int32, providerType string) error {
	if g.DesiredSize == appliedSize {
		return TransitionResizeSucceeded.Apply(g, ctx, EventResizeSucceeded{AppliedSize: appliedSize})
	}
	return TransitionResizeSucceededStale.Apply(g, ctx, EventResizeSucceededStale{AppliedSize: appliedSize, ProviderType: providerType})
}

func (g *ScalingGroup) scheduleResize(ctx chasm.MutableContext, providerType string) {
	ctx.AddTask(g, chasm.TaskAttributes{Destination: providerType}, &wcipb.ResizeTask{Attempt: g.Attempt})
}
