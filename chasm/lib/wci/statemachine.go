package wci

import (
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---- Root ----

// EventMigrateToWorkflow freezes the instance for a CHASM → workflow handover.
type EventMigrateToWorkflow struct {
	Token string
}

var TransitionMigrateToWorkflow = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING},
	wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW,
	func(w *WorkerControllerInstance, ctx chasm.MutableContext, event EventMigrateToWorkflow) error {
		w.MigrationToken = event.Token
		ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.MigrateToWorkflowTask{})
		return nil
	},
)

// EventWorkflowActivated means the pending workflow accepted activation and owns the WCI now.
type EventWorkflowActivated struct{}

var TransitionWorkflowActivated = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW},
	wcipb.WCI_STATUS_MIGRATED,
	func(*WorkerControllerInstance, chasm.MutableContext, EventWorkflowActivated) error { return nil },
)

// EventWorkflowMigrationAborted resumes scaling on CHASM after a failed CHASM → workflow handover.
type EventWorkflowMigrationAborted struct{}

var TransitionWorkflowMigrationAborted = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW},
	wcipb.WCI_STATUS_RUNNING,
	func(w *WorkerControllerInstance, ctx chasm.MutableContext, _ EventWorkflowMigrationAborted) error {
		w.MigrationToken = ""
		w.armTimersFromState(ctx)
		return nil
	},
)

// EventMigrationActivated commits a workflow → CHASM handover; the caller replays queued task-adds after.
type EventMigrationActivated struct{}

var TransitionMigrationActivated = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_PENDING_ACTIVATION},
	wcipb.WCI_STATUS_RUNNING,
	func(w *WorkerControllerInstance, ctx chasm.MutableContext, _ EventMigrationActivated) error {
		w.PendingExpireTime = nil
		w.armTimersFromState(ctx)
		wcimetrics.InstanceCreated.With(metricsFor(ctx, w, "")).Record(1)
		return nil
	},
)

// EventPendingMigrationAborted closes a pending target on abort or expiry.
type EventPendingMigrationAborted struct{}

var TransitionPendingMigrationAborted = chasm.NewTransition(
	[]wcipb.WciStatus{wcipb.WCI_STATUS_PENDING_ACTIVATION},
	wcipb.WCI_STATUS_MIGRATION_ABORTED,
	func(w *WorkerControllerInstance, _ chasm.MutableContext, _ EventPendingMigrationAborted) error {
		w.PendingExpireTime = nil
		return nil
	},
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
