package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// MetricsPollTimerTaskHandler fires at next_poll_time and hands the I/O to FetchSnapshotTask.
type MetricsPollTimerTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *MetricsPollTimerTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.MetricsPollTimerTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetPollTime(), w.NextPollTime), nil
}

func (h *MetricsPollTimerTaskHandler) Execute(
	ctx chasm.MutableContext,
	w *WorkerControllerInstance,
	_ chasm.TaskAttributes,
	task *wcipb.MetricsPollTimerTask,
) error {
	ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.FetchSnapshotTask{PollTime: task.GetPollTime()})
	return nil
}

// PeriodicValidationTimerTaskHandler re-arms first, so a dropped validation never stops the cadence.
type PeriodicValidationTimerTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *PeriodicValidationTimerTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.PeriodicValidationTimerTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetValidationTime(), w.NextValidationTime), nil
}

func (h *PeriodicValidationTimerTaskHandler) Execute(
	ctx chasm.MutableContext,
	w *WorkerControllerInstance,
	_ chasm.TaskAttributes,
	_ *wcipb.PeriodicValidationTimerTask,
) error {
	w.armValidationTimer(ctx, ctx.Now(w).Add(validationInterval))
	ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.ValidateSpecTask{ValidationTime: common.CloneProto(w.NextValidationTime)})
	return nil
}

// PendingExpiryTaskHandler aborts a pending migration target its coordinator never activated.
type PendingExpiryTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *PendingExpiryTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.PendingExpiryTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_PENDING_ACTIVATION && proto.Equal(task.GetExpireTime(), w.PendingExpireTime), nil
}

func (h *PendingExpiryTaskHandler) Execute(
	ctx chasm.MutableContext,
	w *WorkerControllerInstance,
	_ chasm.TaskAttributes,
	_ *wcipb.PendingExpiryTask,
) error {
	return TransitionPendingMigrationAborted.Apply(w, ctx, EventPendingMigrationAborted{})
}
