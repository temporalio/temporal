package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/proto"
)

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
