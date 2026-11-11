package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/proto"
)

// ActivationDeadlineTaskHandler aborts a pending migration target its coordinator never activated.
type ActivationDeadlineTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *ActivationDeadlineTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.ActivationDeadlineTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_PENDING_ACTIVATION && proto.Equal(task.GetDeadline(), w.ActivationDeadline), nil
}

func (h *ActivationDeadlineTaskHandler) Execute(
	ctx chasm.MutableContext,
	w *WorkerControllerInstance,
	_ chasm.TaskAttributes,
	_ *wcipb.ActivationDeadlineTask,
) error {
	return TransitionPendingMigrationAborted.Apply(w, ctx, EventPendingMigrationAborted{})
}
