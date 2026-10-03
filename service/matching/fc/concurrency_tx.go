package fc

import (
	"context"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

type concurrencyTx struct {
	cs                  *concurrencyState
	slotID              string
	configUpdate        *taskqueuepb.ConcurrencyLimit
	configUpdateVersion int64
	pri                 wakePriority
	// once we've called reserve, the slots hint from the server supersedes our check token
	calledReserve bool
}

func newConcurrencyTx(
	cs *concurrencyState,
	slotID string,
	configUpdate *taskqueuepb.ConcurrencyLimit,
	configUpdateVersion int64,
	pri wakePriority,
) *concurrencyTx {
	return &concurrencyTx{
		cs:                  cs,
		slotID:              slotID,
		configUpdate:        configUpdate,
		configUpdateVersion: configUpdateVersion,
		pri:                 pri,
	}
}

func (ct *concurrencyTx) check(cb ReadinessCallback) error {
	return ct.cs.check(cb, ct.pri)
}

func (ct *concurrencyTx) cancelCheck() {
	if !ct.calledReserve {
		ct.cs.cancelCheck()
	}
}

func (ct *concurrencyTx) reserve(ctx context.Context) error {
	ct.calledReserve = true
	return ct.cs.reserve(ctx, ct.slotID, ct.configUpdate, ct.configUpdateVersion)
}

func (ct *concurrencyTx) commit(ctx context.Context) error {
	return ct.cs.commit(ctx, ct.slotID)
}

func (ct *concurrencyTx) cancelReserve(ctx context.Context) {
	ct.cs.cancelReserve(ctx, ct.slotID)
}
