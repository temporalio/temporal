package fc

import (
	"context"

	"github.com/google/uuid"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

type concurrencyTx struct {
	cs                  *concurrencyState
	slotID              string // generated lazily, use getSlotID
	configUpdate        *taskqueuepb.ConcurrencyLimit
	configUpdateVersion int64
	pri                 wakePriority
	checkSeq            int64
}

func newConcurrencyTx(
	cs *concurrencyState,
	configUpdate *taskqueuepb.ConcurrencyLimit,
	configUpdateVersion int64,
	pri wakePriority,
) *concurrencyTx {
	return &concurrencyTx{
		cs:                  cs,
		configUpdate:        configUpdate,
		configUpdateVersion: configUpdateVersion,
		pri:                 pri,
	}
}

// getSlotID returns the slot id for this tx. It's generated lazily so we don't create it until
// we get to Reserve.
func (ct *concurrencyTx) getSlotID() string {
	if ct.slotID == "" {
		// TODO(fc): consider deriving slot id from task to fix some nongraceful failover situations
		ct.slotID = uuid.NewString()
	}
	return ct.slotID
}

func (ct *concurrencyTx) check(cb ReadinessCallback) error {
	var err error
	ct.checkSeq, err = ct.cs.check(cb, ct.pri)
	return err
}

func (ct *concurrencyTx) cancelCheck() {
	ct.cs.cancelCheck(ct.checkSeq)
}

func (ct *concurrencyTx) reserve(ctx context.Context) error {
	return ct.cs.reserve(ctx, ct.getSlotID(), ct.configUpdate, ct.configUpdateVersion)
}

func (ct *concurrencyTx) commit(ctx context.Context) error {
	return ct.cs.commit(ctx, ct.getSlotID())
}

func (ct *concurrencyTx) cancelReserve(ctx context.Context) {
	ct.cs.cancelReserve(ctx, ct.getSlotID())
}

func (ct *concurrencyTx) close() {
}

func (ct *concurrencyTx) ref() *taskqueuespb.LimiterRef {
	return &taskqueuespb.LimiterRef{
		LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY,
		Key:         ct.cs.limiterKey(),
		SlotId:      ct.getSlotID(),
	}
}
