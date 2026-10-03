package fc

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching/simplelimiter"
)

var errCommitFailure = serviceerror.NewFailedPrecondition("commit failed")
var errInvalidTxState = serviceerror.NewInternal("invalid fc tx state")

// A client of the flow control system should call NewTx when it it's ready to match a task, to
// perform the flow control commit protocol. It should then call:
//   - tx.Check() -> checks readiness, if not ready atomically registers cb to be called when
//     possibly ready
//   - tx.Reserve(ctx) -> on error retry the task
//   - tx.LimiterRefs() to get refs to pass to history (for releasing later)
//   - history.RecordTaskStarted(refs) -> on error call tx.Rollback()
//   - tx.Commit(ctx) -> on error DROP the task
//
// A nil *Tx is valid and all operations are no-ops.
// Tx is not safe for concurrent use.
// TODO(fc): consider getting rid of fcTask and passing []Limiters and (int32,time.Time)?
// but may need task identity for choosing slot id
func (r *Readiness) NewTx(nsID namespace.ID, task fcTask, cb ReadinessCallback) *Tx {
	lims := canonicalLimiters(task)
	if len(lims) == 0 {
		return nil
	}

	limiterTxs := make([]limiterTx, len(lims))
	refs := make([]*taskqueuespb.LimiterRef, 0, len(lims))
	for i, lim := range lims {
		ltx, ref := r.makeLimiterTx(nsID, task, lim)
		limiterTxs[i] = ltx
		if ref != nil {
			refs = append(refs, ref)
		}
	}

	return &Tx{
		limiters: limiterTxs,
		refs:     refs,
		cb:       cb,
	}
}

func (r *Readiness) makeLimiterTx(nsID namespace.ID, task fcTask, lim Limiter) (limiterTx, *taskqueuespb.LimiterRef) {
	switch lim.Type {
	case enumsspb.LIMITER_TYPE_CONCURRENCY:
		cs := r.getConcurrencyLimiter(nsID, lim.Key)

		// if config is missing or wrong type, just leave it out
		config, _ := lim.Config.(*taskqueuepb.ConcurrencyLimit)
		pri := makeWakePriority(task.PriorityAndAge())
		// TODO(fc): consider deriving slot id from task to fix some nongraceful failover situations
		slotID := uuid.NewString()

		tx := newConcurrencyTx(cs, slotID, config, lim.ConfigVersion, pri)
		ref := &taskqueuespb.LimiterRef{LimiterType: lim.Type, Key: lim.Key, SlotId: slotID}
		return tx, ref

	case enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT:
		lls := r.getLocalLimiter(nsID, lim.Key)

		// if config is missing or wrong type, zero params means "no limit"
		config, _ := lim.Config.(simplelimiter.Params)
		pri := makeWakePriority(task.PriorityAndAge())

		return newLocalLimiterTx(lls, config, pri), nil

	default:
		// TODO(fc): log or notify here
		return noopLimiterTx{}, nil
	}
}

// Holds state for an invocation of the flow control commit protocol. See Readiness.NewTx.
type Tx struct {
	limiters []limiterTx
	refs     []*taskqueuespb.LimiterRef
	cb       ReadinessCallback
	state    [MaxLimiters]txState
}

type limiterTx interface {
	check(ReadinessCallback) error
	cancelCheck()
	reserve(context.Context) error
	commit(context.Context) error
	cancelReserve(context.Context)
}

type txState int8 // just so we can pack these in an array

const (
	txStateInit txState = iota
	txStateChecked
	txStateReserved
	txStateCommitted
	txStateCommitFailed
	txStateCanceled
)

// LimiterRefs returns references to limiters that should be passed to history and stored with
// activity/workflow task state.
func (tx *Tx) LimiterRefs() []*taskqueuespb.LimiterRef {
	if tx == nil {
		return nil
	}
	return tx.refs
}

// Check checks and consumes local tokens for each limiter.
// // ReadinessState gets the readiness state of a limiter. If it's blocked and cb is not nil,
// // cb.OnReady will be called once when the state of the limiter transitions to ready. If it is
// // ready, the callback will be removed from the limiter
// TODO(fc): comment more here
func (tx *Tx) Check() (retErr error) {
	if tx == nil {
		return nil // no limiters
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(nil)
		}
	}()

	for i, lim := range tx.limiters {
		if tx.state[i] != txStateInit {
			return errInvalidTxState
		}
		if err := lim.check(tx.cb); err != nil {
			return err
		}
		tx.state[i] = txStateChecked
	}
	return nil
}

// Reserve checks that all limiters can be satisfied now.
// Reserve may make RPC calls.
func (tx *Tx) Reserve(ctx context.Context) (retErr error) {
	if tx == nil {
		return nil // no limiters
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(ctx)
		}
	}()

	// reserve must be sequential
	for i, lim := range tx.limiters {
		if tx.state[i] != txStateChecked {
			return errInvalidTxState
		}
		if err := lim.reserve(ctx); err != nil {
			return err
		}
		tx.state[i] = txStateReserved
	}
	return nil
}

// Commit turns reservations into committed slots.
// Reserve may make RPC calls.
func (tx *Tx) Commit(ctx context.Context) (retErr error) {
	if tx == nil {
		return nil // no limiters
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(ctx)
		}
	}()

	// commit all concurrently
	var wg sync.WaitGroup
	errs := make([]error, len(tx.limiters))
	for i, lim := range tx.limiters {
		if tx.state[i] != txStateReserved {
			return errInvalidTxState
		}
		if i == len(tx.limiters)-1 {
			errs[i] = lim.commit(ctx)
		} else {
			wg.Go(func() { errs[i] = lim.commit(ctx) })
		}
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			tx.state[i] = txStateCommitFailed
		} else {
			tx.state[i] = txStateCommitted
		}
	}
	return errors.Join(errs...)
}

// Rollback cancels checks and reservations on any limiters that have been made and not
// committed so far. ctx may be nil only if Reserve has not been called yet.
func (tx *Tx) Rollback(ctx context.Context) {
	if tx == nil {
		return // no limiters
	}
	for i, lim := range tx.limiters {
		switch tx.state[i] {
		case txStateReserved:
			// this is best-effort, reservations have timeouts so it's okay if we fail to cancel
			lim.cancelReserve(ctx)
			fallthrough
		case txStateChecked:
			lim.cancelCheck()
			tx.state[i] = txStateCanceled
		}
	}
}

type noopLimiterTx struct{}

func (noopLimiterTx) check(cb ReadinessCallback) error { return nil }
func (noopLimiterTx) cancelCheck()                     {}
func (noopLimiterTx) reserve(context.Context) error    { return nil }
func (noopLimiterTx) commit(context.Context) error     { return nil }
func (noopLimiterTx) cancelReserve(context.Context)    {}
