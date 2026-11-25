//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination tx_mock_test.go

package fc

import (
	"context"
	"errors"
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching/simplelimiter"
)

var errInvalidTxState = serviceerror.NewInternal("invalid fc tx state")

// NewTx should be called when a client of the flow control system is ready to match a task, to
// perform the flow control commit protocol. It should then call:
//   - tx.Check() -> checks readiness, if not ready atomically registers cb to be called when
//     possibly ready (or tx.SkipCheck() for a task forwarded from another partition)
//   - tx.Reserve(ctx) -> on error retry the task
//   - tx.LimiterRefs() to get refs to pass to history (for releasing later)
//   - history.RecordTaskStarted(refs) -> on error call tx.Rollback()
//   - tx.Commit(ctx) -> on error DROP the task
//   - tx.Close() -> release limiter pins (also called by Rollback)
//   - tx.Hints() after Close, to pass back to the partition that forwarded the task, if any
//
// If the task is forwarded to another partition after Check, call tx.ApplyHints() with the hints
// that the other partition returns, and then tx.Rollback() if the forward failed.
//
// A nil callback checks readiness without registering for notifications.
// A nil *Tx is valid and all operations are no-ops.
// Tx is not safe for concurrent use.
// TODO(fc): consider getting rid of fcTask and passing []Limiter and (int32,time.Time)?
// but may need task identity for choosing slot id
func (r *Readiness) NewTx(nsID namespace.ID, task fcTask, cb ReadinessCallback) *Tx {
	lims := task.Limiters()
	if len(lims) == 0 {
		return nil
	}
	sortLimiters(lims)

	tx := &Tx{cb: cb}
	tx.limiters = tx.limitersBuf[:0]
	for _, lim := range lims {
		tx.limiters = append(tx.limiters, txEntry{limiterTx: r.makeLimiterTx(nsID, task, lim)})
	}
	return tx
}

func (r *Readiness) makeLimiterTx(nsID namespace.ID, task fcTask, lim Limiter) limiterTx {
	switch lim.Type {
	case enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT:
		lls := r.getLocalLimiter(nsID, lim.Key)

		// if config is missing or wrong type, zero params means "no limit"
		var config simplelimiter.Params
		if p, ok := lim.Config.(*simplelimiter.Params); ok && p != nil {
			config = *p
		}
		pri := makeWakePriority(task.PriorityAndAge())

		return newLocalLimiterTx(lls, config, pri)

	default:
		// TODO(fc): log or notify here
		return noopLimiterTx{}
	}
}

// Tx holds state for an invocation of the flow control commit protocol. See Readiness.NewTx.
type Tx struct {
	limiters     []txEntry // points into limitersBuf unless there are more than MaxLimiters
	limitersBuf  [MaxLimiters]txEntry
	cb           ReadinessCallback
	closed       bool
	skippedCheck bool // true if SkipCheck was used, so there are no local tokens to return.
}

type limiterTx interface {
	check(ReadinessCallback) error
	cancelCheck()
	reserve(context.Context) error
	commit(context.Context) error
	cancelReserve(context.Context)
	close()
}

// txEntry is the part of a Tx for one limiter.
type txEntry struct {
	limiterTx
	state txState
}

type txState int8

const (
	txStateInit txState = iota
	txStateChecked
	txStateReserved
	txStateCommitted
	txStateCommitFailed
	txStateCanceled
)

// Check checks and consumes local tokens for each limiter.
// If it's not ready and a callback was passed to NewTx, the callback will be registered to be
// called when the limiter may be ready. If it is ready, a registered callback will be removed.
func (tx *Tx) Check() (retErr error) {
	if tx == nil {
		return nil // no limiters
	} else if tx.closed {
		return errInvalidTxState
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(context.Background())
		}
	}()

	for i := range tx.limiters {
		lim := &tx.limiters[i]
		if lim.state != txStateInit {
			return errInvalidTxState
		}
		if err := lim.check(tx.cb); err != nil {
			return err
		}
		lim.state = txStateChecked
	}
	return nil
}

// SkipCheck moves all limiters to the checked state without checking or consuming local
// tokens. This is used for tasks forwarded from another partition, which already did Check on
// that partition.
func (tx *Tx) SkipCheck() (retErr error) {
	if tx == nil {
		return nil // no limiters
	} else if tx.closed {
		return errInvalidTxState
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(context.Background())
		}
	}()

	for i := range tx.limiters {
		if tx.limiters[i].state != txStateInit {
			return errInvalidTxState
		}
	}
	for i := range tx.limiters {
		tx.limiters[i].state = txStateChecked
	}
	tx.skippedCheck = true
	return nil
}

// Reserve checks that all limiters can be satisfied now.
// Reserve may make RPC calls.
func (tx *Tx) Reserve(ctx context.Context) (retErr error) {
	if tx == nil {
		return nil // no limiters
	} else if tx.closed {
		return errInvalidTxState
	}
	defer func() {
		if retErr != nil {
			// TODO(fc): use disconnected context in case this one is canceled/deadline exceeded
			tx.Rollback(ctx)
		}
	}()

	// reserve must be sequential
	for i := range tx.limiters {
		lim := &tx.limiters[i]
		if lim.state != txStateChecked {
			return errInvalidTxState
		}
		if err := lim.reserve(ctx); err != nil {
			return err
		}
		lim.state = txStateReserved
	}
	return nil
}

// Commit turns reservations into committed slots.
// Commit may make RPC calls.
func (tx *Tx) Commit(ctx context.Context) (retErr error) {
	if tx == nil {
		return nil // no limiters
	} else if tx.closed {
		return errInvalidTxState
	}
	defer func() {
		if retErr != nil {
			tx.Rollback(ctx)
		}
	}()

	for i := range tx.limiters {
		if tx.limiters[i].state != txStateReserved {
			return errInvalidTxState
		}
	}

	// commit all concurrently
	var wg sync.WaitGroup
	errs := make([]error, len(tx.limiters))
	for i := range tx.limiters {
		lim := &tx.limiters[i]
		f := func() {
			errs[i] = lim.commit(ctx)
			if errs[i] != nil {
				lim.state = txStateCommitFailed
			} else {
				lim.state = txStateCommitted
			}
		}
		if i == len(tx.limiters)-1 {
			f()
		} else {
			wg.Go(f)
		}
	}
	wg.Wait()

	return errors.Join(errs...)
}

// Rollback cancels checks and reservations on any limiters that have been made and not
// committed so far. ctx may be nil only if Reserve has not been called yet.
func (tx *Tx) Rollback(ctx context.Context) {
	if tx == nil || tx.closed {
		return
	}
	defer tx.Close()
	for i := range tx.limiters {
		lim := &tx.limiters[i]
		switch lim.state {
		case txStateReserved:
			// this is best-effort, reservations have timeouts so it's okay if we fail to cancel
			lim.cancelReserve(ctx)
			fallthrough
		case txStateChecked:
			if !tx.skippedCheck {
				lim.cancelCheck()
			}
			lim.state = txStateCanceled
		default:
			// nothing to cancel
		}
	}
}

// Close releases limiter pins. Call Rollback first if the transaction needs to be canceled.
func (tx *Tx) Close() {
	if tx == nil || tx.closed {
		return
	}
	tx.closed = true
	for _, lim := range tx.limiters {
		lim.close()
	}
}

type noopLimiterTx struct{}

func (noopLimiterTx) check(cb ReadinessCallback) error { return nil }
func (noopLimiterTx) cancelCheck()                     {}
func (noopLimiterTx) reserve(context.Context) error    { return nil }
func (noopLimiterTx) commit(context.Context) error     { return nil }
func (noopLimiterTx) cancelReserve(context.Context)    {}
func (noopLimiterTx) close()                           {}
