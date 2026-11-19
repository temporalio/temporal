package fc

import (
	"context"
	"errors"
	"sync"

	"go.temporal.io/api/serviceerror"
)

var errInvalidTxState = serviceerror.NewInternal("invalid fc tx state")

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
// Reserve may make RPC calls.
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
