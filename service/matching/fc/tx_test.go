package fc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.uber.org/mock/gomock"
)

type txSuite struct {
	parallelsuite.Suite[*txSuite]
}

func TestTx(t *testing.T) {
	parallelsuite.Run(t, &txSuite{})
}

func newMockTx(ctrl *gomock.Controller, count int) (*Tx, []*MocklimiterTx) {
	tx := &Tx{limiters: make([]txEntry, count)}
	limiters := make([]*MocklimiterTx, count)
	for i := range limiters {
		limiters[i] = NewMocklimiterTx(ctrl)
		tx.limiters[i] = txEntry{limiterTx: limiters[i]}
	}
	return tx, limiters
}

func (s *txSuite) newTx(count int) (*Tx, []*MocklimiterTx) {
	s.T().Helper()
	return newMockTx(gomock.NewController(s.T()), count)
}

func expectChecks(callback any, limiters []*MocklimiterTx) {
	calls := make([]any, 0, len(limiters))
	for _, limiter := range limiters {
		calls = append(calls, limiter.EXPECT().check(callback).Return(nil))
	}
	gomock.InOrder(calls...)
}

func expectReserves(ctx context.Context, limiters []*MocklimiterTx) {
	calls := make([]any, 0, len(limiters))
	for _, limiter := range limiters {
		calls = append(calls, limiter.EXPECT().reserve(ctx).Return(nil))
	}
	gomock.InOrder(calls...)
}

func TestNilTx(t *testing.T) {
	t.Parallel()
	var tx *Tx
	require.NoError(t, tx.Check())
	require.NoError(t, tx.SkipCheck())
	require.NoError(t, tx.Reserve(t.Context()))
	require.NoError(t, tx.Commit(t.Context()))
	tx.Rollback(t.Context())
	tx.Close()
}

func (s *txSuite) TestSuccessfulTransaction() {
	// Use more entries than MaxLimiters to cover every entry in a larger transaction.
	ctx := s.Context()
	tx, limiters := s.newTx(MaxLimiters + 1)
	cb := &testCallback{}
	tx.cb = cb

	expectChecks(gomock.Cond(func(got ReadinessCallback) bool { return got == cb }), limiters)
	s.NoError(tx.Check())
	expectReserves(ctx, limiters)
	s.NoError(tx.Reserve(ctx))
	for _, limiter := range limiters {
		limiter.EXPECT().commit(ctx).Return(nil)
	}
	s.NoError(tx.Commit(ctx))

	for _, limiter := range limiters {
		limiter.EXPECT().close()
	}
	tx.Close()
	tx.Close()
	tx.Rollback(ctx)
}

func (s *txSuite) TestCheckFailureRefundsOnlyEarlierChecks() {
	for failAt, name := range []string{"first", "middle", "last"} {
		s.Run(name, func(s *txSuite) {
			tx, limiters := s.newTx(3)
			checkErr := errors.New("check blocked")
			checks := make([]any, 0, failAt+1)
			for _, limiter := range limiters[:failAt] {
				checks = append(checks, limiter.EXPECT().check(nil).Return(nil))
			}
			failed := limiters[failAt].EXPECT().check(nil).Return(checkErr)
			gomock.InOrder(append(checks, failed)...)
			for i, limiter := range limiters {
				cleanup := failed
				if i < failAt {
					cleanup = limiter.EXPECT().cancelCheck().After(failed)
				}
				limiter.EXPECT().close().After(cleanup)
			}

			s.ErrorIs(tx.Check(), checkErr)
		})
	}
}

func (s *txSuite) TestReserveFailureCancelsOnlyEarlierReservations() {
	for failAt, name := range []string{"first", "middle", "last"} {
		s.Run(name, func(s *txSuite) {
			ctx := s.Context()
			tx, limiters := s.newTx(3)
			expectChecks(nil, limiters)
			s.NoError(tx.Check())

			reserveErr := errors.New("reserve blocked")
			reserves := make([]any, 0, failAt+1)
			for _, limiter := range limiters[:failAt] {
				reserves = append(reserves, limiter.EXPECT().reserve(ctx).Return(nil))
			}
			failed := limiters[failAt].EXPECT().reserve(ctx).Return(reserveErr)
			gomock.InOrder(append(reserves, failed)...)
			for i, limiter := range limiters {
				cleanup := failed
				if i < failAt {
					cleanup = limiter.EXPECT().cancelReserve(ctx).After(failed)
				}
				checkCanceled := limiter.EXPECT().cancelCheck().After(cleanup)
				limiter.EXPECT().close().After(checkCanceled)
			}

			s.ErrorIs(tx.Reserve(ctx), reserveErr)
		})
	}
}

func (s *txSuite) TestCommitRunsConcurrentlyAndJoinsErrors() {
	ctx, cancel := context.WithCancel(testcontext.For(s.T(), testcontext.WithTimeout(5*time.Second)))
	defer cancel()
	ctrl, ctx := gomock.WithContext(ctx, s.T())
	tx, limiters := newMockTx(ctrl, 3)
	firstErr := errors.New("commit rejected")
	lastErr := errors.New("commit response lost")
	entered := make(chan struct{}, len(limiters))
	release := make(chan struct{})

	expectChecks(nil, limiters)
	s.NoError(tx.Check())
	expectReserves(ctx, limiters)
	s.NoError(tx.Reserve(ctx))
	for i, err := range []error{firstErr, nil, lastErr} {
		limiter := limiters[i]
		commit := limiter.EXPECT().commit(ctx).DoAndReturn(func(ctx context.Context) error {
			entered <- struct{}{}
			select {
			case <-release:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		// A failed RPC can leave a committed slot. None of these entries is safe to refund.
		limiter.EXPECT().close().After(commit)
	}

	result := make(chan error, 1)
	go func() { result <- tx.Commit(ctx) }()
	// Each commit must enter while the others are still waiting at the gate.
	for range limiters {
		s.Rcv(entered)
	}
	close(release)
	err := s.Rcv(result)

	s.ErrorIs(err, firstErr)
	s.ErrorIs(err, lastErr)
}

func (s *txSuite) TestRollbackAfterCheck() {
	tx, limiters := s.newTx(2)
	expectChecks(nil, limiters)
	s.NoError(tx.Check())

	for _, limiter := range limiters {
		canceled := limiter.EXPECT().cancelCheck()
		limiter.EXPECT().close().After(canceled)
	}
	tx.Rollback(s.Context())
	tx.Rollback(s.Context())
	tx.Close()
}

func (s *txSuite) TestRollbackAfterReserve() {
	ctx := s.Context()
	tx, limiters := s.newTx(2)
	expectChecks(nil, limiters)
	s.NoError(tx.Check())
	expectReserves(ctx, limiters)
	s.NoError(tx.Reserve(ctx))

	for _, limiter := range limiters {
		gomock.InOrder(limiter.EXPECT().cancelReserve(ctx), limiter.EXPECT().cancelCheck(), limiter.EXPECT().close())
	}
	tx.Rollback(ctx)
	tx.Rollback(ctx)
}

func (s *txSuite) TestCloseAfterCheckKeepsConsumptionAndRejectsMoreWork() {
	tx, limiters := s.newTx(1)
	l := limiters[0]
	l.EXPECT().check(nil).Return(nil)
	s.NoError(tx.Check())

	l.EXPECT().close()
	tx.Close()
	s.ErrorIs(tx.Check(), errInvalidTxState)
	s.ErrorIs(tx.SkipCheck(), errInvalidTxState)
	s.ErrorIs(tx.Reserve(s.Context()), errInvalidTxState)
	s.ErrorIs(tx.Commit(s.Context()), errInvalidTxState)
	tx.Rollback(s.Context())
}

func (s *txSuite) TestReserveBeforeCheck() {
	tx, limiters := s.newTx(2)
	for _, limiter := range limiters {
		limiter.EXPECT().close()
	}

	s.ErrorIs(tx.Reserve(s.Context()), errInvalidTxState)
}

func (s *txSuite) TestCommitValidatesAllEntriesBeforeStarting() {
	ctx := s.Context()
	tx, limiters := s.newTx(2)
	expectChecks(nil, limiters)
	s.NoError(tx.Check())
	// Only the first entry has a reservation. Commit must validate both before making any RPC.
	tx.limiters[0].state = txStateReserved

	first, second := limiters[0], limiters[1]
	gomock.InOrder(first.EXPECT().cancelReserve(ctx), first.EXPECT().cancelCheck(), first.EXPECT().close())
	gomock.InOrder(second.EXPECT().cancelCheck(), second.EXPECT().close())

	s.ErrorIs(tx.Commit(ctx), errInvalidTxState)
}

func (s *txSuite) TestCheckTwiceRollsBack() {
	tx, limiters := s.newTx(1)
	l := limiters[0]
	l.EXPECT().check(nil).Return(nil)
	s.NoError(tx.Check())
	gomock.InOrder(l.EXPECT().cancelCheck(), l.EXPECT().close())

	s.ErrorIs(tx.Check(), errInvalidTxState)
}

func (s *txSuite) TestReserveTwiceRollsBack() {
	ctx := s.Context()
	tx, limiters := s.newTx(1)
	l := limiters[0]
	l.EXPECT().check(nil).Return(nil)
	s.NoError(tx.Check())
	l.EXPECT().reserve(ctx).Return(nil)
	s.NoError(tx.Reserve(ctx))
	gomock.InOrder(l.EXPECT().cancelReserve(ctx), l.EXPECT().cancelCheck(), l.EXPECT().close())

	s.ErrorIs(tx.Reserve(ctx), errInvalidTxState)
}

func (s *txSuite) TestSkipCheckCanReserveAndCommit() {
	ctx := s.Context()
	tx, limiters := s.newTx(1)
	l := limiters[0]
	s.NoError(tx.SkipCheck())

	gomock.InOrder(l.EXPECT().reserve(ctx).Return(nil), l.EXPECT().commit(ctx).Return(nil), l.EXPECT().close())
	s.NoError(tx.Reserve(ctx))
	s.NoError(tx.Commit(ctx))
	tx.Close()
}

func (s *txSuite) TestRollbackAfterSkipCheckCancelsOnlyReservations() {
	ctx := s.Context()
	tx, limiters := s.newTx(2)
	s.NoError(tx.SkipCheck())
	expectReserves(ctx, limiters)
	s.NoError(tx.Reserve(ctx))

	for _, limiter := range limiters {
		gomock.InOrder(limiter.EXPECT().cancelReserve(ctx), limiter.EXPECT().close())
	}
	tx.Rollback(ctx)
}

func (s *txSuite) TestInvalidSkipCheckDoesNotInventConsumedTokens() {
	tx, limiters := s.newTx(2)
	// If SkipCheck updates entries while validating them, rollback will refund a's nonexistent token.
	tx.limiters[1].state = txStateChecked
	limiters[0].EXPECT().close()
	gomock.InOrder(limiters[1].EXPECT().cancelCheck(), limiters[1].EXPECT().close())

	s.ErrorIs(tx.SkipCheck(), errInvalidTxState)
}

func TestSortLimiters(t *testing.T) {
	t.Parallel()
	a := Limiter{Type: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "a", Config: "config-a", ConfigVersion: 1}
	b := Limiter{Type: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "b", Config: "config-b", ConfigVersion: 2}
	c := Limiter{
		Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "a", Source: LimiterSourceConfig,
		Config: "config-c", ConfigVersion: 3,
	}
	want := []Limiter{a, b, c}
	for _, input := range [][]Limiter{
		{a, b, c}, {a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a},
	} {
		sortLimiters(input)
		require.Equal(t, want, input)
	}
}
