package fc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
)

type txSuite struct {
	parallelsuite.Suite[*txSuite]
}

func TestTx(t *testing.T) {
	parallelsuite.Run(t, &txSuite{})
}

type txCallLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *txCallLog) record(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (s *txSuite) requireCalls(log *txCallLog, want string) {
	s.T().Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	s.Equal(strings.Fields(want), log.calls)
}

type testLimiterTx struct {
	name string
	log  *txCallLog

	checkCallback ReadinessCallback
	checkErr      error
	reserveErr    error
	onCommit      func(context.Context) error
}

func (l *testLimiterTx) check(cb ReadinessCallback) error {
	l.log.record(l.name + ".check")
	l.checkCallback = cb
	return l.checkErr
}

func (l *testLimiterTx) reserve(context.Context) error {
	l.log.record(l.name + ".reserve")
	return l.reserveErr
}

func (l *testLimiterTx) commit(ctx context.Context) error {
	l.log.record(l.name + ".commit")
	if l.onCommit != nil {
		return l.onCommit(ctx)
	}
	return nil
}

func (l *testLimiterTx) cancelCheck() {
	l.log.record(l.name + ".cancelCheck")
}

func (l *testLimiterTx) cancelReserve(context.Context) {
	l.log.record(l.name + ".cancelReserve")
}

func (l *testLimiterTx) close() {
	l.log.record(l.name + ".close")
}

func newTestTx(names ...string) (*Tx, []*testLimiterTx, *txCallLog) {
	log := &txCallLog{}
	limiters := make([]*testLimiterTx, len(names))
	tx := &Tx{limiters: make([]txEntry, len(names))}
	for i, name := range names {
		limiters[i] = &testLimiterTx{name: name, log: log}
		tx.limiters[i] = txEntry{limiterTx: limiters[i]}
	}
	return tx, limiters, log
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
	tx, limiters, log := newTestTx("a", "b", "c", "d")
	tx.cb = &testCallback{}

	s.NoError(tx.Check())
	for _, limiter := range limiters {
		s.Same(tx.cb, limiter.checkCallback)
	}
	s.NoError(tx.Reserve(s.Context()))
	s.requireCalls(log, "a.check b.check c.check d.check a.reserve b.reserve c.reserve d.reserve")

	s.NoError(tx.Commit(s.Context()))
	want := strings.Fields(
		"a.check b.check c.check d.check a.reserve b.reserve c.reserve d.reserve a.commit b.commit c.commit d.commit",
	)
	s.ElementsMatch(want, log.calls)

	tx.Close()
	tx.Close()
	tx.Rollback(s.Context())

	s.ElementsMatch(strings.Fields("a.close b.close c.close d.close"), log.calls[len(want):])
}

func (s *txSuite) TestCheckFailureRefundsOnlyEarlierChecks() {
	for _, tc := range []struct {
		name string
		at   int
		want string
	}{
		{"first", 0, "a.check a.close b.close c.close"},
		{"middle", 1, "a.check b.check a.cancelCheck a.close b.close c.close"},
		{"last", 2, "a.check b.check c.check a.cancelCheck b.cancelCheck a.close b.close c.close"},
	} {
		s.Run(tc.name, func(s *txSuite) {
			tx, limiters, log := newTestTx("a", "b", "c")
			checkErr := errors.New("check blocked")
			limiters[tc.at].checkErr = checkErr

			s.ErrorIs(tx.Check(), checkErr)
			s.requireCalls(log, tc.want)
		})
	}
}

func (s *txSuite) TestReserveFailureCancelsOnlyEarlierReservations() {
	for _, tc := range []struct {
		name string
		at   int
		want string
	}{
		{"first", 0, "a.reserve a.cancelCheck b.cancelCheck c.cancelCheck"},
		{"middle", 1, "a.reserve b.reserve a.cancelReserve a.cancelCheck b.cancelCheck c.cancelCheck"},
		{"last", 2, "a.reserve b.reserve c.reserve a.cancelReserve a.cancelCheck b.cancelReserve b.cancelCheck c.cancelCheck"},
	} {
		s.Run(tc.name, func(s *txSuite) {
			tx, limiters, log := newTestTx("a", "b", "c")
			reserveErr := errors.New("reserve blocked")
			limiters[tc.at].reserveErr = reserveErr
			s.NoError(tx.Check())

			s.ErrorIs(tx.Reserve(s.Context()), reserveErr)
			s.requireCalls(log, "a.check b.check c.check "+tc.want+" a.close b.close c.close")
		})
	}
}

func (s *txSuite) TestCommitRunsConcurrentlyAndJoinsErrors() {
	ctx, cancel := context.WithCancel(testcontext.For(s.T(), testcontext.WithTimeout(5*time.Second)))
	defer cancel()
	tx, limiters, log := newTestTx("a", "b", "c")
	firstErr := errors.New("commit rejected")
	lastErr := errors.New("commit response lost")
	entered := make(chan string, len(limiters))
	release := make(chan struct{})
	for i, err := range []error{firstErr, nil, lastErr} {
		limiter := limiters[i]
		limiter.onCommit = func(ctx context.Context) error {
			entered <- limiter.name
			select {
			case <-release:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	s.NoError(tx.Check())
	s.NoError(tx.Reserve(ctx))

	result := make(chan error, 1)
	go func() { result <- tx.Commit(ctx) }()
	// Each commit must enter while the others are still waiting at the gate.
	committing := []string{s.Rcv(entered), s.Rcv(entered), s.Rcv(entered)}
	s.ElementsMatch([]string{"a", "b", "c"}, committing)
	close(release)
	err := s.Rcv(result)

	s.ErrorIs(err, firstErr)
	s.ErrorIs(err, lastErr)
	// A failed RPC can leave a committed slot. None of these entries is safe to refund.
	s.ElementsMatch(strings.Fields(
		"a.check b.check c.check a.reserve b.reserve c.reserve a.commit b.commit c.commit a.close b.close c.close",
	), log.calls)
}

func (s *txSuite) TestRollbackAfterCheck() {
	tx, _, log := newTestTx("a", "b")
	s.NoError(tx.Check())

	tx.Rollback(s.Context())
	tx.Rollback(s.Context())
	tx.Close()

	s.requireCalls(log, "a.check b.check a.cancelCheck b.cancelCheck a.close b.close")
}

func (s *txSuite) TestRollbackAfterReserve() {
	tx, _, log := newTestTx("a", "b")
	s.NoError(tx.Check())
	s.NoError(tx.Reserve(s.Context()))

	tx.Rollback(s.Context())
	tx.Rollback(s.Context())

	s.requireCalls(log, "a.check b.check a.reserve b.reserve a.cancelReserve a.cancelCheck b.cancelReserve b.cancelCheck a.close b.close")
}

func (s *txSuite) TestCloseAfterCheckKeepsConsumptionAndRejectsMoreWork() {
	tx, _, log := newTestTx("a")
	s.NoError(tx.Check())

	tx.Close()
	s.ErrorIs(tx.Check(), errInvalidTxState)
	s.ErrorIs(tx.SkipCheck(), errInvalidTxState)
	s.ErrorIs(tx.Reserve(s.Context()), errInvalidTxState)
	s.ErrorIs(tx.Commit(s.Context()), errInvalidTxState)
	tx.Rollback(s.Context())

	s.requireCalls(log, "a.check a.close")
}

func (s *txSuite) TestReserveBeforeCheck() {
	tx, _, log := newTestTx("a", "b")

	s.ErrorIs(tx.Reserve(s.Context()), errInvalidTxState)

	s.requireCalls(log, "a.close b.close")
}

func (s *txSuite) TestCommitValidatesAllEntriesBeforeStarting() {
	tx, _, log := newTestTx("a", "b")
	s.NoError(tx.Check())
	// Only the first entry has a reservation. Commit must validate both before making any RPC.
	tx.limiters[0].state = txStateReserved

	s.ErrorIs(tx.Commit(s.Context()), errInvalidTxState)

	s.requireCalls(log, "a.check b.check a.cancelReserve a.cancelCheck b.cancelCheck a.close b.close")
}

func (s *txSuite) TestCheckTwiceRollsBack() {
	tx, _, log := newTestTx("a")
	s.NoError(tx.Check())

	s.ErrorIs(tx.Check(), errInvalidTxState)

	s.requireCalls(log, "a.check a.cancelCheck a.close")
}

func (s *txSuite) TestReserveTwiceRollsBack() {
	tx, _, log := newTestTx("a")
	s.NoError(tx.Check())
	s.NoError(tx.Reserve(s.Context()))

	s.ErrorIs(tx.Reserve(s.Context()), errInvalidTxState)

	s.requireCalls(log, "a.check a.reserve a.cancelReserve a.cancelCheck a.close")
}

func (s *txSuite) TestSkipCheckCanReserveAndCommit() {
	tx, _, log := newTestTx("a")

	s.NoError(tx.SkipCheck())
	s.NoError(tx.Reserve(s.Context()))
	s.NoError(tx.Commit(s.Context()))
	tx.Close()

	s.requireCalls(log, "a.reserve a.commit a.close")
}

func (s *txSuite) TestRollbackAfterSkipCheckCancelsOnlyReservations() {
	tx, _, log := newTestTx("a", "b")
	s.NoError(tx.SkipCheck())
	s.NoError(tx.Reserve(s.Context()))

	tx.Rollback(s.Context())

	s.requireCalls(log, "a.reserve b.reserve a.cancelReserve b.cancelReserve a.close b.close")
}

func (s *txSuite) TestInvalidSkipCheckDoesNotInventConsumedTokens() {
	tx, _, log := newTestTx("a", "b")
	// If SkipCheck updates entries while validating them, rollback will refund a's nonexistent token.
	tx.limiters[1].state = txStateChecked

	s.ErrorIs(tx.SkipCheck(), errInvalidTxState)

	s.requireCalls(log, "b.cancelCheck a.close b.close")
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
