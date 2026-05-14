package sqlplugin

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// ---- minimal noop SQL driver used by reconnect tests ----

type (
	noopDriver struct{}
	noopConn   struct{}
	noopStmt   struct{}
	noopRows   struct{}
)

func (noopDriver) Open(_ string) (driver.Conn, error)        { return noopConn{}, nil }
func (noopConn) Prepare(_ string) (driver.Stmt, error)       { return noopStmt{}, nil }
func (noopConn) Close() error                                { return nil }
func (noopConn) Begin() (driver.Tx, error)                   { return nil, driver.ErrBadConn }

func (noopStmt) Close() error                                    { return nil }
func (noopStmt) NumInput() int                                   { return 0 }
func (noopStmt) Exec(_ []driver.Value) (driver.Result, error)    { return nil, nil }
func (noopStmt) Query(_ []driver.Value) (driver.Rows, error)     { return noopRows{}, nil }
func (noopRows) Columns() []string                               { return nil }
func (noopRows) Close() error                                    { return nil }
func (noopRows) Next(_ []driver.Value) error                     { return io.EOF }

var registerNoopDriverOnce sync.Once

const noopDriverName = "noop_test_driver"

func mustNoopDB(t *testing.T) *sqlx.DB {
	t.Helper()
	registerNoopDriverOnce.Do(func() {
		sql.Register(noopDriverName, noopDriver{})
	})
	db, err := sqlx.Open(noopDriverName, "")
	require.NoError(t, err)
	return db
}

// ---- existing throttle test ----

// TestDatabaseHandleReconnect tests the reconnection behavior when there are connection errors.
func TestDatabaseHandleReconnect(t *testing.T) {
	testCases := []struct {
		msg                     string        // test description
		numRetries              int           // number of time to try reconnecting
		retryDelay              time.Duration // wait time (in this test) before trying to reconnect
		expectedConnectAttempts int           // expected number of times a connection attempt is made.
	}{
		{"No retries", 0, 0, 1},
		{"Retry once immediately", 1, 0, 1},
		{"Retry many times immediately", 5, 0, 1},
		{"Retry once after session retry interval", 1, sessionRefreshMinInternal + time.Millisecond, 2},
		{"Retry many times with session retry interval", 5, sessionRefreshMinInternal + time.Millisecond, 6},
		{"Retry many times quickly - but collectively exceeding session retry interval. Expect 2 total attempts", 5, sessionRefreshMinInternal/5 + time.Millisecond, 2},
	}

	needsRefreshFunc := func(_ error) bool { return false }

	for _, tc := range testCases {
		t.Run(tc.msg, func(t *testing.T) {
			actualConnectAttemptCount := 0
			connectFunc := func() (*sqlx.DB, error) {
				actualConnectAttemptCount++
				return nil, errTest
			}
			fakeTimeSource := clock.NewEventTimeSource().Update(time.Now())
			dbHandle := NewDatabaseHandle(DbKindUnknown, connectFunc, needsRefreshFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, fakeTimeSource)
			assert.NotNil(t, dbHandle)

			for i := 0; i < tc.numRetries; i++ {
				fakeTimeSource.Advance(tc.retryDelay)
				db, err := dbHandle.DB()
				assert.Nil(t, db, tc.msg)
				assert.ErrorIs(t, err, DatabaseUnavailableError)
			}
			assert.Equal(t, tc.expectedConnectAttempts, actualConnectAttemptCount, tc.msg)
		})
	}
}

var errTest = errors.New("test")

// TestReconnectPoolAccumulationDuringOutage reproduces the connection storm described in issue #9211.
//
// When many goroutines hit a connection error at the same moment, they all call
// reconnect(true) within the same 1s throttle window. Under the old code the
// first call destroyed the pool (Store(nil)) before the throttle check, leaving
// h.db nil for the whole window. Every retry then saw a nil pool, triggered
// another ConvertError, and the cycle repeated — each successful reconnect created
// a fresh *sql.DB while the previous one's Close() goroutine was stuck waiting
// for in-flight connections to drain. Pools accumulated.
//
// The fix: check throttle BEFORE destroying the existing pool, and return the
// existing pool if throttled. This means a burst of concurrent force-reconnects
// within one throttle window leaves the pool alive and never calls connect() more
// than once per window.
func TestReconnectPoolAccumulationDuringOutage(t *testing.T) {
	connectCount := 0
	connectFunc := func() (*sqlx.DB, error) {
		connectCount++
		return mustNoopDB(t), nil
	}

	fakeTimeSource := clock.NewEventTimeSource().Update(time.Now())
	dbHandle := NewDatabaseHandle(
		DbKindUnknown,
		connectFunc,
		func(_ error) bool { return false },
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		fakeTimeSource,
	)
	require.NotNil(t, dbHandle)
	require.Equal(t, 1, connectCount, "initial connect should have been called once")

	// simulate a burst of ConvertError calls that all arrive within the same
	// 1-second throttle window — no time advance between calls
	for i := 0; i < 5; i++ {
		dbHandle.reconnect(true)
	}

	// no time has passed, so the throttle should have blocked all 5 reconnect
	// attempts and returned the existing pool — connect() must not be called again
	assert.Equal(t, 1, connectCount,
		"connect() must not be called again during a throttle window — "+
			"each extra call creates a new pool that multiplies connections at recovery")

	// the pool must still be alive; callers should not get DatabaseUnavailableError
	db, err := dbHandle.DB()
	assert.NoError(t, err, "pool should survive a rapid burst of reconnect(true) calls")
	assert.NotNil(t, db, "pool should not be nil after rapid burst")
}

// TestReconnectNilPoolOnThrottle reproduces the second aspect of the same bug:
// reconnect(true) destroys the current pool (h.db = nil) BEFORE checking the throttle.
// When throttled, no new pool is created, so h.db stays nil for the entire
// throttle window. All DB() calls during that window return DatabaseUnavailableError
// even though the previous pool was perfectly healthy.
//
// This forces every goroutine that gets DatabaseUnavailableError to call ConvertError
// again on the next retry, triggering another reconnect(true), destroying the pool
// again — locking the handle into a nil-pool loop for the entire outage window.
func TestReconnectNilPoolOnThrottle(t *testing.T) {
	connectFunc := func() (*sqlx.DB, error) {
		return mustNoopDB(t), nil
	}

	fakeTimeSource := clock.NewEventTimeSource().Update(time.Now())
	dbHandle := NewDatabaseHandle(
		DbKindUnknown,
		connectFunc,
		func(_ error) bool { return false },
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		fakeTimeSource,
	)

	// confirm the initial pool is healthy
	db, err := dbHandle.DB()
	require.NoError(t, err)
	require.NotNil(t, db)

	// first force-reconnect succeeds (replaces pool)
	fakeTimeSource.Advance(sessionRefreshMinInternal + time.Millisecond)
	dbHandle.reconnect(true)

	// second force-reconnect within the throttle window (simulates a burst of
	// ConvertError calls arriving within the same second)
	dbHandle.reconnect(true) // no time advance — throttled

	// BUG: under current code the second reconnect(true) nils out the pool
	// BEFORE reaching the throttle check, so DB() now returns nil+error.
	db, err = dbHandle.DB()
	assert.NoError(t, err,
		"DB() should return the existing healthy pool during the throttle window, not DatabaseUnavailableError")
	assert.NotNil(t, db,
		"DB() should not return nil when a healthy pool was just established")
}


