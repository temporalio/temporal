package sqlplugin

import (
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

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

// TestDatabaseHandleReconnectAfterClockStepsBack tests that a wall clock that steps backwards
// does not keep the refresh throttled until the clock catches up with the last refresh.
func TestDatabaseHandleReconnectAfterClockStepsBack(t *testing.T) {
	connectAttempts := 0
	connectFunc := func() (*sqlx.DB, error) {
		connectAttempts++
		return nil, errTest
	}
	needsRefreshFunc := func(_ error) bool { return false }

	start := time.Now()
	fakeTimeSource := clock.NewEventTimeSource().Update(start)
	dbHandle := NewDatabaseHandle(DbKindUnknown, connectFunc, needsRefreshFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, fakeTimeSource)
	require.Equal(t, 1, connectAttempts)

	// The first attempt after the step is not throttled by a refresh that is now in the future.
	fakeTimeSource.Update(start.Add(-5 * time.Minute))
	_, err := dbHandle.DB()
	require.ErrorIs(t, err, DatabaseUnavailableError)
	require.Equal(t, 2, connectAttempts)

	// The throttle keeps working from the stepped clock.
	_, err = dbHandle.DB()
	require.ErrorIs(t, err, DatabaseUnavailableError)
	require.Equal(t, 2, connectAttempts)

	fakeTimeSource.Advance(sessionRefreshMinInternal + time.Millisecond)
	_, err = dbHandle.DB()
	require.ErrorIs(t, err, DatabaseUnavailableError)
	require.Equal(t, 3, connectAttempts)
}

var errTest = errors.New("test")
