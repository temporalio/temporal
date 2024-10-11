// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sqlplugin

import (
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
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
				return nil, testErr
			}
			dbHandle := NewDatabaseHandle(connectFunc, needsRefreshFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler)
			assert.NotNil(t, dbHandle)

			for i := 0; i < tc.numRetries; i++ {
				<-time.NewTimer(tc.retryDelay).C
				db, err := dbHandle.DB()
				assert.Nil(t, db, tc.msg)
				assert.ErrorIs(t, err, DatabaseUnavailableError)
			}
			assert.Equal(t, tc.expectedConnectAttempts, actualConnectAttemptCount, tc.msg)
		})
	}
}

var testErr = errors.New("test")
