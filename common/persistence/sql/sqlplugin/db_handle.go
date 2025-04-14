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
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	// TODO: this should be dynamic config.
	sessionRefreshMinInternal = 1 * time.Second
)

var (
	DatabaseUnavailableError = serviceerror.NewUnavailable("no usable database connection found")
)

type DatabaseHandle struct {
	running      bool
	db           atomic.Pointer[sqlx.DB]
	connect      func() (*sqlx.DB, error)
	needsRefresh func(error) bool

	lastRefresh time.Time
	metrics     metrics.Handler
	logger      log.Logger
	timeSource  clock.TimeSource
	// Ensures only one refresh call happens at a time
	sync.Mutex
}

// An invalid connection returns `DatabaseUnavailableError` for all operations
type invalidConn struct{}

func NewDatabaseHandle(
	connect func() (*sqlx.DB, error),
	needsRefresh func(error) bool,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *DatabaseHandle {
	handle := &DatabaseHandle{
		running:      true,
		connect:      connect,
		needsRefresh: needsRefresh,
		metrics:      metricsHandler,
		logger:       logger,
		timeSource:   timeSource,
	}
	handle.reconnect(true)
	return handle
}

// Close and reopen the underlying database connection
func (h *DatabaseHandle) reconnect(force bool) *sqlx.DB {
	h.Lock()
	defer h.Unlock()

	// Don't reconnect if we've been closed
	if !h.running {
		return nil
	}

	prevConn := h.db.Load()
	if prevConn != nil {
		if !force {
			// Another goroutine already reconnected
			return prevConn
		}

		h.db.Store(nil)
		// Store `nil` to prevent other goroutines from slamming the now-unusable database with
		// transactions we know will fail
		go prevConn.Close()
	}

	metrics.PersistenceSessionRefreshAttempts.With(h.metrics).Record(1)

	now := h.timeSource.Now()
	lastRefresh := h.lastRefresh
	if now.Sub(lastRefresh) < sessionRefreshMinInternal {
		h.logger.Warn("sql handle: did not refresh database connection pool because the last refresh was too close",
			tag.NewDurationTag("min_refresh_interval_seconds", sessionRefreshMinInternal))
		handler := h.metrics.WithTags(metrics.FailureTag("throttle"))
		metrics.PersistenceSessionRefreshFailures.With(handler).Record(1)
		return nil
	}

	h.lastRefresh = now
	newConn, err := h.connect()
	if err != nil {
		h.logger.Error("sql handle: unable to refresh database connection pool", tag.Error(err))
		handler := h.metrics.WithTags(metrics.FailureTag("error"))
		metrics.PersistenceSessionRefreshFailures.With(handler).Record(1)
		return nil
	}

	h.db.Store(newConn)
	return newConn
}

func (h *DatabaseHandle) Close() {
	h.Lock()
	defer h.Unlock()

	if h.running {
		h.running = false
		db := h.db.Swap(nil)
		if db != nil {
			db.Close()
		}
	}
}

func (h *DatabaseHandle) DB() (*sqlx.DB, error) {
	if db := h.db.Load(); db != nil {
		return db, nil
	}

	if db := h.reconnect(false); db != nil {
		return db, nil
	}
	return nil, DatabaseUnavailableError
}

func (h *DatabaseHandle) Conn() Conn {
	if db := h.db.Load(); db != nil {
		return db
	}

	if db := h.reconnect(false); db != nil {
		return db
	}
	return invalidConn{}
}

func (h *DatabaseHandle) ConvertError(err error) error {
	if h.needsRefresh(err) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) {
		h.reconnect(true)
		return serviceerror.NewUnavailable(fmt.Sprintf("database connection lost: %s", err.Error()))
	}
	return err
}

func (invalidConn) Rebind(query string) string {
	return query
}

func (invalidConn) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, DatabaseUnavailableError
}

func (invalidConn) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return nil, DatabaseUnavailableError
}

func (invalidConn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return DatabaseUnavailableError
}

func (invalidConn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return DatabaseUnavailableError
}

func (invalidConn) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	return nil, DatabaseUnavailableError
}
