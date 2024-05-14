package sqlplugin

import (
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	uberatomic "go.uber.org/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	// TODO: this should be dynamic config. For now we reuse the same setting as our cassandra implementation
	sessionRefreshMinInternal = 5 * time.Second
)

type DatabaseHandle struct {
	running      atomic.Bool
	db           uberatomic.Pointer[sqlx.DB]
	connect      func() (*sqlx.DB, error)
	needsRefresh func(error) bool

	lastRefresh time.Time
	metrics     metrics.Handler
	logger      log.Logger
	// Ensures only one refresh call happens at a time
	sync.Mutex
}

func NewDatabaseHandle(
	connect func() (*sqlx.DB, error),
	needsRefresh func(error) bool,
	logger log.Logger,
	metrics metrics.Handler,
) *DatabaseHandle {
	handle := &DatabaseHandle{
		connect:      connect,
		needsRefresh: needsRefresh,
		metrics:      metrics,
		logger:       logger,
	}
	handle.running.Store(true)
	handle.reconnect()
	return handle
}

// Close and reopen the underlying database connection
func (h *DatabaseHandle) reconnect() {
	if !h.running.Load() {
		return
	}

	h.Lock()
	defer h.Unlock()

	metrics.PersistenceSessionRefreshAttempts.With(h.metrics).Record(1)

	now := time.Now()
	lastRefresh := h.lastRefresh
	h.lastRefresh = now
	if now.Sub(lastRefresh) < sessionRefreshMinInternal {
		h.logger.Warn("sql handle: did not refresh database connection pool because the last refresh was too close",
			tag.NewDurationTag("min_refresh_interval_seconds", sessionRefreshMinInternal))
		handler := h.metrics.WithTags(metrics.FailureTag("throttle"))
		metrics.PersistenceSessionRefreshFailures.With(handler).Record(1)
		return
	}

	newConn, err := h.connect()
	if err != nil {
		h.logger.Error("sql handle: unable to refresh database connection pool", tag.Error(err))
		handler := h.metrics.WithTags(metrics.FailureTag("error"))
		metrics.PersistenceSessionRefreshFailures.With(handler).Record(1)
		return
	}

	prevConn := h.db.Swap(newConn)
	if prevConn != nil {
		go prevConn.Close()
	}
}

func (h *DatabaseHandle) Close() {
	// Already stopped
	if !h.running.Swap(true) {
		return
	}
	h.db.Load().Close()
}

func (h *DatabaseHandle) DB() *sqlx.DB {
	return h.db.Load()
}

func (h *DatabaseHandle) Conn() Conn {
	return h.db.Load()
}

func (h *DatabaseHandle) HandleError(err error) {
	if h.needsRefresh(err) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) {
		h.reconnect()
	}
}
