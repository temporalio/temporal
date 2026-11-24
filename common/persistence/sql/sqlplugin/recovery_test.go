package sqlplugin

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
)

func TestDatabaseHandleRefreshPreservesConnectionLimit(t *testing.T) {
	t.Parallel()

	var active, peak, opened, pools atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(context.Context) (driver.Conn, error) {
				n := active.Add(1)
				for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
				}
				opened.Add(1)
				return &recoveryConnection{onClose: func() { active.Add(-1) }}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	ts := clock.NewEventTimeSource().Update(time.Now())
	c.timeSource = ts
	h := NewDatabaseHandle(DbKindUnknown, func() (*sqlx.DB, error) {
		pools.Add(1)
		p := sqlx.NewDb(sql.OpenDB(c), "recovery")
		p.SetMaxOpenConns(3)
		p.SetMaxIdleConns(3)
		return p, nil
	}, func(error) bool { return false }, log.NewNoopLogger(), metrics.NoopMetricsHandler, ts)
	t.Cleanup(h.Close)
	p, err := h.DB()
	require.NoError(t, err)
	tx, err := p.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := tx.Rollback()
		if !errors.Is(err, sql.ErrTxDone) {
			require.NoError(t, err)
		}
	})
	rows, err := p.QueryContext(t.Context(), "held rows")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	for range 10 {
		ts.Advance(sessionRefreshMinInternal + time.Millisecond)
		require.Error(t, h.ConvertError(io.EOF))
		current, err := h.DB()
		require.NoError(t, err)
		require.Same(t, p, current)
		_, err = current.ExecContext(t.Context(), "another operation")
		require.NoError(t, err)
		require.LessOrEqual(t, active.Load(), int32(3))
	}
	require.Equal(t, int32(1), pools.Load())
	require.Equal(t, int32(3), peak.Load())
	require.Greater(t, opened.Load(), int32(3))
}

func TestDatabaseHandleRefreshRecoversAfterFailedDial(t *testing.T) {
	t.Parallel()

	var endpoint atomic.Value
	endpoint.Store("initial")
	var lastDial atomic.Value
	var pools atomic.Int32
	ts := clock.NewEventTimeSource().Update(time.Now())
	c := NewRefreshingConnector(
		func() (string, error) { return endpoint.Load().(string), nil },
		func(dsn string) (driver.Connector, error) {
			return recoveryConnector{connect: func(context.Context) (driver.Conn, error) {
				lastDial.Store(dsn)
				if dsn == "unavailable" {
					return nil, context.DeadlineExceeded
				}
				return &recoveryConnection{}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	c.timeSource = ts
	h := NewDatabaseHandle(DbKindUnknown, func() (*sqlx.DB, error) {
		pools.Add(1)
		return sqlx.NewDb(sql.OpenDB(c), "recovery"), nil
	}, func(error) bool { return false }, log.NewNoopLogger(), metrics.NoopMetricsHandler, ts)
	t.Cleanup(h.Close)
	p, err := h.DB()
	require.NoError(t, err)
	_, err = p.ExecContext(t.Context(), "initial operation")
	require.NoError(t, err)

	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	endpoint.Store("unavailable")
	require.Error(t, h.ConvertError(io.EOF))
	_, err = p.ExecContext(t.Context(), "operation during failover")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, h.ConvertError(err), context.DeadlineExceeded)

	endpoint.Store("replacement")
	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	_, err = p.ExecContext(t.Context(), "operation after failover")
	require.NoError(t, err)
	require.Equal(t, "replacement", lastDial.Load())
	require.Equal(t, int32(1), pools.Load())
}

func TestDatabaseHandleDialErrorsDoNotInvalidateRecovery(t *testing.T) {
	t.Parallel()

	var attempts, closed atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				switch attempts.Add(1) {
				case 1:
					return nil, syscall.ECONNREFUSED
				case 2:
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				default:
				}
				return &recoveryConnection{onClose: func() { closed.Add(1) }}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	ts := clock.NewEventTimeSource().Update(time.Now())
	c.timeSource = ts
	metricsHandler := metricstest.NewCaptureHandler()
	h := NewDatabaseHandle(DbKindUnknown, func() (*sqlx.DB, error) {
		p := sqlx.NewDb(sql.OpenDB(c), "recovery")
		p.SetMaxOpenConns(2)
		return p, nil
	}, func(error) bool { return false }, log.NewNoopLogger(), metricsHandler, ts)
	t.Cleanup(h.Close)
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	p, err := h.DB()
	require.NoError(t, err)
	_, err = p.ExecContext(t.Context(), "initial failed connection")
	require.ErrorIs(t, err, syscall.ECONNREFUSED)
	require.Error(t, h.ConvertError(err))
	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	done := make(chan error, 1)
	go func() {
		_, err := p.ExecContext(t.Context(), "slow recovery connection")
		done <- err
	}()
	await.Rcv(t, started)
	for range 3 {
		ts.Advance(sessionRefreshMinInternal + time.Millisecond)
		_, err := p.ExecContext(t.Context(), "request during recovery")
		require.ErrorIs(t, err, syscall.ECONNREFUSED)
		require.Error(t, h.ConvertError(err))
	}
	close(release)
	require.NoError(t, await.Rcv(t, done))
	require.Equal(t, int32(2), attempts.Load())
	require.Zero(t, closed.Load())
	require.Len(t, capture.SnapshotMetric(metrics.PersistenceSessionRefreshAttempts.Name()), 1)
	failures := capture.SnapshotMetric(metrics.PersistenceSessionRefreshFailures.Name())
	require.Len(t, failures, 1)
	require.Equal(t, "error", failures[0].Tags[metrics.FailureTag("").Key])
}

func TestRefreshingConnectorDiscardsDialBeforeRefresh(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls, closed atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				if calls.Add(1) == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return &recoveryConnection{onClose: func() { closed.Add(1) }}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	type result struct {
		conn driver.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := c.Connect(t.Context())
		done <- result{conn: conn, err: err}
	}()
	await.Rcv(t, started)
	c.Driver().(interface{ Refresh() }).Refresh()
	close(release)
	r := await.Rcv(t, done)
	require.ErrorIs(t, r.err, driver.ErrBadConn)
	require.Nil(t, r.conn)
	require.Equal(t, int32(1), closed.Load())
	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestRefreshingConnectorDoesNotReplayExecutionError(t *testing.T) {
	t.Parallel()

	for _, executionErr := range []error{io.ErrUnexpectedEOF, errors.New("read-only transaction")} {
		t.Run(executionErr.Error(), func(t *testing.T) {
			t.Parallel()
			var executions atomic.Int32
			c := NewRefreshingConnector(
				func() (string, error) { return "database", nil },
				func(string) (driver.Connector, error) {
					return recoveryConnector{connect: func(context.Context) (driver.Conn, error) {
						return &recoveryConnection{executions: &executions, executionErr: executionErr}, nil
					}}, nil
				},
				recoveryDriver{},
			)
			db := sql.OpenDB(c)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			_, err := db.ExecContext(t.Context(), "non-idempotent write")
			require.ErrorIs(t, err, executionErr)
			require.Equal(t, int32(1), executions.Load())
		})
	}
}

func TestRefreshingConnectorPreservesCanceledTransactionCleanup(t *testing.T) {
	t.Parallel()

	var opened, closed atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(context.Context) (driver.Conn, error) {
				opened.Add(1)
				return &recoveryConnection{onClose: func() { closed.Add(1) }}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	_, validator := conn.(driver.Validator)
	require.False(t, validator)
	require.NoError(t, conn.Close())

	db := sql.OpenDB(c)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	_, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	cancel()
	await.RequireTrue(t, func() bool { return closed.Load() == 2 }, time.Second, time.Millisecond)
	_, err = db.ExecContext(t.Context(), "operation after cancellation")
	require.NoError(t, err)
	require.Equal(t, int32(3), opened.Load())
}

func TestRefreshingConnectorCanceledDialDoesNotDelayRecovery(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	var calls atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				if calls.Add(1) == 1 {
					close(started)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &recoveryConnection{}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		_, err := c.Connect(ctx)
		done <- err
	}()
	await.Rcv(t, started)
	cancel()
	require.ErrorIs(t, await.Rcv(t, done), context.Canceled)
	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Equal(t, int32(2), calls.Load())
}

func TestRefreshingConnectorAllowsOneRecoveryProbe(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				switch calls.Add(1) {
				case 1:
					return nil, io.ErrUnexpectedEOF
				case 2:
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				default:
				}
				return &recoveryConnection{}, nil
			}}, nil
		},
		recoveryDriver{},
	)
	ts := clock.NewEventTimeSource().Update(time.Now())
	c.timeSource = ts
	_, err := c.Connect(t.Context())
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = c.Connect(t.Context())
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, int32(1), calls.Load())
	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	type result struct {
		conn driver.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := c.Connect(t.Context())
		done <- result{conn: conn, err: err}
	}()
	await.Rcv(t, started)
	for range 3 {
		_, err = c.Connect(t.Context())
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	}
	require.Equal(t, int32(2), calls.Load())
	close(release)
	r := await.Rcv(t, done)
	require.NoError(t, r.err)
	require.NoError(t, r.conn.Close())
	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Equal(t, int32(3), calls.Load())
}

func TestRefreshingConnectorAllowsConcurrentHealthyDials(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				started <- struct{}{}
				select {
				case <-release:
					return &recoveryConnection{}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}, nil
		},
		recoveryDriver{},
	)
	type result struct {
		conn driver.Conn
		err  error
	}
	done := make(chan result, 2)
	for range 2 {
		go func() {
			conn, err := c.Connect(t.Context())
			done <- result{conn: conn, err: err}
		}()
	}
	for range 2 {
		await.Rcv(t, started)
	}
	close(release)
	for range 2 {
		r := await.Rcv(t, done)
		require.NoError(t, r.err)
		require.NoError(t, r.conn.Close())
	}
}

func TestDatabaseHandleCloseDoesNotWaitForDial(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}}, nil
		},
		recoveryDriver{},
	)
	h := NewDatabaseHandle(DbKindUnknown, func() (*sqlx.DB, error) {
		return sqlx.NewDb(sql.OpenDB(c), "recovery"), nil
	}, func(error) bool { return false }, log.NewNoopLogger(), metrics.NoopMetricsHandler, clock.NewRealTimeSource())
	t.Cleanup(h.Close)
	p, err := h.DB()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	operation := make(chan error, 1)
	go func() {
		_, err := p.ExecContext(ctx, "operation during outage")
		operation <- err
	}()
	await.Rcv(t, started)
	shutdown := make(chan struct{})
	go func() {
		h.Close()
		close(shutdown)
	}()
	await.Rcv(t, shutdown)
	cancel()
	require.ErrorIs(t, await.Rcv(t, operation), context.Canceled)
	_, err = h.DB()
	require.ErrorIs(t, err, DatabaseUnavailableError)
}

type recoveryDriver struct{}

func (recoveryDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use the connector")
}

type recoveryConnector struct {
	connect func(context.Context) (driver.Conn, error)
}

func (c recoveryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.connect(ctx)
}

func (recoveryConnector) Driver() driver.Driver {
	return recoveryDriver{}
}

type recoveryConnection struct {
	closed       atomic.Bool
	onClose      func()
	executions   *atomic.Int32
	executionErr error
}

func (c *recoveryConnection) Close() error {
	if c.closed.CompareAndSwap(false, true) && c.onClose != nil {
		c.onClose()
	}
	return nil
}

func (c *recoveryConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}

func (c *recoveryConnection) Begin() (driver.Tx, error) {
	return recoveryTransaction{}, nil
}

func (c *recoveryConnection) ResetSession(context.Context) error {
	return nil
}

func (c *recoveryConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.executions != nil {
		c.executions.Add(1)
	}
	return driver.RowsAffected(1), c.executionErr
}

func (c *recoveryConnection) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return recoveryRows{}, nil
}

type recoveryTransaction struct{}

func (recoveryTransaction) Commit() error   { return nil }
func (recoveryTransaction) Rollback() error { return nil }

type recoveryRows struct{}

func (recoveryRows) Columns() []string         { return []string{"value"} }
func (recoveryRows) Close() error              { return nil }
func (recoveryRows) Next([]driver.Value) error { return io.EOF }
