package sqlplugin

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

// RefreshingConnector refreshes connection settings without replacing the pool.
type RefreshingConnector struct {
	buildDSN     func() (string, error)
	newConnector func(dsn string) (driver.Connector, error)
	driver       driver.Driver
	generation   atomic.Uint64
	timeSource   clock.TimeSource
	mu           sync.Mutex
	connectErr   error
	retryAfter   time.Time
	probing      bool
	attempt      uint64
	lastSuccess  uint64
}

type refreshingDriver struct {
	driver.Driver
	connector *RefreshingConnector
}

type connectError struct {
	err        error
	reportable bool
	reported   atomic.Bool
}

func (e *connectError) Error() string { return e.err.Error() }
func (e *connectError) Unwrap() error { return e.err }

func NewRefreshingConnector(
	buildDSN func() (string, error),
	newConnector func(dsn string) (driver.Connector, error),
	d driver.Driver,
) *RefreshingConnector {
	return &RefreshingConnector{
		buildDSN:     buildDSN,
		newConnector: newConnector,
		driver:       d,
		timeSource:   clock.NewRealTimeSource(),
	}
}

func (c *RefreshingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	probe := false
	if c.connectErr != nil {
		if c.probing || c.timeSource.Now().Before(c.retryAfter) {
			err := c.connectErr
			c.mu.Unlock()
			return nil, err
		}
		c.probing = true
		probe = true
	}
	c.attempt++
	attempt := c.attempt
	generation := c.generation.Load()
	c.mu.Unlock()

	conn, err := c.connect(ctx)
	c.mu.Lock()
	if probe {
		c.probing = false
	}
	if err != nil {
		err = &connectError{err: err, reportable: ctx.Err() == nil}
		if ctx.Err() == nil && attempt >= c.lastSuccess {
			c.connectErr = err
			c.retryAfter = c.timeSource.Now().Add(sessionRefreshMinInternal)
		}
		c.mu.Unlock()
		return nil, err
	}
	if generation != c.generation.Load() {
		c.mu.Unlock()
		return nil, &connectError{err: errors.Join(driver.ErrBadConn, conn.Close())}
	}
	c.lastSuccess = max(c.lastSuccess, attempt)
	c.connectErr = nil
	c.retryAfter = time.Time{}
	c.mu.Unlock()
	wrapped := &refreshingConn{Conn: conn, connector: c, generation: generation}
	if _, ok := conn.(driver.Validator); ok {
		return &validatingConn{refreshingConn: wrapped}, nil
	}
	return wrapped, nil
}

func (c *RefreshingConnector) connect(ctx context.Context) (driver.Conn, error) {
	dsn, err := c.buildDSN()
	if err != nil {
		return nil, err
	}
	connector, err := c.newConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (c *RefreshingConnector) Driver() driver.Driver {
	return refreshingDriver{Driver: c.driver, connector: c}
}

func (d refreshingDriver) Refresh() {
	d.connector.generation.Add(1)
}
