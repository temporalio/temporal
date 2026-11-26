package sqlplugin

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/await"
)

func TestRefreshingConnectorPreservesValidatorOnlyTransactionCancellation(t *testing.T) {
	t.Parallel()

	for _, resettable := range []bool{false, true} {
		name := "validator only"
		if resettable {
			name = "validator and resetter"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var opened, closed atomic.Int32
			rolledBack := make(chan struct{}, 1)
			c := NewRefreshingConnector(
				func() (string, error) { return "database", nil },
				func(string) (driver.Connector, error) {
					return recoveryConnector{connect: func(context.Context) (driver.Conn, error) {
						opened.Add(1)
						conn := &validatorOnlyConnection{closed: &closed, rolledBack: rolledBack}
						if resettable {
							return &resettingValidatorConnection{validatorOnlyConnection: conn}, nil
						}
						return conn, nil
					}}, nil
				},
				recoveryDriver{},
			)
			db := sql.OpenDB(c)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			_, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			cancel()
			await.Rcv(t, rolledBack)
			_, err = db.ExecContext(t.Context(), "operation after canceled transaction")
			require.NoError(t, err)
			if resettable {
				require.Equal(t, int32(1), opened.Load())
				require.Zero(t, closed.Load())
			} else {
				require.Equal(t, int32(2), opened.Load())
				require.Equal(t, int32(1), closed.Load())
			}
		})
	}
}

func TestRefreshingConnectorKeepsProbeOwnedByItsAttempt(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	dialErr := errors.New("database unavailable")
	var attempts atomic.Int32
	c := NewRefreshingConnector(
		func() (string, error) { return "database", nil },
		func(string) (driver.Connector, error) {
			return recoveryConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				switch attempts.Add(1) {
				case 1:
					close(firstStarted)
					<-ctx.Done()
					return nil, ctx.Err()
				case 2:
					return nil, dialErr
				case 3:
					close(probeStarted)
					select {
					case <-releaseProbe:
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
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.Connect(ctx)
		firstDone <- err
	}()
	await.Rcv(t, firstStarted)
	_, err := c.Connect(t.Context())
	require.ErrorIs(t, err, dialErr)
	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	type result struct {
		conn driver.Conn
		err  error
	}
	probeDone := make(chan result, 1)
	go func() {
		conn, err := c.Connect(t.Context())
		probeDone <- result{conn: conn, err: err}
	}()
	await.Rcv(t, probeStarted)
	cancel()
	require.ErrorIs(t, await.Rcv(t, firstDone), context.Canceled)
	conn, err := c.Connect(t.Context())
	if conn != nil {
		require.NoError(t, conn.Close())
	}
	require.ErrorIs(t, err, dialErr)
	require.Equal(t, int32(3), attempts.Load())
	close(releaseProbe)
	probe := await.Rcv(t, probeDone)
	require.NoError(t, probe.err)
	require.NoError(t, probe.conn.Close())
}

type validatorOnlyConnection struct {
	closed     *atomic.Int32
	rolledBack chan struct{}
}

func (*validatorOnlyConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (c *validatorOnlyConnection) Close() error {
	c.closed.Add(1)
	return nil
}
func (c *validatorOnlyConnection) Begin() (driver.Tx, error) {
	return validatorOnlyTransaction{rolledBack: c.rolledBack}, nil
}
func (*validatorOnlyConnection) IsValid() bool { return true }
func (*validatorOnlyConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

type resettingValidatorConnection struct{ *validatorOnlyConnection }

func (*resettingValidatorConnection) ResetSession(context.Context) error { return nil }

type validatorOnlyTransaction struct{ rolledBack chan struct{} }

func (validatorOnlyTransaction) Commit() error { return nil }
func (tx validatorOnlyTransaction) Rollback() error {
	tx.rolledBack <- struct{}{}
	return nil
}
