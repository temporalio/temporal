package postgresql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
)

type (
	// otherDriverConnector reports driver and connects with connections that are
	// not pgx's, as a custom SQL.Connect can.
	otherDriverConnector struct {
		driver sqldriver.Driver
	}

	otherDriverConn struct {
		sqldriver.Conn
	}
)

func TestBeginPipelinedTx_OtherDriverUnsupported(t *testing.T) {
	t.Parallel()
	pdb := newPipelineTestDB(t, otherDriverConnector{driver: &pq.Driver{}})

	_, err := pdb.BeginPipelinedTx(context.Background())
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
}

func TestCommit_OtherDriverConnectionNotWritten(t *testing.T) {
	t.Parallel()
	pdb := newPipelineTestDB(t, otherDriverConnector{driver: stdlib.GetDefaultDriver()})

	tx, err := pdb.BeginPipelinedTx(context.Background())
	require.NoError(t, err)
	require.ErrorIs(t, tx.Commit(), sqlplugin.ErrPipelineNotWritten)
}

// A batch whose context is done before pgx sends it writes nothing.
func TestSend_CanceledContextNotWritten(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &pipeline{ctx: ctx, statements: []pipelinedStatement{{sql: "SELECT 1"}}}

	notWritten, err := p.send(connectToFakeServer(t))
	require.True(t, notWritten)
	require.ErrorIs(t, err, context.Canceled)
}

// newPipelineTestDB returns a pgx plugin DB whose pool connects with connector.
func newPipelineTestDB(t *testing.T, connector sqldriver.Connector) *db {
	sqlDB := sqlx.NewDb(sql.OpenDB(connector), "pgx")
	handle := sqlplugin.NewDatabaseHandle(
		sqlplugin.DbKindMain,
		func() (*sqlx.DB, error) { return sqlDB, nil },
		func(error) bool { return false },
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	t.Cleanup(handle.Close)
	return newDB(sqlplugin.DbKindMain, "temporal", &driver.PGXDriver{}, handle, nil, log.NewNoopLogger())
}

// connectToFakeServer connects pgx to an in-process server that completes the
// startup handshake and then discards what it receives.
func connectToFakeServer(t *testing.T) *pgx.Conn {
	config, err := pgx.ParseConfig("host=fake user=temporal sslmode=disable")
	require.NoError(t, err)
	config.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveFake(server)
		return client, nil
	}
	conn, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func serveFake(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn)
}

func (c otherDriverConnector) Connect(context.Context) (sqldriver.Conn, error) {
	return otherDriverConn{}, nil
}

func (c otherDriverConnector) Driver() sqldriver.Driver {
	return c.driver
}

func (otherDriverConn) Close() error {
	return nil
}
