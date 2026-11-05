package postgresql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
)

type (
	// otherDriverConnector connects with connections that are not pgx's, as a
	// custom SQL.Connect can.
	otherDriverConnector struct{}

	otherDriverConn struct {
		sqldriver.Conn
	}
)

// The first connection decides that the DB pipelines.
func TestBeginPipelinedTx_Supported(t *testing.T) {
	t.Parallel()
	pdb := newPipelineTestDB(t, stdlib.GetConnector(*fakeServerConfig(t, serveFake)))

	tx, err := pdb.BeginPipelinedTx(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, pipeliningSupported, pdb.pipelining.Load())
}

func TestBeginPipelinedTx_OtherDriverConnectionUnsupported(t *testing.T) {
	t.Parallel()
	pdb := newPipelineTestDB(t, otherDriverConnector{})

	_, err := pdb.BeginPipelinedTx(context.Background())
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
	sqlDB, err := pdb.handle.DB()
	require.NoError(t, err)
	require.Zero(t, sqlDB.Stats().InUse)
}

// A DB that rejected a connection rejects later transactions without taking one.
func TestBeginPipelinedTx_SimpleProtocolUnsupported(t *testing.T) {
	t.Parallel()
	config := fakeServerConfig(t, serveFake)
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pdb := newPipelineTestDB(t, stdlib.GetConnector(*config))

	_, err := pdb.BeginPipelinedTx(context.Background())
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pdb.BeginPipelinedTx(ctx)
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
}

// A pipelined transaction holds a pooled connection from Begin until Rollback.
func TestRollback_ReleasesConnection(t *testing.T) {
	t.Parallel()
	pdb := newPipelineTestDB(t, stdlib.GetConnector(*fakeServerConfig(t, serveFake)))
	sqlDB, err := pdb.handle.DB()
	require.NoError(t, err)

	tx, err := pdb.BeginPipelinedTx(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, sqlDB.Stats().InUse)
	require.NoError(t, tx.Rollback())
	require.Zero(t, sqlDB.Stats().InUse)
}

// Only a division by zero at a check statement is that check failing.
func TestSend_CheckFailure(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		code      string
		checkErr  error
		wantCheck bool
	}{
		{name: "division by zero at a check", code: "22012", checkErr: sqlplugin.ErrShardRangeIDMismatch, wantCheck: true},
		{name: "deadlock at a check", code: "40P01", checkErr: sqlplugin.ErrShardRangeIDMismatch},
		{name: "division by zero at another statement", code: "22012"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := fakeServerConfig(t, serveFakeError(tc.code))
			config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			conn, err := pgx.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			p := &pipeline{
				ctx:        context.Background(),
				statements: []pipelinedStatement{{sql: "SELECT 1", checkErr: tc.checkErr}},
			}

			err = p.send(conn)
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, tc.code, pgErr.Code)
			if tc.wantCheck {
				require.ErrorIs(t, err, tc.checkErr)
			} else {
				require.Same(t, pgErr, err)
			}
		})
	}
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

// fakeServerConfig returns a pgx config whose connections go to an in-process
// server that runs serve.
func fakeServerConfig(t *testing.T, serve func(net.Conn)) *pgx.ConnConfig {
	config, err := pgx.ParseConfig("host=fake user=temporal sslmode=disable")
	require.NoError(t, err)
	config.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serve(server)
		return client, nil
	}
	return config
}

func serveFake(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	if startFake(conn) == nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn)
}

// serveFakeError answers the first query with an error with code.
func serveFakeError(code string) func(net.Conn) {
	return func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		backend := startFake(conn)
		if backend == nil {
			return
		}
		if _, err := backend.Receive(); err != nil {
			return
		}
		backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: "fake error"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}
}

// startFake completes the startup handshake, and returns nil when it fails.
func startFake(conn net.Conn) *pgproto3.Backend {
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return nil
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	// pgx runs simple protocol queries only with these settings.
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return nil
	}
	return backend
}

func (c otherDriverConnector) Connect(context.Context) (sqldriver.Conn, error) {
	return otherDriverConn{}, nil
}

func (c otherDriverConnector) Driver() sqldriver.Driver {
	return stdlib.GetDefaultDriver()
}

func (otherDriverConn) Close() error {
	return nil
}
