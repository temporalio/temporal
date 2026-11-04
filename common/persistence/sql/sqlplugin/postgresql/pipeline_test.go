package postgresql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
)

type (
	// otherDriverConnector connects with a driver other than pgx's stdlib, as a
	// custom SQL.Connect can.
	otherDriverConnector struct{}

	otherDriverConn struct {
		sqldriver.Conn
	}
)

func TestBeginPipelinedTx_PQUnsupported(t *testing.T) {
	t.Parallel()
	pdb := newDB(sqlplugin.DbKindMain, "temporal", &driver.PQDriver{}, nil, nil, log.NewNoopLogger())

	_, err := pdb.BeginPipelinedTx(context.Background())
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
}

func TestCommit_OtherDriverConnectionNotWritten(t *testing.T) {
	t.Parallel()
	sqlDB := sqlx.NewDb(sql.OpenDB(otherDriverConnector{}), "pgx")
	handle := sqlplugin.NewDatabaseHandle(
		sqlplugin.DbKindMain,
		func() (*sqlx.DB, error) { return sqlDB, nil },
		func(error) bool { return false },
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	defer handle.Close()
	pdb := newDB(sqlplugin.DbKindMain, "temporal", &driver.PGXDriver{}, handle, nil, log.NewNoopLogger())

	tx, err := pdb.BeginPipelinedTx(context.Background())
	require.NoError(t, err)
	require.ErrorIs(t, tx.Commit(), sqlplugin.ErrPipelineNotWritten)
}

func (otherDriverConnector) Connect(context.Context) (sqldriver.Conn, error) {
	return otherDriverConn{}, nil
}

func (otherDriverConnector) Driver() sqldriver.Driver {
	return nil
}

func (otherDriverConn) Close() error {
	return nil
}
