package postgresql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"net/url"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/resolver"
)

func TestDefaultDatabaseDiscoveryPingsCandidates(t *testing.T) {
	t.Parallel()
	cfg := &config.SQL{ConnectAddr: "database:5432"}
	d := &discoveryDriver{}
	p := &plugin{driver: d}
	db, err := p.createDBConnection(cfg, resolver.NewNoopResolver())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.Equal(t, []string{"/postgres", "/defaultdb"}, d.attempts)
	require.Empty(t, cfg.DatabaseName)
	require.ErrorContains(t, d.pools[0].PingContext(t.Context()), "database is closed")
	require.NoError(t, db.PingContext(t.Context()))
}

type discoveryDriver struct {
	driver.PQDriver
	attempts []string
	pools    []*sqlx.DB
}

func (d *discoveryDriver) CreateRefreshableConnection(buildDSN func() (string, error)) (*sqlx.DB, error) {
	c := &discoveryConnector{buildDSN: buildDSN, attempts: &d.attempts}
	db := sqlx.NewDb(sql.OpenDB(c), "postgres")
	d.pools = append(d.pools, db)
	return db, nil
}

type discoveryConnector struct {
	buildDSN func() (string, error)
	attempts *[]string
}

func (c *discoveryConnector) Connect(context.Context) (sqldriver.Conn, error) {
	dsn, err := c.buildDSN()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	*c.attempts = append(*c.attempts, u.Path)
	if u.Path == "/postgres" {
		return nil, errors.New("database does not exist")
	}
	return discoveryConn{}, nil
}

func (*discoveryConnector) Driver() sqldriver.Driver { return nil }

type discoveryConn struct{}

func (discoveryConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (discoveryConn) Close() error                 { return nil }
func (discoveryConn) Begin() (sqldriver.Tx, error) { return nil, errors.New("not implemented") }
