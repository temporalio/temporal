package mssql

import (
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mssql/session"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin. Requires SQL Server 2019+
	// (UTF-8 collations, deterministic JSON functions used by the
	// visibility schema).
	PluginName = "mssql2019"
)

// defaultDatabaseName is used to establish an admin connection when no
// database name is configured (e.g. temporal-sql-tool create-database).
const defaultDatabaseName = "master"

type plugin struct {
	queryConverter sqlplugin.VisibilityQueryConverter
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{
		queryConverter: &queryConverter{},
	})
}

func (p *plugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return p.queryConverter
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (sqlplugin.GenericDB, error) {
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.createDBConnection(cfg, r)
	}
	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, isConnNeedsRefreshError, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDB(dbKind, cfg.DatabaseName, handle, nil, logger)
	return db, nil
}

// createDBConnection creates and returns a reference to a logical connection
// to the underlying SQL Server database. The returned object is tied to a
// single database and can be used to perform CRUD operations on the tables
// in the database.
func (p *plugin) createDBConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	if cfg.DatabaseName != "" {
		mssqlSession, err := session.NewSession(cfg, resolver)
		if err != nil {
			return nil, err
		}
		return mssqlSession.DB, nil
	}

	// Database name not provided, connect to master for admin operations.
	// Use a shallow copy so that the session's refreshable-connection closure
	// (which captures the config pointer) sees the correct DatabaseName on
	// reconnect
	cfgCopy := *cfg
	cfgCopy.DatabaseName = defaultDatabaseName
	mssqlSession, err := session.NewSession(&cfgCopy, resolver)
	if err != nil {
		return nil, err
	}
	return mssqlSession.DB, nil
}
