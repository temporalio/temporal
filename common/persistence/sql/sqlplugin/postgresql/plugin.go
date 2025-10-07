package postgresql

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/session"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName    = "postgres12"
	PluginNamePGX = "postgres12_pgx"
)

var defaultDatabaseNames = []string{
	"postgres",  // normal PostgreSQL default DB name
	"defaultdb", // special behavior for Aiven: #1389
}

type plugin struct {
	driver         driver.Driver
	queryConverter sqlplugin.VisibilityQueryConverter
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{
		driver:         &driver.PQDriver{},
		queryConverter: &queryConverter{},
	})
	sql.RegisterPlugin(PluginNamePGX, &plugin{
		driver:         &driver.PGXDriver{},
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
	needsRefresh := p.driver.IsConnNeedsRefreshError
	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, needsRefresh, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDB(dbKind, cfg.DatabaseName, p.driver, handle, nil)
	return db, nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (p *plugin) createDBConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	if cfg.DatabaseName != "" {
		postgresqlSession, err := session.NewSession(cfg, p.driver, resolver)
		if err != nil {
			return nil, err
		}
		return postgresqlSession.DB, nil
	}

	// database name not provided
	// try defaults
	defer func() { cfg.DatabaseName = "" }()

	var errors []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		if postgresqlSession, err := session.NewSession(
			cfg,
			p.driver,
			resolver,
		); err == nil {
			return postgresqlSession.DB, nil
		} else {
			errors = append(errors, err)
		}
	}
	return nil, serviceerror.NewUnavailable(
		fmt.Sprintf("unable to connect to DB, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errors),
	)
}
