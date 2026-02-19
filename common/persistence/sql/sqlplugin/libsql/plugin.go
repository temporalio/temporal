package libsql

import (
	"fmt"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

const (
	// PluginName is the name of the plugin
	PluginName = "libsql"
)

type plugin struct {
	queryConverter sqlplugin.VisibilityQueryConverter
	connPool       *connPool
}

func init() {
	sql.RegisterPlugin(PluginName, &plugin{
		queryConverter: &queryConverter{},
		connPool:       newConnPool(),
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
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	conn, err := p.connPool.Allocate(cfg, r, logger, p.createDBConnection)
	if err != nil {
		return nil, err
	}
	db := newDB(dbKind, cfg.DatabaseName, conn, nil, logger)
	db.OnClose(func() { p.connPool.Close(cfg) })
	return db, nil
}

// createDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database.
func (p *plugin) createDBConnection(
	cfg *config.SQL,
	_ resolver.ServiceResolver,
	logger log.Logger,
) (*sqlx.DB, error) {
	dsn := buildDSN(cfg)

	db, err := sqlx.Connect(goSQLDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("libsql connect (%s): %w", dsn, err)
	}

	// Single connection to avoid "database is locked" with file-backed engines.
	// See https://github.com/mattn/go-sqlite3#faq
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	switch {
	case cfg.ConnectAttributes["mode"] == "memory":
		if err := p.setupDatabase(cfg, db, logger); err != nil {
			_ = db.Close()
			return nil, err
		}
	case cfg.ConnectAttributes["setup"] == "true":
		if err := p.setupDatabase(cfg, db, logger); err != nil && !isTableExistsError(err) {
			_ = db.Close()
			return nil, err
		}
	}

	return db, nil
}

func (p *plugin) setupDatabase(cfg *config.SQL, conn *sqlx.DB, logger log.Logger) error {
	db := newDB(sqlplugin.DbKindUnknown, cfg.DatabaseName, conn, nil, logger)
	defer func() { _ = db.Close() }()

	err := db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		return err
	}

	return sqliteschema.SetupSchemaOnDB(db)
}

func buildDSN(cfg *config.SQL) string {
	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = make(map[string]string)
	}

	name := cfg.DatabaseName

	if name == ":memory:" || cfg.ConnectAttributes["mode"] == "memory" {
		return ":memory:"
	}

	// go-libsql expects file: prefix
	if !strings.HasPrefix(name, "file:") {
		return "file:" + name
	}

	return name
}
