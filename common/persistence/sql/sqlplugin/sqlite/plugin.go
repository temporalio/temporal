package sqlite

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	sqliteschema "go.temporal.io/server/schema/sqlite"
	expmaps "golang.org/x/exp/maps"
)

const (
	// PluginName is the name of the plugin
	PluginName = "sqlite"
)

// List of non-pragma parameters
// Taken from https://www.sqlite.org/uri.html
var queryParameters = map[string]struct{}{
	"cache":     {},
	"immutable": {},
	"mode":      {},
	"modeof":    {},
	"nolock":    {},
	"psow":      {},
	"setup":     {},
	"vfs":       {},
}

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
	db.OnClose(func() { p.connPool.Close(cfg) }) // remove reference
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
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("error building DSN: %w", err)
	}

	db, err := sqlx.Connect(goSQLDriverName, dsn)
	if err != nil {
		return nil, err
	}

	// Connection pool settings.
	//
	// By default, SQLite only supports a single writer at a time (see
	// https://github.com/mattn/go-sqlite3#faq). Without WAL (Write-Ahead Logging)
	// mode, concurrent connections will encounter "database is locked" errors.
	// With WAL mode enabled (journal_mode=wal), SQLite supports concurrent readers
	// alongside a single writer, making multiple connections safe and beneficial
	// for throughput.
	//
	// These settings mirror the behavior of the MySQL and PostgreSQL plugins:
	// respect the user's config values when set, otherwise default to 1 for
	// backward compatibility and safety.
	walEnabled := strings.EqualFold(cfg.ConnectAttributes["journal_mode"], "wal")
	if cfg.MaxConns > 0 {
		if cfg.MaxConns > 1 && !walEnabled {
			logger.Warn(
				"SQLite MaxConns > 1 without WAL mode (journal_mode=wal) may cause 'database is locked' errors. "+
					"Consider enabling WAL mode via ConnectAttributes.",
				tag.NewInt("maxConns", cfg.MaxConns),
			)
		}
		db.SetMaxOpenConns(cfg.MaxConns)
	} else {
		db.SetMaxOpenConns(1)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(1)
	}
	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}
	// For in-memory databases, the database is deleted when the last connection
	// closes. Set ConnMaxIdleTime to 0 (infinite) to prevent idle connections
	// from being reaped, which would destroy the database.
	if cfg.ConnectAttributes["mode"] == "memory" {
		db.SetConnMaxIdleTime(0)
	}

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	switch {
	case cfg.ConnectAttributes["mode"] == "memory":
		// creates temporary DB overlay in order to configure database and schemas
		if err := p.setupSQLiteDatabase(cfg, db, logger); err != nil {
			_ = db.Close()
			return nil, err
		}
	case cfg.ConnectAttributes["setup"] == "true": // file mode, optional setting to setup the schema
		if err := p.setupSQLiteDatabase(cfg, db, logger); err != nil && !isTableExistsError(err) { // benign error indicating tables already exist
			_ = db.Close()
			return nil, err
		}
	}

	return db, nil
}

func (p *plugin) setupSQLiteDatabase(cfg *config.SQL, conn *sqlx.DB, logger log.Logger) error {
	db := newDB(sqlplugin.DbKindUnknown, cfg.DatabaseName, conn, nil, logger)
	defer func() { _ = db.Close() }()

	err := db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		return err
	}

	// init tables
	return sqliteschema.SetupSchemaOnDB(db)
}

func buildDSN(cfg *config.SQL) (string, error) {
	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = make(map[string]string)
	}
	vals, err := buildDSNAttr(cfg)
	if err != nil {
		return "", err
	}
	dsn := fmt.Sprintf(
		"file:%s?%v",
		cfg.DatabaseName,
		vals.Encode(),
	)
	return dsn, nil
}

func buildDSNAttr(cfg *config.SQL) (url.Values, error) {
	parameters := url.Values{}

	// sort ConnectAttributes to get a deterministic order
	keys := expmaps.Keys(cfg.ConnectAttributes)
	sort.Strings(keys)

	for _, k := range keys {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(cfg.ConnectAttributes[k])
		if parameters.Get(key) != "" {
			return nil, fmt.Errorf("duplicate connection attr: %v:%v, %v:%v",
				key,
				parameters.Get(key),
				key, value,
			)
		}

		if _, isValidQueryParameter := queryParameters[key]; isValidQueryParameter {
			parameters.Set(key, value)
			continue
		}

		// assume pragma
		parameters.Add("_pragma", fmt.Sprintf("%s=%s", key, value))
	}
	// set time format
	parameters.Add("_time_format", "sqlite")
	return parameters, nil
}
