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
	_ log.Logger,
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	conn, err := p.connPool.Allocate(cfg, r, p.createDBConnection)
	if err != nil {
		return nil, err
	}
	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
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
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("error building DSN: %w", err)
	}

	db, err := sqlx.Connect(goSQLDriverName, dsn)
	if err != nil {
		return nil, err
	}

	// The following options are set based on advice from https://github.com/mattn/go-sqlite3#faq
	//
	// Dealing with the error `database is locked`
	// > ... set the database connections of the SQL package to 1.
	db.SetMaxOpenConns(1)
	// Settings for in-memory database (should be fine for file mode as well)
	// > Note that if the last database connection in the pool closes, the in-memory database is deleted.
	// > Make sure the max idle connection limit is > 0, and the connection lifetime is infinite.
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	switch {
	case cfg.ConnectAttributes["mode"] == "memory":
		// creates temporary DB overlay in order to configure database and schemas
		if err := p.setupSQLiteDatabase(cfg, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	case cfg.ConnectAttributes["setup"] == "true": // file mode, optional setting to setup the schema
		if err := p.setupSQLiteDatabase(cfg, db); err != nil && !isTableExistsError(err) { // benign error indicating tables already exist
			_ = db.Close()
			return nil, err
		}
	}

	return db, nil
}

func (p *plugin) setupSQLiteDatabase(cfg *config.SQL, conn *sqlx.DB) error {
	db := newDB(sqlplugin.DbKindUnknown, cfg.DatabaseName, conn, nil)
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
