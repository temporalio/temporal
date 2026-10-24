package sql

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/tools/common/schema"
)

type (
	// Connection is the connection to database
	Connection struct {
		dbName       string
		adminDB      sqlplugin.AdminDB
		visibilityDB sqlplugin.VisibilityVersionDB
	}
)

const dbType = "sql"

var _ schema.DB = (*Connection)(nil)

// NewConnection creates a new connection to database
func NewConnection(cfg *config.SQL, logger log.Logger) (*Connection, error) {
	return newConnection(cfg, logger, false)
}

func newConnection(cfg *config.SQL, logger log.Logger, visibility bool) (*Connection, error) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), logger, metrics.NoopMetricsHandler)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		adminDB: db,
		dbName:  cfg.DatabaseName,
	}
	if visibility {
		visibilityDB, ok := db.(sqlplugin.VisibilityVersionDB)
		if !ok {
			_ = db.Close()
			return nil, schema.NewConfigError("--visibility is not supported by SQL plugin " + cfg.PluginName)
		}
		connection.visibilityDB = visibilityDB
	}
	return connection, nil
}

// CreateSchemaVersionTables sets up the schema version tables
func (c *Connection) CreateSchemaVersionTables() error {
	if c.visibilityDB != nil {
		return c.visibilityDB.CreateVisibilityVersionTables()
	}
	return c.adminDB.CreateSchemaVersionTables()
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (c *Connection) ReadSchemaVersion() (string, error) {
	if c.visibilityDB != nil {
		return c.visibilityDB.ReadVisibilityVersion(c.dbName)
	}
	return c.adminDB.ReadSchemaVersion(c.dbName)
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (c *Connection) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	if c.visibilityDB != nil {
		return c.visibilityDB.UpdateVisibilityVersion(c.dbName, newVersion, minCompatibleVersion)
	}
	return c.adminDB.UpdateSchemaVersion(c.dbName, newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (c *Connection) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	if c.visibilityDB != nil {
		return c.visibilityDB.WriteVisibilityUpdateLog(oldVersion, newVersion, manifestMD5, desc)
	}
	return c.adminDB.WriteSchemaUpdateLog(oldVersion, newVersion, manifestMD5, desc)
}

// Exec executes a sql statement
func (c *Connection) Exec(stmt string, args ...any) error {
	return c.adminDB.Exec(stmt, args...)
}

// ListTables returns a list of tables in this database
func (c *Connection) ListTables() ([]string, error) {
	return c.adminDB.ListTables(c.dbName)
}

// DropTable drops a given table from the database
func (c *Connection) DropTable(name string) error {
	return c.adminDB.DropTable(name)
}

// DropAllTables drops all tables from this database
func (c *Connection) DropAllTables() error {
	tables, err := c.ListTables()
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := c.DropTable(tab); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
func (c *Connection) CreateDatabase(name string) error {
	return c.adminDB.CreateDatabase(name)
}

// DropDatabase drops a database
func (c *Connection) DropDatabase(name string) error {
	return c.adminDB.DropDatabase(name)
}

// Close closes the sql client
func (c *Connection) Close() {
	if c.adminDB != nil {
		err := c.adminDB.Close()
		if err != nil {
			panic("cannot close connection")
		}
	}
}

// Type gives the type of db
func (c *Connection) Type() string {
	return dbType
}
