package postgres

import (
	"errors"
	"fmt"
	"net"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"github.com/temporalio/temporal/common/persistence/sql"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
	"github.com/temporalio/temporal/common/service/config"
)

const (
	// PluginName is the name of the plugin
	PluginName             = "postgres"
	dataSourceNamePostgres = "user=%v password=%v host=%v port=%v dbname=%v sslmode=disable "
)

var errTLSNotImplemented = errors.New("tls for postgres has not been implemented")

type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB initialize the db object
func (d *plugin) CreateDB(cfg *config.SQL) (sqlplugin.DB, error) {
	conn, err := d.createDBConnection(cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(conn, nil)
	return db, nil
}

// CreateAdminDB initialize the adminDB object
func (d *plugin) CreateAdminDB(cfg *config.SQL) (sqlplugin.AdminDB, error) {
	conn, err := d.createDBConnection(cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(conn, nil)
	return db, nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (d *plugin) createDBConnection(cfg *config.SQL) (*sqlx.DB, error) {
	err := registerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid connect address, it must be in host:port format, %v, err: %v", cfg.ConnectAddr, err)
	}

	dbName := cfg.DatabaseName
	//NOTE: postgres doesn't allow to connect with empty dbName, the admin dbName is "postgres"
	if dbName == "" {
		dbName = "postgres"
	}
	db, err := sqlx.Connect(PluginName, fmt.Sprintf(dataSourceNamePostgres, cfg.User, cfg.Password, host, port, dbName))

	if err != nil {
		return nil, err
	}

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

// TODO: implement postgres specific support for TLS
func registerTLSConfig(cfg *config.SQL) error {
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return nil
	}
	return errTLSNotImplemented
}
