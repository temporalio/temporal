package sqlite

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

// db represents a logical connection to sqlite database
type db struct {
	dbKind sqlplugin.DbKind
	dbName string

	onClose func() error

	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
	logger    log.Logger
}

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying sqlite database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	xdb *sqlx.DB,
	tx *sqlx.Tx,
	logger log.Logger,
) *db {
	mdb := &db{
		dbKind: dbKind,
		dbName: dbName,
		db:     xdb,
		tx:     tx,
		logger: logger,
	}
	mdb.conn = xdb
	if tx != nil {
		mdb.conn = tx
	}
	mdb.converter = &converter{}
	return mdb
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	xtx, err := mdb.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return newDB(mdb.dbKind, mdb.dbName, mdb.db, xtx, mdb.logger), nil
}

// Commit commits a previously started transaction
func (mdb *db) Commit() error {
	return mdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (mdb *db) Rollback() error {
	return mdb.tx.Rollback()
}

func (mdb *db) OnClose(hook func() error) {
	mdb.onClose = hook
}

// Close closes the connection to the sqlite db
func (mdb *db) Close() error {
	// database connection will be automatically closed by the hook handler when all references are removed
	if mdb.onClose == nil {
		return nil
	}
	// de-registers the database from conn pool
	return mdb.onClose()
}

// PluginName returns the name of the plugin
func (mdb *db) PluginName() string {
	return PluginName
}

// DbName returns the name of the database
func (mdb *db) DbName() string {
	return mdb.dbName
}

// ExpectedVersion returns expected version.
func (mdb *db) ExpectedVersion() string {
	switch mdb.dbKind {
	case sqlplugin.DbKindMain:
		return sqliteschema.Version
	case sqlplugin.DbKindVisibility:
		return sqliteschema.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", mdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (mdb *db) VerifyVersion() error {
	return nil
	// TODO(jlegrone): implement this
	// expectedVersion := mdb.ExpectedVersion()
	// return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}
