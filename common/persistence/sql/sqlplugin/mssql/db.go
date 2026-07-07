package mssql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	mssqlschemaV2019 "go.temporal.io/server/schema/mssql/v2019"
)

func (mdb *db) IsDupEntryError(err error) bool {
	return isDupEntryError(err)
}

func (mdb *db) IsDupDatabaseError(err error) bool {
	return isDupDatabaseError(err)
}

// db represents a logical connection to a SQL Server database
type db struct {
	dbKind sqlplugin.DbKind
	dbName string

	converter DataConverter
	logger    log.Logger

	handle *sqlplugin.DatabaseHandle
	tx     *sqlx.Tx
}

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying SQL Server database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
	logger log.Logger,
) *db {
	mdb := &db{
		dbKind: dbKind,
		dbName: dbName,
		handle: handle,
		tx:     tx,
		logger: logger,
	}
	mdb.converter = &converter{}
	return mdb
}

func (mdb *db) conn() sqlplugin.Conn {
	if mdb.tx != nil {
		return mdb.tx
	}
	return mdb.handle.Conn()
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	db, err := mdb.handle.DB()
	if err != nil {
		// This error needs no conversion
		return nil, err
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, mdb.handle.ConvertError(err)
	}
	return newDB(mdb.dbKind, mdb.dbName, mdb.handle, tx, mdb.logger), nil
}

// Close closes the connection to the mssql db
func (mdb *db) Close() error {
	mdb.handle.Close()
	return nil
}

// PluginName returns the name of the mssql plugin
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
		return mssqlschemaV2019.Version
	case sqlplugin.DbKindVisibility:
		return mssqlschemaV2019.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", mdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (mdb *db) VerifyVersion() error {
	expectedVersion := mdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}

// Commit commits a previously started transaction
func (mdb *db) Commit() error {
	return mdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (mdb *db) Rollback() error {
	return mdb.tx.Rollback()
}

// Query helpers below rebind before executing: all static queries in this
// plugin are written with `?` placeholders and the sqlserver driver only
// accepts @pN parameters. Rebind is idempotent — an already-rebound
// statement contains no `?` and passes through unchanged — so call sites
// that rebind explicitly (sqlx.In expansion, visibility query converter
// output) are safe. Named (`:name`) queries are compiled by sqlx itself and
// bypass these helpers' rebinding.

func (mdb *db) ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	conn := mdb.conn()
	res, err := conn.ExecContext(ctx, conn.Rebind(stmt), args...)
	return res, mdb.handle.ConvertError(err)
}

func (mdb *db) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	conn := mdb.conn()
	err := conn.GetContext(ctx, dest, conn.Rebind(query), args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) Select(dest any, query string, args ...any) error {
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	err = db.Select(dest, db.Rebind(query), args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	conn := mdb.conn()
	err := conn.SelectContext(ctx, dest, conn.Rebind(query), args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	res, err := mdb.conn().NamedExecContext(ctx, query, arg)
	return res, mdb.handle.ConvertError(err)
}

func (mdb *db) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	stmt, err := mdb.conn().PrepareNamedContext(ctx, query)
	return stmt, mdb.handle.ConvertError(err)
}

func (mdb *db) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, db.Rebind(query), args...)
	return rows, mdb.handle.ConvertError(err)
}

func (mdb *db) Rebind(query string) string {
	return mdb.conn().Rebind(query)
}
