// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/resolver"
	postgresqlschemaV12 "go.temporal.io/server/schema/postgresql/v12"
)

func (pdb *db) IsDupEntryError(err error) bool {
	return pdb.dbDriver.IsDupEntryError(err)
}

func (pdb *db) IsDupDatabaseError(err error) bool {
	return pdb.dbDriver.IsDupDatabaseError(err)
}

// db represents a logical connection to postgresql database
type db struct {
	dbKind   sqlplugin.DbKind
	dbName   string
	dbDriver driver.Driver

	plugin    *plugin
	cfg       *config.SQL
	resolver  resolver.ServiceResolver
	converter DataConverter

	handle *sqlplugin.DatabaseHandle
	tx     *sqlx.Tx
}

var _ sqlplugin.DB = (*db)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying postgresql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	dbDriver driver.Driver,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
) *db {
	mdb := &db{
		dbKind:   dbKind,
		dbName:   dbName,
		dbDriver: dbDriver,
		handle:   handle,
		tx:       tx,
	}
	mdb.converter = &converter{}
	return mdb
}

func (pdb *db) DB() *sqlx.DB {
	if pdb.tx != nil {
		panic("cannot use DB() in a transaction")
	}

	return pdb.handle.DB()
}

func (pdb *db) conn() sqlplugin.Conn {
	if pdb.tx != nil {
		return pdb.tx
	}
	return pdb.handle.Conn()
}

func (pdb *db) handleError(err error) {
	pdb.handle.HandleError(err)
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (pdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	tx, err := pdb.handle.DB().BeginTxx(ctx, nil)
	if err != nil {
		pdb.handle.HandleError(err)
		return nil, err
	}
	logr := log.NewTestLogger()
	logr.Error("Began transaction")
	return newDB(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.handle, tx), nil
}

// Close closes the connection to the mysql db
func (pdb *db) Close() error {
	pdb.handle.Close()
	return nil
}

// PluginName returns the name of the mysql plugin
func (pdb *db) PluginName() string {
	return PluginName
}

// DbName returns the name of the database
func (pdb *db) DbName() string {
	return pdb.dbName
}

// ExpectedVersion returns expected version.
func (pdb *db) ExpectedVersion() string {
	switch pdb.dbKind {
	case sqlplugin.DbKindMain:
		return postgresqlschemaV12.Version
	case sqlplugin.DbKindVisibility:
		return postgresqlschemaV12.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", pdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (pdb *db) VerifyVersion() error {
	expectedVersion := pdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(pdb, pdb.dbName, expectedVersion)
}

// Commit commits a previously started transaction
func (pdb *db) Commit() error {
	return pdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (pdb *db) Rollback() error {
	return pdb.tx.Rollback()
}

// Helper methods to hide common error handling

func (pdb *db) ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	res, err := pdb.conn().ExecContext(ctx, stmt, args...)
	if err != nil {
		pdb.handleError(err)
	}
	return res, err
}

func (pdb *db) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	err := pdb.conn().GetContext(ctx, dest, query, args...)
	if err != nil {
		pdb.handleError(err)
	}
	return err
}

func (pdb *db) Select(dest any, query string, args ...any) error {
	err := pdb.DB().Select(dest, query, args...)
	if err != nil {
		pdb.handleError(err)
	}
	return err
}

func (pdb *db) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	err := pdb.conn().SelectContext(ctx, dest, query, args...)
	if err != nil {
		pdb.handleError(err)
	}
	return err
}

func (pdb *db) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	res, err := pdb.conn().NamedExecContext(ctx, query, arg)
	if err != nil {
		pdb.handleError(err)
	}
	return res, err
}

func (pdb *db) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	stmt, err := pdb.conn().PrepareNamedContext(ctx, query)
	if err != nil {
		pdb.handleError(err)
	}
	return stmt, err
}

func (pdb *db) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := pdb.DB().QueryContext(ctx, query, args...)
	if err != nil {
		pdb.handleError(err)
	}
	return rows, err
}

func (pdb *db) Rebind(query string) string {
	return pdb.handle.DB().Rebind(query)
}
