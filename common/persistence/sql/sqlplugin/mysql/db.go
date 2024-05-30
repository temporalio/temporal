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

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	mysqlschemaV8 "go.temporal.io/server/schema/mysql/v8"
)

// MySQL error codes
const (
	// ErrDupEntryCode MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
	// so we don't do the insert and return a ConditionalUpdate error.
	ErrDupEntryCode = 1062

	// Cannot execute statement in a READ ONLY transaction.
	readOnlyTransactionCode = 1792
	// Too many connections open
	tooManyConnectionsCode = 1040
	// Running in read-only mode
	readOnlyModeCode = 1836
)

// db represents a logical connection to mysql database
type db struct {
	dbKind sqlplugin.DbKind
	dbName string

	handle    *sqlplugin.DatabaseHandle
	tx        *sqlx.Tx
	converter DataConverter
}

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

func isConnNeedsRefreshError(err error) bool {
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	return myErr.Number == readOnlyModeCode || myErr.Number == readOnlyTransactionCode || myErr.Number == tooManyConnectionsCode
}

func (mdb *db) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	return ok && sqlErr.Number == ErrDupEntryCode
}

// newDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
) *db {
	mdb := &db{
		dbKind: dbKind,
		dbName: dbName,
		handle: handle,
		tx:     tx,
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
		return nil, err
	}
	xtx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, mdb.handle.ConvertError(err)
	}
	return newDB(mdb.dbKind, mdb.dbName, mdb.handle, xtx), nil
}

// Commit commits a previously started transaction
func (mdb *db) Commit() error {
	return mdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (mdb *db) Rollback() error {
	return mdb.tx.Rollback()
}

// Close closes the connection to the mysql db
func (mdb *db) Close() error {
	mdb.handle.Close()
	return nil
}

// PluginName returns the name of the mysql plugin
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
		return mysqlschemaV8.Version
	case sqlplugin.DbKindVisibility:
		return mysqlschemaV8.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", mdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (mdb *db) VerifyVersion() error {
	expectedVersion := mdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}

// Helper methods to hide common error handling
func (mdb *db) ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	res, err := mdb.conn().ExecContext(ctx, stmt, args...)
	return res, mdb.handle.ConvertError(err)
}

func (mdb *db) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	err := mdb.conn().GetContext(ctx, dest, query, args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	err := mdb.conn().SelectContext(ctx, dest, query, args...)
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

func (mdb *db) Rebind(query string) string {
	return mdb.conn().Rebind(query)
}
