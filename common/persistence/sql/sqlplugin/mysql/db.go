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
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	mysqlschema "go.temporal.io/server/schema/mysql"
)

// db represents a logical connection to mysql database
type db struct {
	dbKind sqlplugin.DbKind
	dbName string

	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
}

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// ErrDupEntryCode MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
// so we don't do the insert and return a ConditionalUpdate error.
const ErrDupEntryCode = 1062

func (mdb *db) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	return ok && sqlErr.Number == ErrDupEntryCode
}

// newDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	xdb *sqlx.DB,
	tx *sqlx.Tx,
) *db {
	mdb := &db{
		dbKind: dbKind,
		dbName: dbName,
		db:     xdb,
		tx:     tx,
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
	return newDB(mdb.dbKind, mdb.dbName, mdb.db, xtx), nil
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
	return mdb.db.Close()
}

// PluginName returns the name of the mysql plugin
func (mdb *db) PluginName() string {
	return PluginName
}

// ExpectedVersion returns expected version.
func (mdb *db) ExpectedVersion() string {
	switch mdb.dbKind {
	case sqlplugin.DbKindMain:
		return mysqlschema.Version
	case sqlplugin.DbKindVisibility:
		return mysqlschema.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", mdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (mdb *db) VerifyVersion() error {
	expectedVersion := mdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}
