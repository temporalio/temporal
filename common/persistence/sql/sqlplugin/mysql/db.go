// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

// db represents a logical connection to mysql database
type db struct {
	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
}

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// ErrDupEntry MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
// so we don't do the insert and return a ConditionalUpdate error.
const ErrDupEntry = 1062

func (mdb *db) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	return ok && sqlErr.Number == ErrDupEntry
}

// newDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
func newDB(xdb *sqlx.DB, tx *sqlx.Tx) *db {
	mdb := &db{db: xdb, tx: tx}
	mdb.conn = xdb
	if tx != nil {
		mdb.conn = tx
	}
	mdb.converter = &converter{}
	return mdb
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *db) BeginTx() (sqlplugin.Tx, error) {
	xtx, err := mdb.db.Beginx()
	if err != nil {
		return nil, err
	}
	return newDB(mdb.db, xtx), nil
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
