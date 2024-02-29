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
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
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

	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
}

var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying postgresql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	dbDriver driver.Driver,
	xdb *sqlx.DB,
	tx *sqlx.Tx,
) *db {
	mdb := &db{
		dbKind:   dbKind,
		dbName:   dbName,
		dbDriver: dbDriver,
		db:       xdb,
		tx:       tx,
	}
	mdb.conn = xdb
	if tx != nil {
		mdb.conn = tx
	}
	mdb.converter = &converter{}
	return mdb
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (pdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	xtx, err := pdb.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return newDB(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.db, xtx), nil
}

// Commit commits a previously started transaction
func (pdb *db) Commit() error {
	return pdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (pdb *db) Rollback() error {
	return pdb.tx.Rollback()
}

// Close closes the connection to the mysql db
func (pdb *db) Close() error {
	return pdb.db.Close()
}

// PluginName returns the name of the mysql plugin
func (pdb *db) PluginName() string {
	return PluginNameV12
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
