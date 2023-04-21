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

	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	mysqlschemaV8 "go.temporal.io/server/schema/mysql/v8"
)

// db represents a logical connection to mysql database
type dbV8 struct {
	db
}

var _ sqlplugin.AdminDB = (*dbV8)(nil)
var _ sqlplugin.DB = (*dbV8)(nil)
var _ sqlplugin.Tx = (*dbV8)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
func newDBV8(
	dbKind sqlplugin.DbKind,
	dbName string,
	xdb *sqlx.DB,
	tx *sqlx.Tx,
) *dbV8 {
	mdb := &dbV8{
		db: db{
			dbKind: dbKind,
			dbName: dbName,
			db:     xdb,
			tx:     tx,
		},
	}
	mdb.conn = xdb
	if tx != nil {
		mdb.conn = tx
	}
	mdb.converter = &converter{}
	return mdb
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *dbV8) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	xtx, err := mdb.db.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return newDBV8(mdb.dbKind, mdb.dbName, mdb.db.db, xtx), nil
}

// PluginName returns the name of the mysql plugin
func (mdb *dbV8) PluginName() string {
	return PluginNameV8
}

// ExpectedVersion returns expected version.
func (mdb *dbV8) ExpectedVersion() string {
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
func (mdb *dbV8) VerifyVersion() error {
	expectedVersion := mdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}
