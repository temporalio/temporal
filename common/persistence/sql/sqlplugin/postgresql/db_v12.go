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
	postgresqlschemaV12 "go.temporal.io/server/schema/postgresql/v12"
)

// dbV12 represents a logical connection to mysql database
type dbV12 struct {
	db
}

var _ sqlplugin.DB = (*dbV12)(nil)
var _ sqlplugin.Tx = (*dbV12)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying postgresql database
func newDBV12(
	dbKind sqlplugin.DbKind,
	dbName string,
	xdb *sqlx.DB,
	tx *sqlx.Tx,
) *dbV12 {
	mdb := &dbV12{
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
func (pdb *dbV12) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	xtx, err := pdb.db.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return newDB(pdb.dbKind, pdb.dbName, pdb.db.db, xtx), nil
}

// PluginName returns the name of the mysql plugin
func (pdb *dbV12) PluginName() string {
	return PluginNameV12
}

// ExpectedVersion returns expected version.
func (pdb *dbV12) ExpectedVersion() string {
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
func (pdb *dbV12) VerifyVersion() error {
	expectedVersion := pdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(pdb, pdb.dbName, expectedVersion)
}
