// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package sqlite

import (
	"context"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

// db represents a logical connection to sqlite database
type db struct {
	dbKind sqlplugin.DbKind
	dbName string

	mu      sync.RWMutex
	onClose []func()

	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
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
) *db {
	mdb := &db{
		dbKind:  dbKind,
		dbName:  dbName,
		onClose: make([]func(), 0),
		db:      xdb,
		tx:      tx,
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

func (mdb *db) OnClose(hook func()) {
	mdb.mu.Lock()
	mdb.onClose = append(mdb.onClose, hook)
	mdb.mu.Unlock()
}

// Close closes the connection to the sqlite db
func (mdb *db) Close() error {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()

	for _, hook := range mdb.onClose {
		// de-registers the database from conn pool
		hook()
	}

	// database connection will be automatically closed by the hook handler when all references are removed
	return nil
}

// PluginName returns the name of the plugin
func (mdb *db) PluginName() string {
	return PluginName
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
