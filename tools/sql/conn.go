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

package sql

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/tools/common/schema"
)

type (
	// Connection is the connection to database
	Connection struct {
		dbName  string
		adminDb sqlplugin.AdminDB
	}
)

var _ schema.DB = (*Connection)(nil)

// NewConnection creates a new connection to database
func NewConnection(cfg *config.SQL) (*Connection, error) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		return nil, err
	}

	return &Connection{
		adminDb: db,
		dbName:  cfg.DatabaseName,
	}, nil
}

// CreateSchemaVersionTables sets up the schema version tables
func (c *Connection) CreateSchemaVersionTables() error {
	return c.adminDb.CreateSchemaVersionTables()
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (c *Connection) ReadSchemaVersion() (string, error) {
	return c.adminDb.ReadSchemaVersion(c.dbName)
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (c *Connection) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	return c.adminDb.UpdateSchemaVersion(c.dbName, newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (c *Connection) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	return c.adminDb.WriteSchemaUpdateLog(oldVersion, newVersion, manifestMD5, desc)
}

// Exec executes a sql statement
func (c *Connection) Exec(stmt string, args ...interface{}) error {
	err := c.adminDb.Exec(stmt, args...)
	return err
}

// ListTables returns a list of tables in this database
func (c *Connection) ListTables() ([]string, error) {
	return c.adminDb.ListTables(c.dbName)
}

// DropTable drops a given table from the database
func (c *Connection) DropTable(name string) error {
	return c.adminDb.DropTable(name)
}

// DropAllTables drops all tables from this database
func (c *Connection) DropAllTables() error {
	tables, err := c.ListTables()
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := c.DropTable(tab); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
func (c *Connection) CreateDatabase(name string) error {
	return c.adminDb.CreateDatabase(name)
}

// DropDatabase drops a database
func (c *Connection) DropDatabase(name string) error {
	return c.adminDb.DropDatabase(name)
}

// Close closes the sql client
func (c *Connection) Close() {
	if c.adminDb != nil {
		err := c.adminDb.Close()
		if err != nil {
			panic("cannot close connection")
		}
	}
}
