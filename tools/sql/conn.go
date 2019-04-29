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

package sql

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/tools/common/schema"
	"github.com/uber/cadence/tools/sql/mysql"
)

type (
	sqlConnectParams struct {
		host       string
		port       int
		user       string
		password   string
		database   string
		driverName string
	}
	sqlConn struct {
		database string
		db       *sqlx.DB
	}
)

const (
	defaultSQLPort              = 3306
	readSchemaVersionSQL        = `SELECT curr_version from schema_version where db_name=?`
	writeSchemaVersionSQL       = `REPLACE into schema_version(db_name, creation_time, curr_version, min_compatible_version) VALUES (?,?,?,?)`
	writeSchemaUpdateHistorySQL = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(?,?,?,?,?,?,?)`

	createSchemaVersionTableSQL = `CREATE TABLE schema_version(db_name VARCHAR(255) not null PRIMARY KEY, ` +
		`creation_time DATETIME(6), ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64));`

	createSchemaUpdateHistoryTableSQL = `CREATE TABLE schema_update_history(` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time DATETIME(6) not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (year, month, update_time));`
)

var _ schema.DB = (*sqlConn)(nil)

func newConn(params *sqlConnectParams) (*sqlConn, error) {
	if params.driverName != mysql.DriverName {
		return nil, fmt.Errorf("unsupported database driver: %v", params.driverName)
	}
	db, err := mysql.NewConnection(params.host, params.port, params.user, params.password, params.database)
	if err != nil {
		return nil, err
	}
	return &sqlConn{db: db, database: params.database}, nil
}

// CreateSchemaVersionTables sets up the schema version tables
func (c *sqlConn) CreateSchemaVersionTables() error {
	if err := c.Exec(createSchemaVersionTableSQL); err != nil {
		return err
	}
	return c.Exec(createSchemaUpdateHistoryTableSQL)
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (c *sqlConn) ReadSchemaVersion() (string, error) {
	var version string
	err := c.db.Get(&version, readSchemaVersionSQL, c.database)
	return version, err
}

// UpdateShemaVersion updates the schema version for the keyspace
func (c *sqlConn) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	_, err := c.db.Exec(writeSchemaVersionSQL, c.database, time.Now(), newVersion, minCompatibleVersion)
	return err
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (c *sqlConn) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	_, err := c.db.Exec(writeSchemaUpdateHistorySQL, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
	return err
}

// Exec executes a sql statement
func (c *sqlConn) Exec(stmt string) error {
	_, err := c.db.Exec(stmt)
	return err
}

// ListTables returns a list of tables in this database
func (c *sqlConn) ListTables() ([]string, error) {
	var tables []string
	err := c.db.Select(&tables, fmt.Sprintf("SHOW TABLES FROM %v", c.database))
	return tables, err
}

// DropTable drops a given table from the database
func (c *sqlConn) DropTable(name string) error {
	return c.Exec(fmt.Sprintf("DROP TABLE %v", name))
}

// DropAllTables drops all tables from this database
func (c *sqlConn) DropAllTables() error {
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
func (c *sqlConn) CreateDatabase(name string) error {
	return c.Exec(fmt.Sprintf("CREATE database %v CHARACTER SET UTF8", name))
}

// DropDatabase drops a database
func (c *sqlConn) DropDatabase(name string) error {
	return c.Exec(fmt.Sprintf("DROP database %v", name))
}

// Close closes the sql client
func (c *sqlConn) Close() {
	if c.db != nil {
		c.db.Close()
	}
}
