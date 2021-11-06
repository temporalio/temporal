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
	"fmt"
	"time"

	"github.com/lib/pq"
)

const (
	readSchemaVersionQuery = `SELECT curr_version from schema_version where version_partition=0 and db_name=$1`

	writeSchemaVersionQuery = `INSERT into schema_version(version_partition, db_name, creation_time, curr_version, min_compatible_version) VALUES (0,$1,$2,$3,$4)
										ON CONFLICT (version_partition, db_name) DO UPDATE 
										  SET creation_time = excluded.creation_time,
										   	  curr_version = excluded.curr_version,
										      min_compatible_version = excluded.min_compatible_version;`

	writeSchemaUpdateHistoryQuery = `INSERT into schema_update_history(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(0,$1,$2,$3,$4,$5,$6,$7)`

	createSchemaVersionTableQuery = `CREATE TABLE IF NOT EXISTS schema_version(` +
		`version_partition INT not null, ` +
		`db_name VARCHAR(255) not null, ` +
		`creation_time TIMESTAMP, ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, db_name));`

	createSchemaUpdateHistoryTableQuery = `CREATE TABLE IF NOT EXISTS schema_update_history(` +
		`version_partition INT not null, ` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time TIMESTAMP not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, year, month, update_time));`

	// NOTE we have to use %v because somehow postgresql doesn't work with ? here
	// It's a small bug in sqlx library
	// TODO https://github.com/uber/cadence/issues/2893
	createDatabaseQuery = "CREATE DATABASE %v"

	dropDatabaseQuery = "DROP DATABASE IF EXISTS %v"

	listTablesQuery = "select table_name from information_schema.tables where table_schema='public'"

	dropTableQuery = "DROP TABLE %v"
)

// CreateSchemaVersionTables sets up the schema version tables
func (pdb *db) CreateSchemaVersionTables() error {
	if err := pdb.Exec(createSchemaVersionTableQuery); err != nil {
		return err
	}
	return pdb.Exec(createSchemaUpdateHistoryTableQuery)
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (pdb *db) ReadSchemaVersion(database string) (string, error) {
	var version string
	err := pdb.db.Get(&version, readSchemaVersionQuery, database)
	return version, err
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (pdb *db) UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error {
	return pdb.Exec(writeSchemaVersionQuery, database, time.Now().UTC(), newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (pdb *db) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	return pdb.Exec(writeSchemaUpdateHistoryQuery, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
}

// Exec executes a sql statement
func (pdb *db) Exec(stmt string, args ...interface{}) error {
	_, err := pdb.db.Exec(stmt, args...)
	return err
}

// ListTables returns a list of tables in this database
func (pdb *db) ListTables(database string) ([]string, error) {
	var tables []string
	err := pdb.db.Select(&tables, listTablesQuery)
	return tables, err
}

// DropTable drops a given table from the database
func (pdb *db) DropTable(name string) error {
	return pdb.Exec(fmt.Sprintf(dropTableQuery, name))
}

// DropAllTables drops all tables from this database
func (pdb *db) DropAllTables(database string) error {
	tables, err := pdb.ListTables(database)
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := pdb.DropTable(tab); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
func (pdb *db) CreateDatabase(name string) error {
	if err := pdb.Exec(fmt.Sprintf(createDatabaseQuery, name)); err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code.Name() == "duplicate_database" {
				return nil
			}
		}
		return err
	}

	return nil
}

// DropDatabase drops a database
func (pdb *db) DropDatabase(name string) error {
	return pdb.Exec(fmt.Sprintf(dropDatabaseQuery, name))
}
