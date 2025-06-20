package mysql

import (
	"fmt"
	"time"
)

const (
	readSchemaVersionQuery = `SELECT curr_version from schema_version where version_partition=0 and db_name=?`

	writeSchemaVersionQuery = `INSERT into schema_version(version_partition, db_name, creation_time, curr_version, min_compatible_version) ` +
		`VALUES (0,?,?,?,?) ` +
		`ON DUPLICATE KEY UPDATE ` +
		`creation_time=VALUES(creation_time), curr_version=VALUES(curr_version), min_compatible_version=VALUES(min_compatible_version)`

	writeSchemaUpdateHistoryQuery = `INSERT into schema_update_history(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(0,?,?,?,?,?,?,?)`

	createSchemaVersionTableQuery = `CREATE TABLE IF NOT EXISTS schema_version(version_partition INT not null, ` +
		`db_name VARCHAR(255) not null, ` +
		`creation_time DATETIME(6), ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, db_name));`

	createSchemaUpdateHistoryTableQuery = `CREATE TABLE IF NOT EXISTS schema_update_history(` +
		`version_partition INT not null, ` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time DATETIME(6) not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, year, month, update_time));`

	// NOTE: we have to use %v because somehow mysql doesn't work with ? here
	createDatabaseQuery = "CREATE DATABASE IF NOT EXISTS %v CHARACTER SET utf8mb4"

	dropDatabaseQuery = "DROP DATABASE IF EXISTS %v"

	listTablesQuery = "SHOW TABLES FROM %v"

	dropTableQuery = "DROP TABLE %v"
)

// CreateSchemaVersionTables sets up the schema version tables
func (mdb *db) CreateSchemaVersionTables() error {
	if err := mdb.Exec(createSchemaVersionTableQuery); err != nil {
		return err
	}
	return mdb.Exec(createSchemaUpdateHistoryTableQuery)
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (mdb *db) ReadSchemaVersion(database string) (string, error) {
	var version string
	db, err := mdb.handle.DB()
	if err != nil {
		return "", err
	}
	err = db.Get(&version, readSchemaVersionQuery, database)
	return version, mdb.handle.ConvertError(err)
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (mdb *db) UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error {
	return mdb.Exec(writeSchemaVersionQuery, database, time.Now().UTC(), newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (mdb *db) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	return mdb.Exec(writeSchemaUpdateHistoryQuery, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
}

// Exec executes a sql statement
func (mdb *db) Exec(stmt string, args ...interface{}) error {
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	_, err = db.Exec(stmt, args...)
	return mdb.handle.ConvertError(err)
}

// ListTables returns a list of tables in this database
func (mdb *db) ListTables(database string) ([]string, error) {
	var tables []string
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	err = db.Select(&tables, fmt.Sprintf(listTablesQuery, database))
	return tables, mdb.handle.ConvertError(err)
}

// DropTable drops a given table from the database
func (mdb *db) DropTable(name string) error {
	return mdb.Exec(fmt.Sprintf(dropTableQuery, name))
}

// DropAllTables drops all tables from this database
func (mdb *db) DropAllTables(database string) error {
	tables, err := mdb.ListTables(database)
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := mdb.DropTable(tab); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
func (mdb *db) CreateDatabase(name string) error {
	return mdb.Exec(fmt.Sprintf(createDatabaseQuery, name))
}

// DropDatabase drops a database
func (mdb *db) DropDatabase(name string) error {
	return mdb.Exec(fmt.Sprintf(dropDatabaseQuery, name))
}
