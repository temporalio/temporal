package postgres

import (
	"fmt"
	"time"
)

const (
	readSchemaVersionQuery = `SELECT curr_version from schema_version where db_name=$1`

	writeSchemaVersionQuery = `INSERT into schema_version(db_name, creation_time, curr_version, min_compatible_version) VALUES ($1,$2,$3,$4)
										ON CONFLICT (db_name) DO UPDATE 
										  SET creation_time = excluded.creation_time,
										   	  curr_version = excluded.curr_version,
										      min_compatible_version = excluded.min_compatible_version;`

	writeSchemaUpdateHistoryQuery = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES($1,$2,$3,$4,$5,$6,$7)`

	createSchemaVersionTableQuery = `CREATE TABLE schema_version(db_name VARCHAR(255) not null PRIMARY KEY, ` +
		`creation_time TIMESTAMP, ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64));`

	createSchemaUpdateHistoryTableQuery = `CREATE TABLE schema_update_history(` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time TIMESTAMP not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (year, month, update_time));`

	// NOTE we have to use %v because somehow postgres doesn't work with ? here
	// It's a small bug in sqlx library
	// TODO https://github.com/uber/cadence/issues/2893
	createDatabaseQuery = "CREATE database %v"

	dropDatabaseQuery = "Drop database %v"

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
	return pdb.Exec(writeSchemaVersionQuery, database, time.Now(), newVersion, minCompatibleVersion)
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
	return pdb.Exec(fmt.Sprintf(createDatabaseQuery, name))
}

// DropDatabase drops a database
func (pdb *db) DropDatabase(name string) error {
	return pdb.Exec(fmt.Sprintf(dropDatabaseQuery, name))
}
