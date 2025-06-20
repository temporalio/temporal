package schema

import (
	"fmt"
)

type (
	mockSQLDB struct {
	}
)

// Exec executes a cql statement
func (db *mockSQLDB) Exec(stmt string, args ...interface{}) error {
	return fmt.Errorf("unimplemented")
}

// DropAllTables drops all tables
func (db *mockSQLDB) DropAllTables() error {
	return fmt.Errorf("unimplemented")
}

// CreateSchemaVersionTables sets up the schema version tables
func (db *mockSQLDB) CreateSchemaVersionTables() error {
	return fmt.Errorf("unimplemented")
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (db *mockSQLDB) ReadSchemaVersion() (string, error) {
	return "", fmt.Errorf("unimplemented")
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (db *mockSQLDB) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	return fmt.Errorf("unimplemented")
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (db *mockSQLDB) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	return fmt.Errorf("unimplemented")
}

// Close gracefully closes the client object
func (db *mockSQLDB) Close() {}

// Type gives the type of db (e.g. "cassandra", "sql")
func (db *mockSQLDB) Type() string {
	return "sql"
}
