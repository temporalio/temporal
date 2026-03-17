package libsql

import (
	"fmt"
	"strings"
	"time"
)

const (
	readSchemaVersionQuery = `SELECT curr_version from schema_version where version_partition=0 and db_name=?`

	writeSchemaVersionQuery = `REPLACE into schema_version(version_partition, db_name, creation_time, curr_version, min_compatible_version) VALUES (0,?,?,?,?)`

	writeSchemaUpdateHistoryQuery = `INSERT into schema_update_history(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(0,?,?,?,?,?,?,?)`

	createSchemaVersionTableQuery = `CREATE TABLE schema_version(version_partition INT not null, ` +
		`db_name VARCHAR(255) not null, ` +
		`creation_time DATETIME(6), ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, db_name));`

	createSchemaUpdateHistoryTableQuery = `CREATE TABLE schema_update_history(` +
		`version_partition INT not null, ` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time DATETIME(6) not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, year, month, update_time));`

	listTablesQuery = "SELECT name FROM sqlite_master WHERE type='table'"

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
	err := mdb.db.Get(&version, readSchemaVersionQuery, database)
	return version, err
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

// Exec executes a sql statement.
// libsql is compiled with DQS=0, so double-quoted strings in DDL are treated as
// identifiers rather than string literals. Temporal's upstream schema uses
// double-quoted JSON paths (e.g. "$.Foo") which need to be single-quoted.
func (mdb *db) Exec(stmt string, args ...interface{}) error {
	_, err := mdb.db.Exec(normalizeDQS(stmt), args...)
	return err
}

// normalizeDQS rewrites double-quoted strings to single-quoted in DDL
// statements. Only touches CREATE/ALTER statements to avoid mangling DML.
func normalizeDQS(stmt string) string {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "CREATE ") && !strings.HasPrefix(upper, "ALTER ") {
		return stmt
	}

	var b strings.Builder
	b.Grow(len(stmt))
	for i := 0; i < len(stmt); i++ {
		ch := stmt[i]
		if ch == '\'' {
			// skip single-quoted string literal
			b.WriteByte(ch)
			i++
			for i < len(stmt) {
				b.WriteByte(stmt[i])
				if stmt[i] == '\'' {
					if i+1 < len(stmt) && stmt[i+1] == '\'' {
						b.WriteByte(stmt[i+1])
						i++
					} else {
						break
					}
				}
				i++
			}
		} else if ch == '"' {
			// rewrite to single quote
			b.WriteByte('\'')
			i++
			for i < len(stmt) {
				if stmt[i] == '"' {
					if i+1 < len(stmt) && stmt[i+1] == '"' {
						b.WriteByte('\'')
						b.WriteByte('\'')
						i += 2
					} else {
						b.WriteByte('\'')
						break
					}
				} else if stmt[i] == '\'' {
					// escape embedded single quotes
					b.WriteByte('\'')
					b.WriteByte('\'')
					i++
				} else {
					b.WriteByte(stmt[i])
					i++
				}
			}
		} else {
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// ListTables returns a list of tables in this database
func (mdb *db) ListTables(database string) ([]string, error) {
	var tables []string
	err := mdb.db.Select(&tables, listTablesQuery)
	return tables, err
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
	// Embedded (file/memory) DB does not use separate create.
	return nil
}

// DropDatabase drops a database
func (mdb *db) DropDatabase(name string) error {
	// Embedded DB does not use separate drop.
	return nil
}
