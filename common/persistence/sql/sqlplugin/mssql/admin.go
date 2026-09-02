package mssql

import (
	"fmt"
	"time"
)

const (
	readSchemaVersionQuery = `SELECT curr_version FROM schema_version WHERE version_partition=0 AND db_name=?`

	writeSchemaVersionQuery = `MERGE schema_version WITH (HOLDLOCK) AS target
 USING (SELECT 0 AS version_partition, ? AS db_name, ? AS creation_time, ? AS curr_version, ? AS min_compatible_version) AS source
 ON target.version_partition = source.version_partition AND target.db_name = source.db_name
 WHEN MATCHED THEN UPDATE SET
   creation_time = source.creation_time,
   curr_version = source.curr_version,
   min_compatible_version = source.min_compatible_version
 WHEN NOT MATCHED THEN INSERT (version_partition, db_name, creation_time, curr_version, min_compatible_version)
   VALUES (source.version_partition, source.db_name, source.creation_time, source.curr_version, source.min_compatible_version);`

	writeSchemaUpdateHistoryQuery = `INSERT INTO schema_update_history(version_partition, [year], [month], update_time, old_version, new_version, manifest_md5, description) VALUES(0,?,?,?,?,?,?,?)`

	createSchemaVersionTableQuery = `IF OBJECT_ID(N'schema_version', N'U') IS NULL CREATE TABLE schema_version(` +
		`version_partition INT NOT NULL, ` +
		`db_name VARCHAR(255) NOT NULL, ` +
		`creation_time DATETIME2(6), ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, db_name));`

	createSchemaUpdateHistoryTableQuery = `IF OBJECT_ID(N'schema_update_history', N'U') IS NULL CREATE TABLE schema_update_history(` +
		`version_partition INT NOT NULL, ` +
		`[year] INT NOT NULL, ` +
		`[month] INT NOT NULL, ` +
		`update_time DATETIME2(6) NOT NULL, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, [year], [month], update_time));`

	// Statements below are executed one per batch: Azure SQL Database requires
	// CREATE DATABASE and DROP DATABASE to be the ONLY statement in a
	// Transact-SQL batch, so the create/RCSI/drop steps cannot be combined
	// into a single guarded block the way boxed SQL Server would allow.
	databaseExistsQuery = `SELECT COUNT(*) FROM sys.databases WHERE name = ?`

	// The UTF-8 CI collation mirrors MySQL's utf8mb4 defaults and must match
	// schema/mssql/v2019's database.sql files.
	createDatabaseQuery = `CREATE DATABASE [%v] COLLATE Latin1_General_100_CI_AS_SC_UTF8`

	// READ_COMMITTED_SNAPSHOT gives MVCC-style non-blocking reads under the
	// default READ COMMITTED isolation, matching the behavior Temporal's
	// persistence layer expects from MySQL/PostgreSQL. Safe to run while the
	// database is freshly created (no other sessions). Azure SQL Database has
	// RCSI ON by default; there this statement is a no-op re-assertion.
	enableRCSIQuery = `ALTER DATABASE [%v] SET READ_COMMITTED_SNAPSHOT ON`

	// Kicks other sessions off the database before dropping it. Not supported
	// on Azure SQL Database (only RESTRICTED_USER/MULTI_USER are), so its
	// failure is tolerated in DropDatabase.
	setSingleUserQuery = `ALTER DATABASE [%v] SET SINGLE_USER WITH ROLLBACK IMMEDIATE`

	dropDatabaseQuery = `DROP DATABASE [%v]`

	listTablesQuery = `SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'`

	dropTableQuery = `DROP TABLE %v`
)

// Exec executes a sql statement
func (mdb *db) Exec(stmt string, args ...any) error {
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	_, err = db.Exec(db.Rebind(stmt), args...)
	return mdb.handle.ConvertError(err)
}

// CreateSchemaVersionTables sets up the schema version tables
func (mdb *db) CreateSchemaVersionTables() error {
	if err := mdb.Exec(createSchemaVersionTableQuery); err != nil {
		return err
	}
	return mdb.Exec(createSchemaUpdateHistoryTableQuery)
}

// ReadSchemaVersion returns the current schema version for the database
func (mdb *db) ReadSchemaVersion(database string) (string, error) {
	var version string
	db, err := mdb.handle.DB()
	if err != nil {
		return "", err
	}
	err = db.Get(&version, db.Rebind(readSchemaVersionQuery), database)
	return version, mdb.handle.ConvertError(err)
}

// UpdateSchemaVersion updates the schema version for the database
func (mdb *db) UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error {
	return mdb.Exec(writeSchemaVersionQuery, database, time.Now().UTC(), newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (mdb *db) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	return mdb.Exec(writeSchemaUpdateHistoryQuery, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
}

// ListTables returns a list of tables in this database
func (mdb *db) ListTables(database string) ([]string, error) {
	var tables []string
	err := mdb.Select(&tables, listTablesQuery)
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

// CreateDatabase creates a database if it doesn't exist and enables
// READ_COMMITTED_SNAPSHOT on it. Each statement runs in its own batch for
// Azure SQL Database compatibility.
func (mdb *db) CreateDatabase(name string) error {
	var count int
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	if err := db.Get(&count, db.Rebind(databaseExistsQuery), name); err != nil {
		return mdb.handle.ConvertError(err)
	}
	if count > 0 {
		return nil
	}
	if err := mdb.Exec(fmt.Sprintf(createDatabaseQuery, name)); err != nil {
		if mdb.IsDupDatabaseError(err) {
			return nil
		}
		return err
	}
	return mdb.Exec(fmt.Sprintf(enableRCSIQuery, name))
}

// DropDatabase drops a database. The SINGLE_USER step kicks other sessions
// off first; it is not supported on Azure SQL Database, where its failure is
// ignored and the DROP proceeds (Azure closes existing connections itself).
func (mdb *db) DropDatabase(name string) error {
	var count int
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	if err := db.Get(&count, db.Rebind(databaseExistsQuery), name); err != nil {
		return mdb.handle.ConvertError(err)
	}
	if count == 0 {
		return nil
	}
	_ = mdb.Exec(fmt.Sprintf(setSingleUserQuery, name))
	return mdb.Exec(fmt.Sprintf(dropDatabaseQuery, name))
}
