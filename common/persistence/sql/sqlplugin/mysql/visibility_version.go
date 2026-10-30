package mysql

import (
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var _ sqlplugin.VisibilityVersionDB = (*db)(nil)

const (
	readVisibilityVersionQuery  = `SELECT curr_version FROM visibility_version WHERE version_partition=0 AND db_name=?`
	writeVisibilityVersionQuery = `INSERT INTO visibility_version(version_partition, db_name, creation_time, curr_version, min_compatible_version) ` +
		`VALUES (0,?,?,?,?) ON DUPLICATE KEY UPDATE ` +
		`creation_time=VALUES(creation_time), curr_version=VALUES(curr_version), min_compatible_version=VALUES(min_compatible_version)`
	writeVisibilityUpdateHistoryQuery = `INSERT INTO visibility_update_history(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(0,?,?,?,?,?,?,?)`
	createVisibilityVersionTableQuery = `CREATE TABLE IF NOT EXISTS visibility_version (` +
		`version_partition INT NOT NULL, db_name VARCHAR(255) NOT NULL, creation_time DATETIME(6), ` +
		`curr_version VARCHAR(64), min_compatible_version VARCHAR(64), PRIMARY KEY (version_partition, db_name))`
	createVisibilityUpdateHistoryTableQuery = `CREATE TABLE IF NOT EXISTS visibility_update_history (` +
		`version_partition INT NOT NULL, year INT NOT NULL, month INT NOT NULL, update_time DATETIME(6) NOT NULL, ` +
		`description VARCHAR(255), manifest_md5 VARCHAR(64), new_version VARCHAR(64), old_version VARCHAR(64), ` +
		`PRIMARY KEY (version_partition, year, month, update_time))`
)

func (mdb *db) CreateVisibilityVersionTables() error {
	if err := mdb.Exec(createVisibilityVersionTableQuery); err != nil {
		return err
	}
	return mdb.Exec(createVisibilityUpdateHistoryTableQuery)
}

func (mdb *db) ReadVisibilityVersion(database string) (string, error) {
	connection, err := mdb.handle.DB()
	if err != nil {
		return "", err
	}
	var version string
	err = connection.Get(&version, readVisibilityVersionQuery, database)
	return version, mdb.handle.ConvertError(err)
}

func (mdb *db) UpdateVisibilityVersion(database, newVersion, minCompatibleVersion string) error {
	return mdb.Exec(writeVisibilityVersionQuery, database, time.Now().UTC(), newVersion, minCompatibleVersion)
}

func (mdb *db) WriteVisibilityUpdateLog(oldVersion, newVersion, manifestMD5, desc string) error {
	now := time.Now().UTC()
	return mdb.Exec(writeVisibilityUpdateHistoryQuery, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
}

type visibilityVersionReader struct {
	*db
}

func (reader visibilityVersionReader) ReadSchemaVersion(database string) (string, error) {
	version, err := reader.ReadVisibilityVersion(database)
	// Existing deployments keep visibility metadata in schema_version until migrated.
	if sqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok && sqlErr.Number == 1146 {
		return reader.db.ReadSchemaVersion(database)
	}
	return version, err
}
