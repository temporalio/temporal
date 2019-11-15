// Copyright (c) 2019 Uber Technologies, Inc.
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

package mysql

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql" // needed to load the mysql driver

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"github.com/uber/cadence/tools/sql"
)

const (
	// DriverName refers to the name of the mysql driver
	driverName = "mysql"

	dataSourceNameMySQL = "%s:%s@%v(%v:%v)/%s?multiStatements=true&parseTime=true&clientFoundRows=true"

	readSchemaVersionMySQL = `SELECT curr_version from schema_version where db_name=?`

	writeSchemaVersionMySQL = `REPLACE into schema_version(db_name, creation_time, curr_version, min_compatible_version) VALUES (?,?,?,?)`

	writeSchemaUpdateHistoryMySQL = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(?,?,?,?,?,?,?)`

	createSchemaVersionTableMySQL = `CREATE TABLE schema_version(db_name VARCHAR(255) not null PRIMARY KEY, ` +
		`creation_time DATETIME(6), ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64));`

	createSchemaUpdateHistoryTableMySQL = `CREATE TABLE schema_update_history(` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time DATETIME(6) not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (year, month, update_time));`

	//NOTE we have to use %v because somehow mysql doesn't work with ? here
	createDatabaseMySQL = "CREATE database %v CHARACTER SET UTF8"

	dropDatabaseMySQL = "Drop database %v"

	listTablesMySQL = "SHOW TABLES FROM %v"

	dropTableMySQL = "DROP TABLE %v"
)

type driver struct{}

var _ sql.Driver = (*driver)(nil)

func init() {
	sql.RegisterDriver(driverName, &driver{})
}

func (d *driver) GetDriverName() string {
	return driverName
}

func (d *driver) CreateDBConnection(driverName, host string, port int, user string, passwd string, database string) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driverName, fmt.Sprintf(dataSourceNameMySQL, user, passwd, "tcp", host, port, database))

	if err != nil {
		return nil, err
	}
	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func (d *driver) GetReadSchemaVersionSQL() string {
	return readSchemaVersionMySQL
}

func (d *driver) GetWriteSchemaVersionSQL() string {
	return writeSchemaVersionMySQL
}

func (d *driver) GetWriteSchemaUpdateHistorySQL() string {
	return writeSchemaUpdateHistoryMySQL
}

func (d *driver) GetCreateSchemaVersionTableSQL() string {
	return createSchemaVersionTableMySQL
}

func (d *driver) GetCreateSchemaUpdateHistoryTableSQL() string {
	return createSchemaUpdateHistoryTableMySQL
}

func (d *driver) GetCreateDatabaseSQL() string {
	return createDatabaseMySQL
}

func (d *driver) GetDropDatabaseSQL() string {
	return dropDatabaseMySQL
}

func (d *driver) GetListTablesSQL() string {
	return listTablesMySQL
}

func (d *driver) GetDropTableSQL() string {
	return dropTableMySQL
}
