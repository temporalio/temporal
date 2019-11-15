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

package postgres

import (
	"fmt"

	_ "github.com/lib/pq" // needed to load the postgres driver

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"github.com/uber/cadence/tools/sql"
)

const (
	driverName = "postgres"

	dataSourceNamePostgres = "user=%v password=%v host=%v port=%v dbname=%v sslmode=disable "

	readSchemaVersionPostgres = `SELECT curr_version from schema_version where db_name=$1`

	writeSchemaVersionPostgres = `INSERT into schema_version(db_name, creation_time, curr_version, min_compatible_version) VALUES ($1,$2,$3,$4)
										ON CONFLICT (db_name) DO UPDATE 
										  SET creation_time = excluded.creation_time,
										   	  curr_version = excluded.curr_version,
										      min_compatible_version = excluded.min_compatible_version;`

	writeSchemaUpdateHistoryPostgres = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES($1,$2,$3,$4,$5,$6,$7)`

	createSchemaVersionTablePostgres = `CREATE TABLE schema_version(db_name VARCHAR(255) not null PRIMARY KEY, ` +
		`creation_time TIMESTAMP, ` +
		`curr_version VARCHAR(64), ` +
		`min_compatible_version VARCHAR(64));`

	createSchemaUpdateHistoryTablePostgres = `CREATE TABLE schema_update_history(` +
		`year int not null, ` +
		`month int not null, ` +
		`update_time TIMESTAMP not null, ` +
		`description VARCHAR(255), ` +
		`manifest_md5 VARCHAR(64), ` +
		`new_version VARCHAR(64), ` +
		`old_version VARCHAR(64), ` +
		`PRIMARY KEY (year, month, update_time));`

	//NOTE we have to use %v because somehow mysql doesn't work with ? here
	createDatabasePostgres = "CREATE database %v"

	dropDatabasePostgres = "Drop database %v"

	listTablesPostgres = "select table_name from information_schema.tables where table_schema='public'"

	dropTablePostgres = "DROP TABLE %v"
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
	if database == "" {
		database = "postgres"
	}
	db, err := sqlx.Connect(driverName, fmt.Sprintf(dataSourceNamePostgres, user, passwd, host, port, database))

	if err != nil {
		return nil, err
	}
	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func (d *driver) GetReadSchemaVersionSQL() string {
	return readSchemaVersionPostgres
}

func (d *driver) GetWriteSchemaVersionSQL() string {
	return writeSchemaVersionPostgres
}

func (d *driver) GetWriteSchemaUpdateHistorySQL() string {
	return writeSchemaUpdateHistoryPostgres
}

func (d *driver) GetCreateSchemaVersionTableSQL() string {
	return createSchemaVersionTablePostgres
}

func (d *driver) GetCreateSchemaUpdateHistoryTableSQL() string {
	return createSchemaUpdateHistoryTablePostgres
}

func (d *driver) GetCreateDatabaseSQL() string {
	return createDatabasePostgres
}

func (d *driver) GetDropDatabaseSQL() string {
	return dropDatabasePostgres
}

func (d *driver) GetListTablesSQL() string {
	return listTablesPostgres
}

func (d *driver) GetDropTableSQL() string {
	return dropTablePostgres
}
