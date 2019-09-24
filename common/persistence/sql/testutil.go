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
	"io/ioutil"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/uber/cadence/common/service/config"
)

const (
	defaultDriverName = "mysql"
	dataSourceName    = "%s:%s@%v(%v)/%s?multiStatements=true&transaction_isolation=%%27READ-COMMITTED%%27&parseTime=true&clientFoundRows=true"
)

func newConnection(cfg config.SQL) (*sqlx.DB, error) {
	var db, err = sqlx.Connect(cfg.DriverName,
		fmt.Sprintf(dataSourceName, cfg.User, cfg.Password, cfg.ConnectProtocol, cfg.ConnectAddr, cfg.DatabaseName))
	if err != nil {
		return nil, err
	}
	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func createDatabase(driver string, addr string, username, password, dbName string, overwrite bool) error {
	var db, err = sqlx.Connect(driver, fmt.Sprintf(dataSourceName, username, password, "tcp", addr, ""))
	if err != nil {
		return fmt.Errorf("failure connecting to mysql database: %v", err)
	}

	if overwrite {
		dropDatabase(db, dbName)
	}
	_, err = db.Exec(`CREATE DATABASE ` + dbName)
	if err != nil {
		return fmt.Errorf("failure creating database %v: %v", dbName, err)
	}
	log.WithField(`database-name`, dbName).Debug(`created database`)
	return nil
}

// DropCassandraKeyspace drops the given keyspace, if it exists
func dropDatabase(db *sqlx.DB, dbName string) (err error) {
	_, err = db.Exec("DROP DATABASE " + dbName)
	if err != nil {
		return err
	}
	log.WithField(`database-name`, dbName).Info(`dropped database`)
	return nil
}

// loadDatabaseSchema loads the schema from the given .sql files on this database
func loadDatabaseSchema(dir string, fileNames []string, db *sqlx.DB, override bool) (err error) {

	for _, file := range fileNames {
		// This is only used in tests. Excluding it from security scanners
		// #nosec
		content, err := ioutil.ReadFile(dir + "/" + file)
		if err != nil {
			return fmt.Errorf("error reading contents of file %v:%v", file, err.Error())
		}
		_, err = db.Exec(string(content))
		if err != nil {
			err = fmt.Errorf("error loading schema from %v: %v", file, err.Error())
		}
	}
	return nil
}
