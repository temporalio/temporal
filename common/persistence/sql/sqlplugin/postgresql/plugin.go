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
	"errors"
	"fmt"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/service/config"
)

const (
	// PluginName is the name of the plugin
	PluginName = "postgres"
	dsnFmt     = "postgres://%v:%v@%v/%v?%v"
)

var errTLSNotImplemented = errors.New("tls for postgresql has not been implemented")

type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB initialize the db object
func (d *plugin) CreateDB(cfg *config.SQL) (sqlplugin.DB, error) {
	conn, err := d.createDBConnection(cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(conn, nil)
	return db, nil
}

// CreateAdminDB initialize the adminDB object
func (d *plugin) CreateAdminDB(cfg *config.SQL) (sqlplugin.AdminDB, error) {
	conn, err := d.createDBConnection(cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(conn, nil)
	return db, nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (d *plugin) createDBConnection(cfg *config.SQL) (*sqlx.DB, error) {

	db, err := sqlx.Connect(PluginName, buildDSN(cfg))
	if err != nil {
		return nil, err
	}
	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func buildDSN(cfg *config.SQL) string {
	tlsAttrs := dsnTSL(cfg).Encode()
	dsn := fmt.Sprintf(
		dsnFmt,
		cfg.User,
		cfg.Password,
		cfg.ConnectAddr,
		databaseName(cfg.DatabaseName),
		tlsAttrs,
	)
	return dsn
}

func databaseName(dbName string) string {
	//NOTE: postgres doesn't allow to connect with empty dbName, the admin dbName is "postgres"
	if dbName == "" {
		return "postgres"
	}
	return dbName
}
