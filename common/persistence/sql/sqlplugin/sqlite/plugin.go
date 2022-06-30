// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package sqlite

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

const (
	// PluginName is the name of the plugin
	PluginName = "sqlite"
)

// List of non-pragma parameters
// Taken from https://www.sqlite.org/uri.html
var queryParameters = map[string]struct{}{
	"cache":     {},
	"immutable": {},
	"mode":      {},
	"modeof":    {},
	"nolock":    {},
	"psow":      {},
	"setup":     {},
	"vfs":       {},
}

type plugin struct {
	connPool *connPool
}

var sqlitePlugin = &plugin{}

func init() {
	sqlitePlugin.connPool = newConnPool()
	sql.RegisterPlugin(PluginName, sqlitePlugin)
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.DB, error) {
	conn, err := p.connPool.Allocate(cfg, r, p.createDBConnection)
	if err != nil {
		return nil, err
	}

	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
	db.OnClose(func() { p.connPool.Close(cfg) }) // remove reference

	return db, nil
}

// CreateAdminDB initialize the db object
func (p *plugin) CreateAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.AdminDB, error) {
	conn, err := p.connPool.Allocate(cfg, r, p.createDBConnection)
	if err != nil {
		return nil, err
	}

	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
	db.OnClose(func() { p.connPool.Close(cfg) }) // remove reference

	return db, nil
}

// createDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database.
func (p *plugin) createDBConnection(
	cfg *config.SQL,
	_ resolver.ServiceResolver,
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("error building DSN: %w", err)
	}

	db, err := sqlx.Connect(goSqlDriverName, dsn)
	if err != nil {
		return nil, err
	}

	// The following options are set based on advice from https://github.com/mattn/go-sqlite3#faq
	//
	// Dealing with the error `database is locked`
	// > ... set the database connections of the SQL package to 1.
	db.SetMaxOpenConns(1)
	// Settings for in-memory database (should be fine for file mode as well)
	// > Note that if the last database connection in the pool closes, the in-memory database is deleted.
	// > Make sure the max idle connection limit is > 0, and the connection lifetime is infinite.
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	switch {
	case cfg.ConnectAttributes["mode"] == "memory":
		// creates temporary DB overlay in order to configure database and schemas
		if err := p.setupSQLiteDatabase(cfg, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	case cfg.ConnectAttributes["setup"] == "true": // file mode, optional setting to setup the schema
		if err := p.setupSQLiteDatabase(cfg, db); err != nil && !isTableExistsError(err) { // benign error indicating tables already exist
			_ = db.Close()
			return nil, err
		}

	}

	return db, nil
}

func (p *plugin) setupSQLiteDatabase(cfg *config.SQL, conn *sqlx.DB) error {
	db := newDB(sqlplugin.DbKindUnknown, cfg.DatabaseName, conn, nil)
	defer func() { _ = db.Close() }()

	err := db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		return err
	}

	// init tables
	return sqliteschema.SetupSchemaOnDB(db)
}

func buildDSN(cfg *config.SQL) (string, error) {
	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = make(map[string]string)
	}
	vals, err := buildDSNAttr(cfg)
	if err != nil {
		return "", err
	}
	dsn := fmt.Sprintf(
		"file:%s?%v",
		cfg.DatabaseName,
		vals.Encode(),
	)
	return dsn, nil
}

func buildDSNAttr(cfg *config.SQL) (url.Values, error) {
	parameters := url.Values{}
	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			return nil, fmt.Errorf("duplicate connection attr: %v:%v, %v:%v",
				key,
				parameters.Get(key),
				key, value,
			)
		}

		if _, isValidQueryParameter := queryParameters[key]; isValidQueryParameter {
			parameters.Set(key, value)
			continue
		}

		// assume pragma
		parameters.Add("_pragma", fmt.Sprintf("%s=%s", key, value))
	}
	return parameters, nil
}
