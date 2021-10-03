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
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName = "postgres"
	dsnFmt     = "postgres://%v:%v@%v/%v?%v"
)

var (
	defaultDatabaseNames = []string{
		"postgres",  // normal PostgreSQL default DB name
		"defaultdb", // special behavior for Aiven: #1389
	}
)

type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB initialize the db object
func (d *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.DB, error) {
	conn, err := d.createDBConnection(cfg, r)
	if err != nil {
		return nil, err
	}
	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
	return db, nil
}

// CreateAdminDB initialize the adminDB object
func (d *plugin) CreateAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.AdminDB, error) {
	conn, err := d.createDBConnection(cfg, r)
	if err != nil {
		return nil, err
	}
	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
	return db, nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (d *plugin) createDBConnection(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (*sqlx.DB, error) {
	db, err := d.tryConnect(cfg, r)
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

func (d *plugin) tryConnect(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (*sqlx.DB, error) {
	if cfg.DatabaseName != "" {
		return sqlx.Connect(PluginName, buildDSN(cfg, r))
	}

	// database name not provided
	// try defaults
	defer func() { cfg.DatabaseName = "" }()

	var errors []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		if sqlxDB, err := sqlx.Connect(PluginName, buildDSN(cfg, r)); err == nil {
			return sqlxDB, nil
		} else {
			errors = append(errors, err)
		}
	}
	return nil, serviceerror.NewUnavailable(
		fmt.Sprintf("unable to connect to DB, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errors),
	)
}

func buildDSN(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) string {
	tlsAttrs := buildDSNAttr(cfg).Encode()
	resolvedAddr := r.Resolve(cfg.ConnectAddr)[0]
	dsn := fmt.Sprintf(
		dsnFmt,
		cfg.User,
		url.QueryEscape(cfg.Password),
		resolvedAddr,
		cfg.DatabaseName,
		tlsAttrs,
	)
	return dsn
}
