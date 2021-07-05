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

package mysql

import (
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName                   = "mysql"
	isolationLevelAttrName       = "transaction_isolation"
	isolationLevelAttrNameLegacy = "tx_isolation"
	defaultIsolationLevel        = "'READ-COMMITTED'"
	// customTLSName is the name used if a custom tls configuration is created
	customTLSName = "tls-custom"
)

var dsnAttrOverrides = map[string]string{
	"parseTime":       "true",
	"clientFoundRows": "true",
}

type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.DB, error) {
	conn, err := p.createDBConnection(cfg, r)
	if err != nil {
		return nil, err
	}
	db := newDB(dbKind, cfg.DatabaseName, conn, nil)
	return db, nil
}

// CreateAdminDB initialize the db object
func (p *plugin) CreateAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.AdminDB, error) {
	conn, err := p.createDBConnection(cfg, r)
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
func (p *plugin) createDBConnection(cfg *config.SQL, r resolver.ServiceResolver) (*sqlx.DB, error) {
	err := registerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sqlx.Connect(PluginName, buildDSN(cfg, r))
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

func buildDSN(cfg *config.SQL, r resolver.ServiceResolver) string {
	mysqlConfig := mysql.NewConfig()

	mysqlConfig.User = cfg.User
	mysqlConfig.Passwd = cfg.Password
	mysqlConfig.Addr = r.Resolve(cfg.ConnectAddr)[0]
	mysqlConfig.DBName = cfg.DatabaseName
	mysqlConfig.Net = cfg.ConnectProtocol
	mysqlConfig.Params = buildDSNAttrs(cfg)

	// https://github.com/go-sql-driver/mysql/blob/v1.5.0/dsn.go#L104-L106
	// https://github.com/go-sql-driver/mysql/blob/v1.5.0/dsn.go#L182-L189
	if mysqlConfig.Net == "" {
		mysqlConfig.Net = "tcp"
	}

	// https://github.com/go-sql-driver/mysql#rejectreadonly
	// https://github.com/temporalio/temporal/issues/1703
	mysqlConfig.RejectReadOnly = true

	return mysqlConfig.FormatDSN()
}

func buildDSNAttrs(cfg *config.SQL) map[string]string {
	attrs := make(map[string]string, len(dsnAttrOverrides)+len(cfg.ConnectAttributes)+1)
	for k, v := range cfg.ConnectAttributes {
		k1, v1 := sanitizeAttr(k, v)
		attrs[k1] = v1
	}

	// only override isolation level if not specified
	if !hasAttr(attrs, isolationLevelAttrName) &&
		!hasAttr(attrs, isolationLevelAttrNameLegacy) {
		attrs[isolationLevelAttrName] = defaultIsolationLevel
	}

	// these attrs are always overriden
	for k, v := range dsnAttrOverrides {
		attrs[k] = v
	}

	return attrs
}

func hasAttr(attrs map[string]string, key string) bool {
	_, ok := attrs[key]
	return ok
}

func sanitizeAttr(inkey string, invalue string) (string, string) {
	key := strings.ToLower(strings.TrimSpace(inkey))
	value := strings.ToLower(strings.TrimSpace(invalue))
	switch key {
	case isolationLevelAttrName, isolationLevelAttrNameLegacy:
		if value[0] != '\'' { // mysql sys variable values must be enclosed in single quotes
			value = "'" + value + "'"
		}
		return key, value
	default:
		return inkey, invalue
	}
}
