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

package storage

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/common/persistence/sql/storage/mysql"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

const (
	dsnFmt                       = "%s:%s@%v(%v)/%s"
	isolationLevelAttrName       = "transaction_isolation"
	isolationLevelAttrNameLegacy = "tx_isolation"
	defaultIsolationLevel        = "'READ-COMMITTED'"
)

var dsnAttrOverrides = map[string]string{
	"parseTime":       "true",
	"clientFoundRows": "true",
	"multiStatements": "true",
}

// NewSQLDB creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func NewSQLDB(cfg *config.SQL) (sqldb.Interface, error) {
	db, err := sqlx.Connect(cfg.DriverName, buildDSN(cfg))
	if err != nil {
		return nil, err
	}
	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return mysql.NewDB(db, nil), nil
}

func buildDSN(cfg *config.SQL) string {
	attrs := buildDSNAttrs(cfg)
	dsn := fmt.Sprintf(dsnFmt, cfg.User, cfg.Password, cfg.ConnectProtocol, cfg.ConnectAddr, cfg.DatabaseName)
	if attrs != "" {
		dsn = dsn + "?" + attrs
	}
	return dsn
}

func buildDSNAttrs(cfg *config.SQL) string {
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

	first := true
	var buf bytes.Buffer
	for k, v := range attrs {
		if !first {
			buf.WriteString("&")
		}
		first = false
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v)
	}
	return url.PathEscape(buf.String())
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
