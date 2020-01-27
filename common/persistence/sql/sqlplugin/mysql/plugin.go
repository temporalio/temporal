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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/environment"
)

const (
	// PluginName is the name of the plugin
	PluginName                   = "mysql"
	dsnFmt                       = "%s:%s@%v(%v)/%s"
	isolationLevelAttrName       = "transaction_isolation"
	isolationLevelAttrNameLegacy = "tx_isolation"
	defaultIsolationLevel        = "'READ-COMMITTED'"
	// customTLSName is the name used if a custom tls configuration is created
	customTLSName = "tls-custom"
)

var dsnAttrOverrides = map[string]string{
	"parseTime":       "true",
	"clientFoundRows": "true",
	"multiStatements": "true",
}

type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(cfg *config.SQL) (sqlplugin.DB, error) {
	conn, err := p.createDBConnection(cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(conn, nil)
	return db, nil
}

// CreateAdminDB initialize the db object
func (p *plugin) CreateAdminDB(cfg *config.SQL) (sqlplugin.AdminDB, error) {
	conn, err := p.createDBConnection(cfg)
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
func (p *plugin) createDBConnection(cfg *config.SQL) (*sqlx.DB, error) {
	err := registerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

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

func registerTLSConfig(cfg *config.SQL) error {
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return nil
	}

	host, _, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return fmt.Errorf("error in host port from ConnectAddr: %v", err)
	}

	// TODO: create a way to set MinVersion and CipherSuites via cfg.
	tlsConfig := &tls.Config{
		ServerName: host,
	}

	if cfg.TLS.CaFile != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(cfg.TLS.CaFile)
		if err != nil {
			return fmt.Errorf("failed to load CA files: %v", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return fmt.Errorf("failed to append CA file")
		}
		tlsConfig.RootCAs = rootCertPool
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		clientCert := make([]tls.Certificate, 0, 1)
		certs, err := tls.LoadX509KeyPair(
			cfg.TLS.CertFile,
			cfg.TLS.KeyFile,
		)
		if err != nil {
			return fmt.Errorf("failed to load tls x509 key pair: %v", err)
		}
		clientCert = append(clientCert, certs)
		tlsConfig.Certificates = clientCert
	}

	// In order to use the TLS configuration you need to register it. Once registered you use it by specifying
	// `tls` in the connect attributes.
	err = mysql.RegisterTLSConfig(customTLSName, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to register tls config: %v", err)
	}

	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = map[string]string{}
	}

	// If no `tls` connect attribute is provided then we override it to our newly registered tls config automatically.
	// This allows users to simply provide a tls config without needing to remember to also set the connect attribute
	if cfg.ConnectAttributes["tls"] == "" {
		cfg.ConnectAttributes["tls"] = customTLSName
	}

	return nil
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

const (
	testUser      = "uber"
	testPassword  = "uber"
	testSchemaDir = "schema/mysql/v57"
)

// GetTestClusterOption return test options
func GetTestClusterOption() *pt.TestBaseOptions {
	return &pt.TestBaseOptions{
		SQLDBPluginName: PluginName,
		DBUsername:      testUser,
		DBPassword:      testPassword,
		DBHost:          environment.GetMySQLAddress(),
		DBPort:          environment.GetMySQLPort(),
		SchemaDir:       testSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}
