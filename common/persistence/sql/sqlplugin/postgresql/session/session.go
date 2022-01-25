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

package session

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/resolver"
)

const (
	dsnFmt     = "postgres://%v:%v@%v/%v?%v"
	driverName = "postgres"
)

const (
	sslMode        = "sslmode"
	sslModeNoop    = "disable"
	sslModeRequire = "require"
	sslModeFull    = "verify-full"

	sslHost = "host"

	sslCA   = "sslrootcert"
	sslKey  = "sslkey"
	sslCert = "sslcert"
)

type Session struct {
	*sqlx.DB
}

func NewSession(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(cfg, resolver)
	if err != nil {
		return nil, err
	}
	return &Session{DB: db}, nil
}

func (s *Session) Close() {
	if s.DB != nil {
		_ = s.DB.Close()
	}
}

func createConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driverName, buildDSN(cfg, resolver))
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

func buildDSNAttr(cfg *config.SQL) url.Values {
	parameters := url.Values{}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if !cfg.TLS.EnableHostVerification {
			parameters.Set(sslMode, sslModeRequire)
		} else {
			parameters.Set(sslMode, sslModeFull)
			parameters.Set(sslHost, cfg.TLS.ServerName)
		}

		if cfg.TLS.CaFile != "" {
			parameters.Set(sslCA, cfg.TLS.CaFile)
		}
		if cfg.TLS.KeyFile != "" && cfg.TLS.CertFile != "" {
			parameters.Set(sslKey, cfg.TLS.KeyFile)
			parameters.Set(sslCert, cfg.TLS.CertFile)
		}
	} else {
		parameters.Set(sslMode, sslModeNoop)
	}

	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			panic(fmt.Sprintf("duplicate connection attr: %v:%v, %v:%v",
				key,
				parameters.Get(key),
				key, value,
			))
		}
		parameters.Set(key, value)
	}
	return parameters
}
