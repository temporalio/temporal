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

	"go.temporal.io/server/common/config"
)

const (
	postgreSQLSSLMode        = "sslmode"
	postgreSQLSSLModeNoop    = "disable"
	postgreSQLSSLModeRequire = "require"
	postgreSQLSSLModeFull    = "verify-full"

	postgreSQLSSLHost = "host"

	postgreSQLCA   = "sslrootcert"
	postgreSQLKey  = "sslkey"
	postgreSQLCert = "sslcert"
)

func buildDSNAttr(cfg *config.SQL) url.Values {
	parameters := url.Values{}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if !cfg.TLS.EnableHostVerification {
			parameters.Set(postgreSQLSSLMode, postgreSQLSSLModeRequire)
		} else {
			parameters.Set(postgreSQLSSLMode, postgreSQLSSLModeFull)
			parameters.Set(postgreSQLSSLHost, cfg.TLS.ServerName)
		}

		if cfg.TLS.CaFile != "" {
			parameters.Set(postgreSQLCA, cfg.TLS.CaFile)
		}
		if cfg.TLS.KeyFile != "" && cfg.TLS.CertFile != "" {
			parameters.Set(postgreSQLKey, cfg.TLS.KeyFile)
			parameters.Set(postgreSQLCert, cfg.TLS.CertFile)
		}
	} else {
		parameters.Set(postgreSQLSSLMode, postgreSQLSSLModeNoop)
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
