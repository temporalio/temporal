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
	"net/url"

	"go.temporal.io/server/common/service/config"
)

const (
	postgreSQLSSLMode     = "sslmode"
	postgreSQLSSLModeNoop = "disable"
	postgreSQLSSLModeCA   = "verify-ca"
	postgreSQLSSLModeFull = "verify-full"

	postgreSQLSSLHost = "host"

	postgreSQLCA   = "sslrootcert"
	postgreSQLKey  = "sslkey"
	postgreSQLCert = "sslcert"
)

func dsnTSL(cfg *config.SQL) url.Values {
	sslParams := url.Values{}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if !cfg.TLS.EnableHostVerification {
			sslParams.Set(postgreSQLSSLMode, postgreSQLSSLModeCA)
		} else {
			sslParams.Set(postgreSQLSSLMode, postgreSQLSSLModeFull)
			sslParams.Set(postgreSQLSSLHost, cfg.TLS.ServerName)
		}

		if cfg.TLS.CaFile != "" {
			sslParams.Set(postgreSQLCA, cfg.TLS.CaFile)
		}
		if cfg.TLS.KeyFile != "" && cfg.TLS.CertFile != "" {
			sslParams.Set(postgreSQLKey, cfg.TLS.KeyFile)
			sslParams.Set(postgreSQLCert, cfg.TLS.CertFile)
		}
	} else {
		sslParams.Set(postgreSQLSSLMode, postgreSQLSSLModeNoop)
	}
	return sslParams
}
