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

package auth

type (
	// TLS describe TLS configuration (for Cassandra, SQL)
	TLS struct {
		Enabled bool `yaml:"enabled"`

		// CertPath and KeyPath are optional depending on server
		// config, but both fields must be omitted to avoid using a
		// client certificate
		CertFile string `yaml:"certFile"`
		KeyFile  string `yaml:"keyFile"`
		CaFile   string `yaml:"caFile"` //optional depending on server config

		// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this on
		// This option is basically the inverse of InSecureSkipVerify
		// See InSecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info
		EnableHostVerification bool `yaml:"enableHostVerification"`

		ServerName string `yaml:"serverName"`

		// Base64 equivalents of the above artifacts.
		// You cannot specify both a Data and a File for the same artifact (e.g. setting CertFile and CertData)
		CertData string `yaml:"certData"`
		KeyData  string `yaml:"keyData"`
		CaData   string `yaml:"caData"` // optional depending on server config
	}
)
