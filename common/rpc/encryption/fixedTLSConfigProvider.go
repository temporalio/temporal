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

package encryption

import (
	"crypto/tls"
	"time"
)

// FixedTLSConfigProvider is a [TLSConfigProvider] that is for fixed sets of TLS
// configs. This is usually only used for testing.

type FixedTLSConfigProvider struct {
	InternodeServerConfig      *tls.Config
	InternodeClientConfig      *tls.Config
	FrontendServerConfig       *tls.Config
	FrontendClientConfig       *tls.Config
	RemoteClusterClientConfigs map[string]*tls.Config
	CertExpirationChecker      CertExpirationChecker
}

var _ TLSConfigProvider = (*FixedTLSConfigProvider)(nil)

// GetInternodeServerConfig implements [TLSConfigProvider.GetInternodeServerConfig].
func (f *FixedTLSConfigProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return f.InternodeServerConfig, nil
}

// GetInternodeClientConfig implements [TLSConfigProvider.GetInternodeClientConfig].
func (f *FixedTLSConfigProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return f.InternodeClientConfig, nil
}

// GetFrontendServerConfig implements [TLSConfigProvider.GetFrontendServerConfig].
func (f *FixedTLSConfigProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return f.FrontendServerConfig, nil
}

// GetFrontendClientConfig implements [TLSConfigProvider.GetFrontendClientConfig].
func (f *FixedTLSConfigProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return f.FrontendClientConfig, nil
}

// GetRemoteClusterClientConfig implements [TLSConfigProvider.GetRemoteClusterClientConfig].
func (f *FixedTLSConfigProvider) GetRemoteClusterClientConfig(hostname string) (*tls.Config, error) {
	return f.RemoteClusterClientConfigs[hostname], nil
}

// GetExpiringCerts implements [TLSConfigProvider.GetExpiringCerts].
func (f *FixedTLSConfigProvider) GetExpiringCerts(
	timeWindow time.Duration,
) (expiring CertExpirationMap, expired CertExpirationMap, err error) {
	if f.CertExpirationChecker != nil {
		return f.CertExpirationChecker.GetExpiringCerts(timeWindow)
	}
	return nil, nil, nil
}
