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
	"strings"
	"time"

	"go.temporal.io/server/common/config"
)

var _ PerHostCertProviderMap = (*localStorePerHostCertProviderMap)(nil)
var _ CertExpirationChecker = (*localStorePerHostCertProviderMap)(nil)

type localStorePerHostCertProviderMap struct {
	certProviderCache map[string]*localStoreCertProvider
}

func newLocalStorePerHostCertProviderMap(overrides map[string]config.ServerTLS) *localStorePerHostCertProviderMap {

	factory := &localStorePerHostCertProviderMap{}
	if overrides == nil {
		return factory
	}

	factory.certProviderCache = make(map[string]*localStoreCertProvider, len(overrides))

	for host, settings := range overrides {
		factory.certProviderCache[strings.ToLower(host)] = &localStoreCertProvider{
			tlsSettings: &config.GroupTLS{
				Server: settings,
			},
		}
	}

	return factory
}

func (f *localStorePerHostCertProviderMap) GetCertProvider(hostName string) (CertProvider, error) {

	if f.certProviderCache == nil {
		return nil, nil
	}
	cachedCertProvider, ok := f.certProviderCache[strings.ToLower(hostName)]
	if !ok {
		return nil, nil
	}
	return cachedCertProvider, nil
}

func (f *localStorePerHostCertProviderMap) GetExpiringCerts(timeWindow time.Duration,
) (expiring CertExpirationMap, expired CertExpirationMap, err error) {

	expiring = make(CertExpirationMap)
	expired = make(CertExpirationMap)

	for _, provider := range f.certProviderCache {

		providerExpiring, providerExpired, providerError := provider.GetExpiringCerts(timeWindow)
		mergeMaps(expiring, providerExpiring)
		mergeMaps(expired, providerExpired)
		if providerError != nil {
			err = appendError(err, providerError)
		}
	}
	return expiring, expired, err
}
