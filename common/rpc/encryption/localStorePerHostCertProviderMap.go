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
	"go.temporal.io/server/common/log"
)

var _ PerHostCertProviderMap = (*localStorePerHostCertProviderMap)(nil)
var _ CertExpirationChecker = (*localStorePerHostCertProviderMap)(nil)

type localStorePerHostCertProviderMap struct {
	certProviderCache map[string]CertProvider
	clientAuthCache   map[string]bool
}

func newLocalStorePerHostCertProviderMap(
	overrides map[string]config.ServerTLS,
	certProviderFactory CertProviderFactory,
	refreshInterval time.Duration,
	logger log.Logger,
) *localStorePerHostCertProviderMap {

	providerMap := &localStorePerHostCertProviderMap{}
	if overrides == nil {
		return providerMap
	}

	providerMap.certProviderCache = make(map[string]CertProvider, len(overrides))
	providerMap.clientAuthCache = make(map[string]bool, len(overrides))

	for host, settings := range overrides {
		lcHost := strings.ToLower(host)

		provider := certProviderFactory(&config.GroupTLS{Server: settings}, nil, nil, refreshInterval, logger)
		providerMap.certProviderCache[lcHost] = provider
		providerMap.clientAuthCache[lcHost] = settings.RequireClientAuth
	}

	return providerMap
}

// GetCertProvider for a given host name returns a cert provider (nil if not found) and if client authentication is required
func (f *localStorePerHostCertProviderMap) GetCertProvider(hostName string) (CertProvider, bool, error) {

	lcHostName := strings.ToLower(hostName)

	if f.certProviderCache == nil {
		return nil, true, nil
	}
	cachedCertProvider, ok := f.certProviderCache[lcHostName]
	if !ok {
		return nil, true, nil
	}
	clientAuthRequired := f.clientAuthCache[lcHostName]
	return cachedCertProvider, clientAuthRequired, nil
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

func (f *localStorePerHostCertProviderMap) NumberOfHosts() int {

	if f.certProviderCache != nil {
		return len(f.certProviderCache)
	}
	return 0
}
