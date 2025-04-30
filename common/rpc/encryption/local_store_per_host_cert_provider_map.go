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
