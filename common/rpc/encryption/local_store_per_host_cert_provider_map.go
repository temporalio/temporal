package encryption

import (
	"cmp"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

var _ PerHostCertProviderMap = (*localStorePerHostCertProviderMap)(nil)
var _ CertExpirationChecker = (*localStorePerHostCertProviderMap)(nil)

type localStorePerHostCertProviderMap struct {
	certProviderCache map[string]CertProvider
	clientAuthCache   map[string]bool
	tlsVersionsCache  map[string]auth.TLSVersions
}

// perHostTLSSettings is a host override with its host name normalized and its TLS protocol
// version bounds resolved.
type perHostTLSSettings struct {
	server   config.ServerTLS
	versions auth.TLSVersions
}

// resolvePerHostTLSSettings normalizes host names and resolves version bounds, inheriting the
// group bounds for the ones an override does not set, so that adding a host override cannot
// silently widen the range configured for the group.
//
// Host names that collide after normalization are rejected: map iteration order would otherwise
// decide which of them supplies the TLS version bounds, making the effective policy differ
// between restarts. It only reads configuration: no providers are created, so it is safe to
// call before anything that would need to be cleaned up on failure.
func resolvePerHostTLSSettings(group *config.GroupTLS) (map[string]perHostTLSSettings, error) {
	if group.PerHostOverrides == nil {
		return nil, nil
	}

	resolved := make(map[string]perHostTLSSettings, len(group.PerHostOverrides))
	for host, settings := range group.PerHostOverrides {
		normalized := strings.ToLower(host)
		if _, ok := resolved[normalized]; ok {
			return nil, fmt.Errorf("duplicate host override %q: host names are matched case insensitively", normalized)
		}

		versions, err := auth.NewTLSVersions(
			inheritTLSVersion(settings.MinVersion, group.Server.MinVersion),
			inheritTLSVersion(settings.MaxVersion, group.Server.MaxVersion),
		)
		if err != nil {
			return nil, fmt.Errorf("invalid TLS settings for host %q: %w", host, err)
		}
		resolved[normalized] = perHostTLSSettings{server: settings, versions: versions}
	}
	return resolved, nil
}

// inheritTLSVersion falls back to the group value when the override is not set. Values are
// trimmed first, so that a blank override inherits instead of resolving to the default.
func inheritTLSVersion(override string, group string) string {
	return cmp.Or(strings.TrimSpace(override), strings.TrimSpace(group))
}

func newLocalStorePerHostCertProviderMap(
	perHostSettings map[string]perHostTLSSettings,
	certProviderFactory CertProviderFactory,
	refreshInterval time.Duration,
	logger log.Logger,
) *localStorePerHostCertProviderMap {

	providerMap := &localStorePerHostCertProviderMap{}
	if perHostSettings == nil {
		return providerMap
	}

	providerMap.certProviderCache = make(map[string]CertProvider, len(perHostSettings))
	providerMap.clientAuthCache = make(map[string]bool, len(perHostSettings))
	providerMap.tlsVersionsCache = make(map[string]auth.TLSVersions, len(perHostSettings))

	// Host names are already normalized by resolvePerHostTLSSettings.
	for host, settings := range perHostSettings {
		providerMap.certProviderCache[host] = certProviderFactory(
			&config.GroupTLS{Server: settings.server}, nil, nil, refreshInterval, logger)
		providerMap.clientAuthCache[host] = settings.server.RequireClientAuth
		providerMap.tlsVersionsCache[host] = settings.versions
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

// getTLSVersions implements perHostTLSVersionsProvider.
func (f *localStorePerHostCertProviderMap) getTLSVersions(hostName string) (auth.TLSVersions, bool) {
	versions, ok := f.tlsVersionsCache[strings.ToLower(hostName)]
	return versions, ok
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
