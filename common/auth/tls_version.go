package auth

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// DefaultMinTLSVersion is the minimum TLS version used when it is not set in configuration.
const DefaultMinTLSVersion = tls.VersionTLS12

var supportedTLSVersions = map[string]uint16{
	"1.2": tls.VersionTLS12,
	"1.3": tls.VersionTLS13,
}

type (
	// TLSVersions holds the TLS protocol version bounds applied to a tls.Config.
	// The zero value keeps the historical behavior: minimum TLS 1.2 and maximum
	// picked by the Go runtime.
	TLSVersions struct {
		Min uint16
		Max uint16
	}
)

// NewTLSVersions parses minVersion and maxVersion configuration values.
// Both are optional, supported values are "1.2" and "1.3".
func NewTLSVersions(minVersion string, maxVersion string) (TLSVersions, error) {
	var versions TLSVersions
	var err error

	versions.Min, err = parseTLSVersion(minVersion)
	if err != nil {
		return TLSVersions{}, fmt.Errorf("%w: invalid minVersion: %w", ErrTLSConfig, err)
	}
	versions.Max, err = parseTLSVersion(maxVersion)
	if err != nil {
		return TLSVersions{}, fmt.Errorf("%w: invalid maxVersion: %w", ErrTLSConfig, err)
	}
	if versions.Min != 0 && versions.Max != 0 && versions.Max < versions.Min {
		return TLSVersions{}, fmt.Errorf("%w: maxVersion %q is lower than minVersion %q", ErrTLSConfig, maxVersion, minVersion)
	}
	return versions, nil
}

// apply sets version bounds on a tls.Config, defaulting the minimum when unset.
func (v TLSVersions) apply(config *tls.Config) {
	config.MinVersion = cmp.Or(v.Min, uint16(DefaultMinTLSVersion))
	config.MaxVersion = v.Max
}

func parseTLSVersion(version string) (uint16, error) {
	version = strings.TrimSpace(version)
	if version == "" {
		return 0, nil
	}
	parsed, ok := supportedTLSVersions[version]
	if !ok {
		return 0, fmt.Errorf("%q is not a supported TLS version, expected one of: %s",
			version, strings.Join(slices.Sorted(maps.Keys(supportedTLSVersions)), ", "))
	}
	return parsed, nil
}
