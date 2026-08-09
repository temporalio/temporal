package auth

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTLSVersions(t *testing.T) {
	testCases := []struct {
		name        string
		minVersion  string
		maxVersion  string
		expectedMin uint16
		expectedMax uint16
		expectedErr bool
	}{
		{
			name: "not set",
		},
		{
			name:        "min only",
			minVersion:  "1.3",
			expectedMin: tls.VersionTLS13,
		},
		{
			name:        "max only",
			maxVersion:  "1.2",
			expectedMax: tls.VersionTLS12,
		},
		{
			name:        "pinned to 1.3",
			minVersion:  "1.3",
			maxVersion:  "1.3",
			expectedMin: tls.VersionTLS13,
			expectedMax: tls.VersionTLS13,
		},
		{
			name:        "surrounding whitespace is ignored",
			minVersion:  " 1.2 ",
			expectedMin: tls.VersionTLS12,
		},
		{
			name:       "whitespace only means unset",
			minVersion: "  ",
			maxVersion: "\t",
		},
		{
			name:        "unknown min version",
			minVersion:  "1.4",
			expectedErr: true,
		},
		{
			name:        "unsupported min version",
			minVersion:  "1.1",
			expectedErr: true,
		},
		{
			name:        "unknown max version",
			maxVersion:  "TLSv1.3",
			expectedErr: true,
		},
		{
			name:        "max lower than min",
			minVersion:  "1.3",
			maxVersion:  "1.2",
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			versions, err := NewTLSVersions(tc.minVersion, tc.maxVersion)
			if tc.expectedErr {
				require.ErrorIs(t, err, ErrTLSConfig)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedMin, versions.Min)
			require.Equal(t, tc.expectedMax, versions.Max)
		})
	}
}

func TestTLSVersionsApply(t *testing.T) {
	// The exported constructors and zero bounds both keep the historical defaults.
	for _, config := range []*tls.Config{NewEmptyTLSConfig(), newEmptyTLSConfig(TLSVersions{})} {
		require.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
		require.Zero(t, config.MaxVersion)
	}

	versions, err := NewTLSVersions("1.3", "1.3")
	require.NoError(t, err)
	pinned := newEmptyTLSConfig(versions)
	require.Equal(t, uint16(tls.VersionTLS13), pinned.MinVersion)
	require.Equal(t, uint16(tls.VersionTLS13), pinned.MaxVersion)
}
