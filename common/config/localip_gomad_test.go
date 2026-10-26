//go:build gomad

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListenIPRefusesHostDiscovery(t *testing.T) {
	ip, err := ListenIP()
	require.ErrorContains(t, err, "gomad: host interface discovery is unavailable")
	require.Nil(t, ip)
}
