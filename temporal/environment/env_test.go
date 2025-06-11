package environment

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookupLocalhostIPSuccess(t *testing.T) {
	t.Parallel()
	a := assert.New(t)
	ipString := lookupLocalhostIP("localhost")
	ip := net.ParseIP(ipString)
	// localhost needs to resolve to a loopback address
	// whether it's ipv4 or ipv6 - the result depends on
	// the system running this test
	a.True(ip.IsLoopback())
}

func TestLookupLocalhostIPMissingHostname(t *testing.T) {
	t.Parallel()
	a := assert.New(t)
	ipString := lookupLocalhostIP("")
	ip := net.ParseIP(ipString)
	a.True(ip.IsLoopback())
	// if host can't be found, use ipv4 loopback
	a.Equal(ip.String(), localhostIPDefault)
}

func TestLookupLocalhostIPWithIPv6(t *testing.T) {
	t.Parallel()
	a := assert.New(t)
	ipString := lookupLocalhostIP("::1")
	ip := net.ParseIP(ipString)
	a.True(ip.IsLoopback())
	// return ipv6 if only ipv6 is available
	a.Equal(ip, net.ParseIP("::1"))
}
