// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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
	a.Equal(ip.String(), LocalhostIPDefault)
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
