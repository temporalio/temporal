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

package config

// ** This code is copied from tchannel, we would like to not take dependency on tchannel code **

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScoreAddr(t *testing.T) {
	ipv4 := net.ParseIP("10.0.1.2")
	ipv6 := net.ParseIP("2001:db8:a0b:12f0::1")

	tests := []struct {
		msg    string
		iface  net.Interface
		addr   net.Addr
		want   int
		wantIP net.IP
	}{
		{
			msg:    "non-local up ipv4 IPNet address",
			iface:  net.Interface{Flags: net.FlagUp},
			addr:   &net.IPNet{IP: ipv4},
			want:   500,
			wantIP: ipv4,
		},
		{
			msg:    "non-local up ipv4 IPAddr address",
			iface:  net.Interface{Flags: net.FlagUp},
			addr:   &net.IPAddr{IP: ipv4},
			want:   500,
			wantIP: ipv4,
		},
		{
			msg: "non-local up ipv4 IPAddr address, docker interface",
			iface: net.Interface{
				Flags:        net.FlagUp,
				HardwareAddr: mustParseMAC("02:42:ac:11:56:af"),
			},
			addr:   &net.IPNet{IP: ipv4},
			want:   500,
			wantIP: ipv4,
		},
		{
			msg: "non-local up ipv4 address, local MAC address",
			iface: net.Interface{
				Flags:        net.FlagUp,
				HardwareAddr: mustParseMAC("02:42:9c:52:fc:86"),
			},
			addr:   &net.IPNet{IP: ipv4},
			want:   500,
			wantIP: ipv4,
		},
		{
			msg:    "non-local down ipv4 address",
			iface:  net.Interface{},
			addr:   &net.IPNet{IP: ipv4},
			want:   400,
			wantIP: ipv4,
		},
		{
			msg:    "non-local down ipv6 address",
			iface:  net.Interface{},
			addr:   &net.IPAddr{IP: ipv6},
			want:   100,
			wantIP: ipv6,
		},
		{
			msg:   "unknown address type",
			iface: net.Interface{},
			addr:  &net.UnixAddr{Name: "/tmp/socket"},
			want:  -1,
		},
	}

	for _, tt := range tests {
		gotScore, gotIP := scoreAddr(tt.iface, tt.addr)
		assert.Equal(t, tt.want, gotScore, tt.msg)
		assert.Equal(t, tt.wantIP, gotIP, tt.msg)
	}
}
