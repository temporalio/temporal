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

// Package ringpop provides a service-based membership monitor
package ringpop

import (
	"fmt"
	"math/bits"

	"github.com/dgryski/go-farm"
	rpmembership "github.com/temporalio/ringpop-go/membership"

	"go.temporal.io/server/common/membership"
)

// hostInfo represents a host in a ring.
type hostInfo struct {
	addr           string            // ip:port
	labels         map[string]string // this map is shared with ringpop
	labelsChecksum uint64            // cache this for quick comparison
}

var _ rpmembership.Member = (*hostInfo)(nil)
var _ membership.HostInfo = (*hostInfo)(nil)

// newHostInfo creates a new *hostInfo instance
func newHostInfo(addr string, labels map[string]string) *hostInfo {
	return &hostInfo{
		addr:           addr,
		labels:         labels,
		labelsChecksum: checksumLabels(labels),
	}
}

// GetAddress returns the ip:port address
func (hi *hostInfo) GetAddress() string {
	return hi.addr
}

// Identity implements ringpop's Membership interface
func (hi *hostInfo) Identity() string {
	// For now, we just use the address as the identity.
	return hi.addr
}

// Label implements ringpop's Membership interface
func (hi *hostInfo) Label(key string) (string, bool) {
	value, ok := hi.labels[key]
	return value, ok
}

// summary returns a shorthand summary string suitable for logging.
func (hi *hostInfo) summary() string {
	s := hi.GetAddress()
	for k, v := range hi.labels {
		switch k {
		case roleKey, portKey:
			// skip these, they can be determined from context
		default:
			s += fmt.Sprintf("[%s=%s]", k, v)
		}
	}
	return s
}

// checksumLabels returns a checksum of a labels map
func checksumLabels(labels map[string]string) uint64 {
	var c uint64
	for k, v := range labels {
		kfp := farm.Fingerprint64([]byte(k))
		vfp := farm.Fingerprint64([]byte(v))
		// use xor to combine different labels so that it comes out the same with any iteration
		// order, without needing to sort.
		c ^= kfp + bits.RotateLeft64(vfp, 3)
	}
	return c
}
