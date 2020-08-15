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

package host

import (
	"github.com/dgryski/go-farm"

	"go.temporal.io/server/common/membership"
)

type simpleResolver struct {
	hosts    []*membership.HostInfo
	hashfunc func([]byte) uint32
}

// newSimpleResolver returns a service resolver that maintains static mapping
// between services and host info
func newSimpleResolver(service string, hosts []string) membership.ServiceResolver {
	hostInfos := make([]*membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		hostInfos = append(hostInfos, membership.NewHostInfo(host, map[string]string{membership.RoleKey: service}))
	}
	return &simpleResolver{hostInfos, farm.Fingerprint32}
}

func (s *simpleResolver) Lookup(key string) (*membership.HostInfo, error) {
	hash := int(s.hashfunc([]byte(key)))
	idx := hash % len(s.hosts)
	return s.hosts[idx], nil
}

func (s *simpleResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	return nil
}

func (s *simpleResolver) RemoveListener(name string) error {
	return nil
}

func (s *simpleResolver) MemberCount() int {
	return len(s.hosts)
}

func (s *simpleResolver) Members() []*membership.HostInfo {
	return s.hosts
}
