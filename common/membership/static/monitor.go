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

package static

import (
	"context"
	"time"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type (
	staticMonitor struct {
		hosts     map[primitives.ServiceName]Hosts
		resolvers map[primitives.ServiceName]*staticResolver
	}

	Hosts struct {
		// Addresses of all hosts per service.
		All []string
		// Address of this host. May be empty if this host is not running this service.
		Self string
	}
)

func SingleLocalHost(host string) Hosts {
	return Hosts{All: []string{host}, Self: host}
}

func newStaticMonitor(hosts map[primitives.ServiceName]Hosts) membership.Monitor {
	resolvers := make(map[primitives.ServiceName]*staticResolver, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newStaticResolver(hostList.All)
	}

	return &staticMonitor{
		hosts:     hosts,
		resolvers: resolvers,
	}
}

func (s *staticMonitor) Start() {
	for service, r := range s.resolvers {
		r.start(s.hosts[service].All)
	}
}

func (s *staticMonitor) EvictSelf() error {
	return nil
}

func (s *staticMonitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	return 0, nil
}

func (s *staticMonitor) GetResolver(service primitives.ServiceName) (membership.ServiceResolver, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, membership.ErrUnknownService
	}
	return resolver, nil
}

func (s *staticMonitor) GetReachableMembers() ([]string, error) {
	return nil, nil
}

func (s *staticMonitor) WaitUntilInitialized(_ context.Context) error {
	return nil
}

func (s *staticMonitor) SetDraining(draining bool) error {
	return nil
}

func (s *staticMonitor) ApproximateMaxPropagationTime() time.Duration {
	return 0
}
