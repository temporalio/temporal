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

package tests

import (
	"context"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type simpleMonitor struct {
	hosts     map[primitives.ServiceName][]string
	resolvers map[primitives.ServiceName]*simpleResolver
}

// NewSimpleMonitor returns a simple monitor interface
func newSimpleMonitor(hosts map[primitives.ServiceName][]string) *simpleMonitor {
	resolvers := make(map[primitives.ServiceName]*simpleResolver, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newSimpleResolver(service, hostList)
	}

	return &simpleMonitor{
		hosts:     hosts,
		resolvers: resolvers,
	}
}

func (s *simpleMonitor) Start() {
	for service, r := range s.resolvers {
		r.start(s.hosts[service])
	}
}

func (s *simpleMonitor) EvictSelf() error {
	return nil
}

func (s *simpleMonitor) GetResolver(service primitives.ServiceName) (membership.ServiceResolver, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, membership.ErrUnknownService
	}
	return resolver, nil
}

func (s *simpleMonitor) GetReachableMembers() ([]string, error) {
	return nil, nil
}

func (s *simpleMonitor) WaitUntilInitialized(_ context.Context) error {
	return nil
}

func (s *simpleMonitor) SetDraining(draining bool) error {
	return nil
}
