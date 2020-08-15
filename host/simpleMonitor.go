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
	"fmt"

	"go.temporal.io/server/common/membership"
)

type simpleMonitor struct {
	hostInfo  *membership.HostInfo
	resolvers map[string]membership.ServiceResolver
}

// NewSimpleMonitor returns a simple monitor interface
func newSimpleMonitor(serviceName string, hosts map[string][]string) membership.Monitor {
	resolvers := make(map[string]membership.ServiceResolver, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newSimpleResolver(service, hostList)
	}

	hostInfo := membership.NewHostInfo(hosts[serviceName][0], map[string]string{membership.RoleKey: serviceName})
	return &simpleMonitor{hostInfo, resolvers}
}

func (s *simpleMonitor) Start() {
}

func (s *simpleMonitor) Stop() {
}

func (s *simpleMonitor) EvictSelf() error {
	return nil
}

func (s *simpleMonitor) WhoAmI() (*membership.HostInfo, error) {
	return s.hostInfo, nil
}

func (s *simpleMonitor) GetResolver(service string) (membership.ServiceResolver, error) {
	return s.resolvers[service], nil
}

func (s *simpleMonitor) Lookup(service string, key string) (*membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, fmt.Errorf("cannot lookup host for service %v", service)
	}
	return resolver.Lookup(key)
}

func (s *simpleMonitor) AddListener(service string, name string, notifyChannel chan<- *membership.ChangedEvent) error {
	return nil
}

func (s *simpleMonitor) RemoveListener(service string, name string) error {
	return nil
}

func (s *simpleMonitor) GetReachableMembers() ([]string, error) {
	return nil, nil
}

func (s *simpleMonitor) GetMemberCount(service string) (int, error) {
	return 0, nil
}
