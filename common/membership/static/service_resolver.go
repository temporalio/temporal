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
	"sync"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/membership"
)

// staticResolver is a service resolver that maintains static mapping between services and host info
type staticResolver struct {
	mu        sync.Mutex
	hostInfos []membership.HostInfo
	listeners map[string]chan<- *membership.ChangedEvent

	hashfunc func([]byte) uint32
}

func newStaticResolver(hosts []string) *staticResolver {
	hostInfos := make([]membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		hostInfos = append(hostInfos, membership.NewHostInfoFromAddress(host))
	}
	return &staticResolver{
		hostInfos: hostInfos,
		hashfunc:  farm.Fingerprint32,
		listeners: make(map[string]chan<- *membership.ChangedEvent),
	}
}

func (s *staticResolver) start(hosts []string) {
	hostInfos := make([]membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		hostInfos = append(hostInfos, membership.NewHostInfoFromAddress(host))
	}
	event := &membership.ChangedEvent{
		HostsAdded: hostInfos,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.hostInfos = hostInfos

	for _, ch := range s.listeners {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *staticResolver) Lookup(key string) (membership.HostInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hostInfos) == 0 {
		return nil, membership.ErrInsufficientHosts
	}
	hash := int(s.hashfunc([]byte(key)))
	idx := hash % len(s.hostInfos)
	return s.hostInfos[idx], nil
}

func (s *staticResolver) LookupN(key string, _ int) []membership.HostInfo {
	info, err := s.Lookup(key)
	if err != nil {
		return []membership.HostInfo{}
	}
	return []membership.HostInfo{info}
}

func (s *staticResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.listeners[name]
	if ok {
		return membership.ErrListenerAlreadyExist
	}
	s.listeners[name] = notifyChannel
	return nil
}

func (s *staticResolver) RemoveListener(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.listeners[name]
	if !ok {
		return nil
	}
	delete(s.listeners, name)
	return nil
}

func (s *staticResolver) MemberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hostInfos)
}

func (s *staticResolver) AvailableMemberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hostInfos)
}

func (s *staticResolver) Members() []membership.HostInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hostInfos
}

func (s *staticResolver) AvailableMembers() []membership.HostInfo {
	return s.Members()
}

func (s *staticResolver) RequestRefresh() {
}
