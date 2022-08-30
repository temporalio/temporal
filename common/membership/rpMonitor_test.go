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

package membership

import (
	"testing"
	"time"

	"go.temporal.io/server/common/primitives"
	"golang.org/x/exp/maps"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RpoSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestRpoSuite(t *testing.T) {
	suite.Run(t, new(RpoSuite))
}

func (s *RpoSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RpoSuite) TestRingpopMonitor() {
	serviceName := primitives.HistoryService
	testService := NewTestRingpopCluster(s.T(), "rpm-test", 3, "0.0.0.0", "", serviceName, "127.0.0.1")
	s.NotNil(testService, "Failed to create test service")

	rpm := testService.rings[0]

	time.Sleep(time.Second)

	listenCh := make(chan *ChangedEvent, 5)
	err := rpm.AddListener(serviceName, "test-listener", listenCh)
	s.Nil(err, "AddListener failed")

	host, err := rpm.Lookup(serviceName, "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotNil(host, "Ringpop monitor returned a nil host")

	// Force refresh now and drain the notification channel
	resolver, _ := rpm.GetResolver(serviceName)
	resolver.(*ringpopServiceResolver).refresh()
	drainChannel(listenCh)

	s.T().Log("Killing host 1")
	testService.KillHost(testService.hostUUIDs[1])

	select {
	case e := <-listenCh:
		s.Equal(1, len(e.HostsRemoved), "ringpop monitor event does not report the removed host")
		s.Equal(testService.hostAddrs[1], e.HostsRemoved[0].GetAddress(), "ringpop monitor reported that a wrong host was removed")
		s.Nil(e.HostsAdded, "Unexpected host reported to be added by ringpop monitor")
		s.Nil(e.HostsUpdated, "Unexpected host reported to be updated by ringpop monitor")
	case <-time.After(time.Minute):
		s.Fail("Timed out waiting for failure to be detected by ringpop")
	}

	host, err = rpm.Lookup(serviceName, "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotEqual(testService.hostAddrs[1], host.GetAddress(), "Ringpop monitor assigned key to dead host")

	err = rpm.RemoveListener(serviceName, "test-listener")
	s.Nil(err, "RemoveListener() failed")

	rpm.Stop()
	testService.Stop()
}

func (s *RpoSuite) TestCompareMembers() {
	s.testCompareMembers([]string{}, []string{"a"}, []string{"+a"})
	s.testCompareMembers([]string{}, []string{"a", "b"}, []string{"+a", "+b"})
	s.testCompareMembers([]string{"a"}, []string{"a", "b"}, []string{"+b"})
	s.testCompareMembers([]string{}, []string{}, nil)
	s.testCompareMembers([]string{"a"}, []string{"a"}, nil)
	s.testCompareMembers([]string{"a"}, []string{}, []string{"-a"})
	s.testCompareMembers([]string{"a", "b"}, []string{}, []string{"-a", "-b"})
	s.testCompareMembers([]string{"a", "b"}, []string{"a", "b"}, nil)
	s.testCompareMembers([]string{"a", "b", "c"}, []string{"b", "d", "e"}, []string{"+d", "+e", "-a", "-c"})
}

func (s *RpoSuite) testCompareMembers(curr []string, new []string, expectedDiff []string) {
	resolver := &ringpopServiceResolver{}
	currMembers := make(map[string]struct{}, len(curr))
	for _, m := range curr {
		currMembers[m] = struct{}{}
	}
	resolver.membersMap = currMembers
	newMembers, event := resolver.compareMembers(new)
	s.ElementsMatch(new, maps.Keys(newMembers))
	s.Equal(expectedDiff != nil, event != nil)
	if event != nil {
		var diff []string
		for _, a := range event.HostsAdded {
			diff = append(diff, "+"+a.addr)
		}
		for _, a := range event.HostsUpdated {
			diff = append(diff, "~"+a.addr)
		}
		for _, a := range event.HostsRemoved {
			diff = append(diff, "-"+a.addr)
		}
		s.ElementsMatch(expectedDiff, diff)
	}
}

func drainChannel[T any](ch <-chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
