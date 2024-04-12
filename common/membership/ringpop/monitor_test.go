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

package ringpop

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/util"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RpoSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestRpoSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(RpoSuite))
}

func (s *RpoSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RpoSuite) TestMonitor() {
	serviceName := primitives.HistoryService
	testService := newTestCluster(s.T(), "rpm-test", 3, "127.0.0.1", "", serviceName, "127.0.0.1", nil, true)
	s.NotNil(testService, "Failed to create test service")

	rpm := testService.rings[0]

	time.Sleep(time.Second)

	listenCh := make(chan *membership.ChangedEvent, 5)
	r, err := rpm.GetResolver(serviceName)
	s.NoError(err)
	err = r.AddListener("test-listener", listenCh)
	s.Nil(err, "AddListener failed")

	host, err := r.Lookup("key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotNil(host, "Ringpop monitor returned a nil host")

	s.Eventually(func() bool {
		return len(r.Members()) == 3 && len(r.AvailableMembers()) == 3
	}, 10*time.Second, 100*time.Millisecond)

	// Force refresh now and drain the notification channel
	resolver, _ := rpm.GetResolver(serviceName)
	s.NoError(resolver.(*serviceResolver).refresh(refreshModeAlways))
	drainChannel(listenCh)

	s.T().Log("Killing host 1")
	testService.KillHost(testService.hostUUIDs[1])

	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	select {
	case e := <-listenCh:
		s.T().Log("Got update")
		s.Equal(1, len(e.HostsRemoved), "ringpop monitor event does not report the removed host")
		s.Equal(testService.hostAddrs[1], e.HostsRemoved[0].GetAddress(), "ringpop monitor reported that a wrong host was removed")
		s.Nil(e.HostsAdded, "Unexpected host reported to be added by ringpop monitor")
		s.Nil(e.HostsChanged, "Unexpected host reported to be changed by ringpop monitor")
	case <-timer.C:
		s.Fail("Timed out waiting for failure to be detected by ringpop")
	}

	for k := 0; k < 10; k++ {
		host, err = r.Lookup(fmt.Sprintf("key%d", k))
		s.Nil(err, "Ringpop monitor failed to find host for key")
		s.NotEqual(testService.hostAddrs[1], host.GetAddress(), "Ringpop monitor assigned key to dead host")
	}
	s.Equal(2, len(r.Members()))
	s.Equal(2, len(r.AvailableMembers()))

	s.T().Log("Draining host 2")
	testService.DrainHost(testService.hostUUIDs[2])

	select {
	case e := <-listenCh:
		s.T().Log("Got update")
		s.Nil(e.HostsRemoved)
		s.Nil(e.HostsAdded)
		s.Equal(1, len(e.HostsChanged))
		s.Equal(testService.hostAddrs[2], e.HostsChanged[0].GetAddress())
	case <-timer.C:
		s.Fail("Timed out waiting for failure to be detected by ringpop")
	}

	s.Equal(2, len(r.Members()))
	s.Equal(1, len(r.AvailableMembers()))

	err = r.RemoveListener("test-listener")
	s.Nil(err, "RemoveListener() failed")

	testService.Stop()
}

func (s *RpoSuite) TestScheduledUpdates() {
	serviceName := primitives.MatchingService
	// note these are only accurate to the start of the second, so they're effectively rounded
	// down if we start mid-second.
	start := time.Now()
	joinTimes := []time.Time{
		start.Add(2 * time.Second),
		time.Time{},
		start.Add(4 * time.Second),
	}
	testService := newTestCluster(s.T(), "rpm-test", 3, "127.0.0.1", "", serviceName, "127.0.0.1", joinTimes, false)
	s.NotNil(testService, "Failed to create test service")

	observer := rand.Intn(3)
	r, err := testService.rings[observer].GetResolver(serviceName)
	s.NoError(err)

	waitAndCheckMembers := func(elements []string) {
		var addrs []string
		s.Eventually(func() bool {
			addrs = util.MapSlice(r.Members(), func(h membership.HostInfo) string { return h.GetAddress() })
			return len(addrs) == len(elements)
		}, 15*time.Second, 100*time.Millisecond)
		s.ElementsMatch(elements, addrs)
	}

	// we should see only 1 first, then 0,1, then 0,1,2
	waitAndCheckMembers([]string{testService.hostAddrs[1]})

	waitAndCheckMembers([]string{testService.hostAddrs[0], testService.hostAddrs[1]})
	s.Greater(time.Since(start), 1*time.Second)

	waitAndCheckMembers([]string{testService.hostAddrs[0], testService.hostAddrs[1], testService.hostAddrs[2]})
	s.Greater(time.Since(start), 3*time.Second)

	// now remove two at scheduled times. we should see 1 disappear then 0.
	observer = rand.Intn(3)
	r, err = testService.rings[observer].GetResolver(serviceName)
	s.NoError(err)

	start = time.Now()
	_, err = testService.rings[1].EvictSelfAt(start.Add(2 * time.Second))
	s.NoError(err)
	_, err = testService.rings[0].EvictSelfAt(start.Add(4 * time.Second))
	s.NoError(err)

	waitAndCheckMembers([]string{testService.hostAddrs[0], testService.hostAddrs[2]})
	s.Greater(time.Since(start), 1*time.Second)

	waitAndCheckMembers([]string{testService.hostAddrs[2]})
	s.Greater(time.Since(start), 3*time.Second)

	testService.Stop()
}

func (s *RpoSuite) TestCompareMembers() {
	s.verifyMemberDiff([]string{}, []string{"a"}, []string{"+a"})
	s.verifyMemberDiff([]string{}, []string{"a", "b"}, []string{"+a", "+b"})
	s.verifyMemberDiff([]string{"a"}, []string{"a", "b"}, []string{"+b"})
	s.verifyMemberDiff([]string{}, []string{}, nil)
	s.verifyMemberDiff([]string{"a"}, []string{"a"}, nil)
	s.verifyMemberDiff([]string{"a"}, []string{}, []string{"-a"})
	s.verifyMemberDiff([]string{"a", "b"}, []string{}, []string{"-a", "-b"})
	s.verifyMemberDiff([]string{"a", "b"}, []string{"a", "b"}, nil)
	s.verifyMemberDiff([]string{"a", "b", "c"}, []string{"b", "d", "e"}, []string{"+d", "+e", "-a", "-c"})
}

func (s *RpoSuite) verifyMemberDiff(curr []string, new []string, expectedDiff []string) {
	resolver := &serviceResolver{}
	currMembers := make(map[string]*hostInfo, len(curr))
	for _, m := range curr {
		currMembers[m] = newHostInfo(m, nil)
	}
	resolver.ringAndHosts.Store(ringAndHosts{
		hosts: currMembers,
	})
	newHosts := util.MapSlice(new, func(addr string) *hostInfo { return newHostInfo(addr, nil) })
	newMembers, event := resolver.compareMembers(newHosts)
	s.ElementsMatch(new, maps.Keys(newMembers))
	s.Equal(expectedDiff != nil, event != nil)
	if event != nil {
		s.ElementsMatch(expectedDiff, eventToString(event))
	}
}

func (s *RpoSuite) TestCompareMembersWithDraining() {
	resolver := &serviceResolver{}
	currMembers := map[string]*hostInfo{
		"a": newHostInfo("a", nil),
		"b": newHostInfo("b", nil),
		"c": newHostInfo("c", nil),
	}
	resolver.ringAndHosts.Store(ringAndHosts{
		hosts: currMembers,
	})
	newMembers, event := resolver.compareMembers([]*hostInfo{
		newHostInfo("a", nil),
		newHostInfo("b", map[string]string{drainingKey: "true"}),
		newHostInfo("c", nil),
	})
	s.NotNil(newMembers)
	s.Equal(map[string]string{drainingKey: "true"}, newMembers["b"].labels)
	s.Equal("b[draining=true]", newMembers["b"].summary())
	s.NotNil(event)
	s.ElementsMatch([]string{"~b"}, eventToString(event))

	resolver.ringAndHosts.Store(ringAndHosts{
		hosts: newMembers,
	})
	s.Equal(3, len(resolver.Members()))
	s.Equal(2, len(resolver.AvailableMembers()))
}

func eventToString(event *membership.ChangedEvent) []string {
	var diff []string
	for _, a := range event.HostsAdded {
		diff = append(diff, "+"+a.GetAddress())
	}
	for _, a := range event.HostsRemoved {
		diff = append(diff, "-"+a.GetAddress())
	}
	for _, a := range event.HostsChanged {
		diff = append(diff, "~"+a.GetAddress())
	}
	return diff
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
