package ringpop

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	rpmembership "github.com/temporalio/ringpop-go/membership"
	"go.temporal.io/server/common/util"
)

func TestSwitchTime(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	end := start.Add(30 * time.Second)

	require.Equal(t, start, switchTime(start, start, 12345))
	require.Equal(t, start, switchTime(start, start.Add(-time.Second), 12345))
	require.Equal(t, end, switchTime(start, end, math.MaxUint32))
	require.Equal(t, start.Add(6), switchTime(start, end, 0)) // 30s / 2^32 rounded down
	mid := switchTime(start, end, math.MaxUint32/2)
	require.InDelta(t, 15*time.Second, mid.Sub(start), float64(time.Millisecond))
}

func TestPresence(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	end := start.Add(30 * time.Second)
	const hash = math.MaxUint32 / 4 // switches at 7.5s
	sw := switchTime(start, end, hash)

	join := presence{joinStart: start, joinEnd: end}
	require.False(t, join.present(hash, start))
	require.False(t, join.present(hash, sw.Add(-1)))
	require.True(t, join.present(hash, sw))
	require.True(t, join.present(hash, end))

	leave := presence{leaveStart: start, leaveEnd: end}
	require.True(t, leave.present(hash, start))
	require.True(t, leave.present(hash, sw.Add(-1)))
	require.False(t, leave.present(hash, sw))
	require.False(t, leave.present(hash, end))

	// zero-length windows behave like plain startAt/stopAt
	join = presence{joinStart: start, joinEnd: start}
	require.False(t, join.present(hash, start.Add(-1)))
	require.True(t, join.present(hash, start))
	leave = presence{leaveStart: start, leaveEnd: start}
	require.True(t, leave.present(hash, start.Add(-1)))
	require.False(t, leave.present(hash, start))
}

func TestFilterMembersByTime(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	unix := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }

	hosts := []*hostInfo{
		newHostInfo("plain", nil),
		newHostInfo("future-start", map[string]string{startAtKey: unix(5 * time.Second)}),
		newHostInfo("past-start", map[string]string{startAtKey: unix(-5 * time.Second)}),
		newHostInfo("future-stop", map[string]string{stopAtKey: unix(3 * time.Second)}),
		newHostInfo("past-stop", map[string]string{stopAtKey: unix(-3 * time.Second)}),
		newHostInfo("joining", map[string]string{startAtKey: unix(-10 * time.Second), startWindowKey: "30"}),
		newHostInfo("joined", map[string]string{startAtKey: unix(-40 * time.Second), startWindowKey: "30"}),
		newHostInfo("future-join", map[string]string{startAtKey: unix(2 * time.Second), startWindowKey: "30"}),
		newHostInfo("leaving", map[string]string{stopAtKey: unix(-10 * time.Second), stopWindowKey: "40"}),
		newHostInfo("left", map[string]string{stopAtKey: unix(-40 * time.Second), stopWindowKey: "30"}),
		newHostInfo("future-leave", map[string]string{stopAtKey: unix(4 * time.Second), stopWindowKey: "20"}),
		newHostInfo("huge-window", map[string]string{startAtKey: unix(-1 * time.Second), startWindowKey: "999999999999"}),
	}

	filtered, presences, gradualUntil, nextEvent := filterMembersByTime(hosts, now)

	addrs := util.MapSlice(filtered, (*hostInfo).GetAddress)
	require.ElementsMatch(t, []string{
		"plain", "past-start", "future-stop", "joining", "joined", "leaving", "future-leave", "huge-window",
	}, addrs)

	require.Equal(t, map[string]*presence{
		"future-stop":  {leaveStart: now.Add(3 * time.Second), leaveEnd: now.Add(3 * time.Second)},
		"joining":      {joinStart: now.Add(-10 * time.Second), joinEnd: now.Add(20 * time.Second)},
		"leaving":      {leaveStart: now.Add(-10 * time.Second), leaveEnd: now.Add(30 * time.Second)},
		"future-leave": {leaveStart: now.Add(4 * time.Second), leaveEnd: now.Add(24 * time.Second)},
		"huge-window":  {joinStart: now.Add(-1 * time.Second), joinEnd: now.Add(-1*time.Second + maxGradualChangeWindow)},
	}, presences)

	require.Equal(t, now.Add(-1*time.Second+maxGradualChangeWindow), gradualUntil)
	require.Equal(t, now.Add(2*time.Second).Unix(), nextEvent) // future-join start
}

func TestFilterMembersByTimeNoChange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	hosts := []*hostInfo{newHostInfo("a", nil), newHostInfo("b", nil)}
	filtered, presences, gradualUntil, nextEvent := filterMembersByTime(hosts, now)
	require.Len(t, filtered, 2)
	require.Nil(t, presences)
	require.True(t, gradualUntil.IsZero())
	require.Zero(t, nextEvent)
}

// gradualTestCluster builds a resolver as it would look after a refresh at refreshTime.
func gradualTestResolver(hosts []*hostInfo, refreshTime time.Time) *serviceResolver {
	hosts, presences, gradualUntil, _ := filterMembersByTime(hosts, refreshTime)
	ring := newHashRing(100)
	ring.AddMembers(util.MapSlice(hosts, func(h *hostInfo) rpmembership.Member { return h })...)
	hostMap := make(map[string]*hostInfo)
	for _, h := range hosts {
		hostMap[h.GetAddress()] = h
	}
	r := &serviceResolver{}
	r.ringAndHosts.Store(ringAndHosts{
		ring:         ring,
		hosts:        hostMap,
		presence:     presences,
		gradualUntil: gradualUntil,
	})
	return r
}

func gradualTestHosts(n int) []*hostInfo {
	hosts := make([]*hostInfo, n)
	for i := range hosts {
		hosts[i] = newHostInfo(fmt.Sprintf("10.0.0.%d:7235", i), nil)
	}
	return hosts
}

func lookupOne(t *testing.T, r *serviceResolver, key string, now time.Time) string {
	addrs, _ := r.lookupN(key, 1, now)
	require.Len(t, addrs, 1)
	// LookupN must be an extension of Lookup
	addrs3, _ := r.lookupN(key, 3, now)
	require.Equal(t, addrs[0], addrs3[0])
	return addrs[0]
}

func TestGradualJoin(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	const window = 30 * time.Second
	end := start.Add(window)

	base := gradualTestHosts(10)
	joiner := newHostInfo("10.0.0.99:7235", map[string]string{
		startAtKey:     strconv.FormatInt(start.Unix(), 10),
		startWindowKey: strconv.Itoa(int(window / time.Second)),
	})
	before := gradualTestResolver(base, start)
	after := gradualTestResolver(append(base, newHostInfo(joiner.addr, nil)), start)
	r := gradualTestResolver(append(base, joiner), start)
	require.Equal(t, end, r.GradualChangeUntil())

	const keys = 20000
	const buckets = 30
	var counts [buckets]int
	moved := 0
	for i := range keys {
		key := fmt.Sprintf("key-%d", i)
		oldOwner := lookupOne(t, before, key, start)
		newOwner := lookupOne(t, after, key, start)
		require.Equal(t, oldOwner, lookupOne(t, r, key, start), "nothing moves at the start")
		require.Equal(t, newOwner, lookupOne(t, r, key, end), "everything moves by the end")
		if oldOwner == newOwner {
			// key not affected by joiner, check a few times in the middle
			for _, d := range []time.Duration{window / 4, window / 2, 3 * window / 4} {
				require.Equal(t, oldOwner, lookupOne(t, r, key, start.Add(d)))
			}
			continue
		}
		require.Equal(t, joiner.addr, newOwner)
		moved++
		sw := switchTime(start, end, gradualKeyHash(key))
		require.Equal(t, oldOwner, lookupOne(t, r, key, sw.Add(-1)))
		require.Equal(t, newOwner, lookupOne(t, r, key, sw))
		counts[int(sw.Sub(start)*buckets/window)%buckets]++
	}

	// about 1/11 of keys should move, spread evenly over the window
	require.InDelta(t, keys/11, moved, keys/50)
	mean := float64(moved) / buckets
	for i, c := range counts {
		require.Greater(t, float64(c), mean/2, "bucket %d", i)
		require.Less(t, float64(c), mean*2, "bucket %d", i)
	}
}

func TestGradualLeaveAndJoinTogether(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	const window = 30 * time.Second
	end := start.Add(window)
	windowLabel := strconv.Itoa(int(window / time.Second))
	startLabel := strconv.FormatInt(start.Unix(), 10)

	// Simulate a rolling deploy step: one host leaves while a new one joins with the same
	// window. Each key should move at most once, directly to its final owner.
	base := gradualTestHosts(10)
	leaver := newHostInfo(base[3].addr, map[string]string{stopAtKey: startLabel, stopWindowKey: windowLabel})
	joiner := newHostInfo("10.0.0.99:7235", map[string]string{startAtKey: startLabel, startWindowKey: windowLabel})

	before := gradualTestResolver(base, start)
	afterHosts := append(gradualTestHosts(10), newHostInfo(joiner.addr, nil))
	afterHosts = append(afterHosts[:3], afterHosts[4:]...)
	after := gradualTestResolver(afterHosts, start)

	hosts := append(gradualTestHosts(10), joiner)
	hosts[3] = leaver
	r := gradualTestResolver(hosts, start)
	require.Equal(t, end, r.GradualChangeUntil())

	moved := 0
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		oldOwner := lookupOne(t, before, key, start)
		newOwner := lookupOne(t, after, key, start)
		require.Equal(t, oldOwner, lookupOne(t, r, key, start))
		require.Equal(t, newOwner, lookupOne(t, r, key, end))
		if oldOwner == newOwner {
			continue
		}
		moved++
		sw := switchTime(start, end, gradualKeyHash(key))
		require.Equal(t, oldOwner, lookupOne(t, r, key, sw.Add(-1)))
		require.Equal(t, newOwner, lookupOne(t, r, key, sw))
		for _, d := range []time.Duration{window / 4, window / 2, 3 * window / 4} {
			owner := lookupOne(t, r, key, start.Add(d))
			require.Contains(t, []string{oldOwner, newOwner}, owner, "key should move only once")
		}
	}
	require.Positive(t, moved)
}

func TestGradualLeaveAll(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	labels := map[string]string{stopAtKey: strconv.FormatInt(start.Unix(), 10), stopWindowKey: "30"}
	r := gradualTestResolver([]*hostInfo{newHostInfo("a:1", labels)}, start)

	addrs, _ := r.lookupN("key", 1, start)
	require.Equal(t, []string{"a:1"}, addrs)
	addrs, _ = r.lookupN("key", 1, start.Add(30*time.Second))
	require.Empty(t, addrs)
}
