package matching

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/tqid"
)

func TestTQLoadBalancerMapping(t *testing.T) {
	lb := &defaultLoadBalancer{
		lock:         sync.RWMutex{},
		taskQueueLBs: make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)

	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	tqlb := lb.getTaskQueueLoadBalancer(taskQueue)

	tqlb2 := lb.getTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	assert.Equal(t, tqlb2, tqlb, "mapping should be based on content, not the pointer value")

	taskQueueClone := *taskQueue
	tqlb2 = lb.getTaskQueueLoadBalancer(&taskQueueClone)
	assert.Equal(t, tqlb2, tqlb, "mapping should be based on content, not the pointer value")

	tqlb3 := lb.getTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	assert.NotEqual(t, tqlb3, tqlb, "separate load LB should be created for each task type")
}

func TestTQLoadBalancer(t *testing.T) {
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// pick 4 times, each partition picked would have one poller
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	p3 := tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))

	// release one, and pick one, the newly picked one should have one poller
	p3.Release()
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))

	// pick one again, this time it should have 2 pollers
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 2, maxPollerCount(tqlb))
}

func TestTQLoadBalancerForce(t *testing.T) {
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// pick 4 times, each partition picked would have one poller
	p1 := tqlb.forceReadPartition(partitionCount, 1)
	assert.Equal(t, 1, p1.TQPartition.PartitionId())
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.forceReadPartition(partitionCount, 1)
	assert.Equal(t, 2, maxPollerCount(tqlb))

	// when we don't force it should balance out
	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 2, maxPollerCount(tqlb))

	// releasing the forced one and adding another should still be balanced
	p1.Release()
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 2, maxPollerCount(tqlb))

	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 3, maxPollerCount(tqlb))
}

func TestLoadBalancerConcurrent(t *testing.T) {
	wg := &sync.WaitGroup{}
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	concurrentCount := 10 * partitionCount
	wg.Add(concurrentCount)
	for range concurrentCount {
		go func() {
			defer wg.Done()
			tqlb.pickReadPartition(partitionCount, nil, 0)
		}()
	}
	wg.Wait()

	// verify all partition have same pollers.
	assert.Equal(t, 10, tqlb.pollerCounts[0])
	assert.Equal(t, 10, tqlb.pollerCounts[1])
	assert.Equal(t, 10, tqlb.pollerCounts[2])
	assert.Equal(t, 10, tqlb.pollerCounts[3])
}

func TestLoadBalancer_ReducedPartitionCount(t *testing.T) {
	partitionCount := 2
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	p1 := tqlb.pickReadPartition(partitionCount, nil, 0)
	p2 := tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))

	partitionCount += 2 // increase partition count
	p3 := tqlb.pickReadPartition(partitionCount, nil, 0)
	p4 := tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))

	partitionCount -= 2 // reduce partition count
	p5 := tqlb.pickReadPartition(partitionCount, nil, 0)
	p6 := tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 2, maxPollerCount(tqlb))
	assert.Equal(t, 2, maxPollerCount(tqlb))
	p7 := tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 3, maxPollerCount(tqlb))

	// release all of them and it should be ok.
	p1.Release()
	p2.Release()
	p3.Release()
	p4.Release()
	p5.Release()
	p6.Release()
	p7.Release()

	tqlb.pickReadPartition(partitionCount, nil, 0)
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount, nil, 0)
	assert.Equal(t, 2, maxPollerCount(tqlb))
}

func TestHasCompleteAndPositiveBacklog(t *testing.T) {
	positive := number.EncodeCompact8(32)
	require.False(t, hasCompleteAndPositiveBacklog(4, nil))
	require.False(t, hasCompleteAndPositiveBacklog(4, []number.Compact8{0, 0, positive}))
	require.False(t, hasCompleteAndPositiveBacklog(4, []number.Compact8{0, 0, 0, 0, positive}))
	require.True(t, hasCompleteAndPositiveBacklog(4, []number.Compact8{0, 0, 0, positive}))
}

func TestTQLoadBalancerBacklogCapEnablesWeightedReads(t *testing.T) {
	f := newTestTaskQueueFamily(t)
	backlogCounts := []number.Compact8{0, 0, 0, number.EncodeCompact8(1000)}

	t.Run("without backlog cap", func(t *testing.T) {
		tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
		for range 8 {
			tqlb.pickReadPartition(4, backlogCounts, 0)
		}
		require.Equal(t, []int{2, 2, 2, 2}, tqlb.pollerCounts)
	})

	t.Run("backlog weighted", func(t *testing.T) {
		tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
		for range 1000 {
			tqlb.pickReadPartition(4, backlogCounts, number.EncodeCompact8(32))
		}
		require.Greater(t, tqlb.pollerCounts[3], 850)
	})
}

func TestTQLoadBalancerWeighted_RapidlyReturningPollsFollowBacklog(t *testing.T) {
	f := newTestTaskQueueFamily(t)

	largeBacklogCounts := make([]number.Compact8, 32)
	for partitionID := range largeBacklogCounts {
		if partitionID%2 == 0 {
			largeBacklogCounts[partitionID] = number.EncodeCompact8(32)
		} else {
			largeBacklogCounts[partitionID] = number.EncodeCompact8(128)
		}
	}
	testCases := map[string][]number.Compact8{
		"two partitions": {
			number.EncodeCompact8(32),
			number.EncodeCompact8(128),
		},
		"four partitions": {
			number.EncodeCompact8(32),
			number.EncodeCompact8(64),
			number.EncodeCompact8(96),
			number.EncodeCompact8(128),
		},
		"32 partitions": largeBacklogCounts,
	}
	type pendingPoll struct {
		token          *pollToken
		remainingPicks int
	}

	const pollCount = 100_000
	for name, backlogCounts := range testCases {
		t.Run(name, func(t *testing.T) {
			tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
			pickCounts := make([]int, len(backlogCounts))
			pending := make([]pendingPoll, 0, 2)
			rng := rand.New(rand.NewSource(0))
			for range pollCount {
				token := tqlb.pickReadPartition(len(backlogCounts), backlogCounts, number.EncodeCompact8(32))
				pickCounts[token.TQPartition.PartitionId()]++

				stillPending := pending[:0]
				for _, poll := range pending {
					poll.remainingPicks--
					if poll.remainingPicks == 0 {
						poll.token.Release()
					} else {
						stillPending = append(stillPending, poll)
					}
				}
				pending = stillPending

				remainingPicks := rng.Intn(3)
				if remainingPicks == 0 {
					token.Release()
				} else {
					pending = append(pending, pendingPoll{token: token, remainingPicks: remainingPicks})
				}
			}
			for _, poll := range pending {
				poll.token.Release()
			}

			var totalWeight int64
			for partitionID := range backlogCounts {
				totalWeight += readPartitionWeight(backlogCounts, partitionID)
			}
			for partitionID := range backlogCounts {
				expected := float64(pollCount) * float64(readPartitionWeight(backlogCounts, partitionID)) / float64(totalWeight)
				require.InDelta(t, expected, pickCounts[partitionID], pollCount*0.005,
					"partition %d picks should follow its backlog weight", partitionID)
			}
			for _, pollerCount := range tqlb.pollerCounts {
				require.Zero(t, pollerCount)
			}
		})
	}
}

func TestTQLoadBalancerWeighted_LongRunningPollsRetainCoverage(t *testing.T) {
	f := newTestTaskQueueFamily(t)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	backlogCounts := []number.Compact8{
		number.EncodeCompact8(32),
		number.EncodeCompact8(64),
		number.EncodeCompact8(96),
		number.EncodeCompact8(128),
	}
	const pollCount = 10_000
	tokens := make([]*pollToken, 0, pollCount)
	defer func() {
		for _, token := range tokens {
			token.Release()
		}
	}()

	for range pollCount {
		tokens = append(tokens, tqlb.pickReadPartition(len(backlogCounts), backlogCounts, number.EncodeCompact8(32)))
	}

	var totalWeight int64
	for partitionID := range backlogCounts {
		totalWeight += readPartitionWeight(backlogCounts, partitionID)
	}
	for partitionID, pollerCount := range tqlb.pollerCounts {
		expected := float64(pollCount) * float64(readPartitionWeight(backlogCounts, partitionID)) / float64(totalWeight)
		require.InDelta(t, expected, pollerCount, pollCount*0.02,
			"partition %d outstanding polls should follow its backlog weight", partitionID)
		require.Positive(t, pollerCount, "partition %d should retain poll coverage", partitionID)
	}
}

func TestPickReadPartition_NoBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	require.NoError(t, err)
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// no backlog counts -> classic fewest-poller balancing across the 4 read partitions.
	pc := PartitionCounts{Read: 4, Write: 4}
	for range 8 {
		lb.PickReadPartition(tq, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(tq)
	for _, c := range tqlb.pollerCounts {
		require.Equal(t, 2, c)
	}
}

func TestPickReadPartition_IncompleteBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	require.NoError(t, err)
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// Backlog counts shorter than the read count can occur while a scale-up is propagating.
	pc := PartitionCounts{
		Read:         4,
		Write:        4,
		BacklogCap:   number.EncodeCompact8(32),
		BacklogCount: []number.Compact8{0, 100},
	}
	for range 8 {
		lb.PickReadPartition(tq, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(tq)
	require.Equal(t, []int{2, 2, 2, 2}, tqlb.pollerCounts)
}

func TestPickWritePartition_BacklogAware(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	require.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	backlogCap := number.DecodeCompact8(number.EncodeCompact8(21_000_000))

	// Both partitions are below the cap: partition 0 is empty (gap ~21M) and partition 1 is
	// partially full (~12.6M, gap ~8.4M). Writes should favor the emptier partition 0
	// in proportion to the gaps, while partition 1 still receives a meaningful share.
	pc := PartitionCounts{
		Read:         2,
		Write:        2,
		BacklogCap:   200,
		BacklogCount: []number.Compact8{0, number.EncodeCompact8(13_000_000)},
	}
	counts := make([]int, 2)
	const n = 3000
	for range n {
		p := lb.PickWritePartition(taskQueue, pc)
		counts[p.PartitionId()]++
	}
	require.Greater(t, counts[0], counts[1], "emptier partition should receive more writes")
	require.Positive(t, counts[1], "the below-cap partition should still receive some writes")
	gap0 := backlogCap - number.DecodeCompact8(0)
	gap1 := backlogCap - number.DecodeCompact8(number.EncodeCompact8(13_000_000))
	require.InDelta(t, float64(n)*float64(gap0)/float64(gap0+gap1), counts[0], float64(n)*0.05,
		"writes split in proportion to each partition's gap to cap")

	// Now, every partition at/above cap -> no gap to weight by, so the picker declines and the caller
	// falls back to uniform random.
	pcAtCap := PartitionCounts{
		Read:         2,
		Write:        2,
		BacklogCap:   200,
		BacklogCount: []number.Compact8{200, 200},
	}
	atCap := make([]int, 2)
	for range n {
		p := lb.PickWritePartition(taskQueue, pcAtCap)
		atCap[p.PartitionId()]++
	}
	for i := range atCap {
		require.InDelta(t, n/2, atCap[i], float64(n)*0.1, "at-cap partition %d roughly uniform", i)
	}
}

func TestPickWritePartition_NoBacklogUniform(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	require.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// no backlog cap -> uniform random across the 4 write partitions.
	pc := PartitionCounts{Read: 4, Write: 4}
	counts := make([]int, 4)
	const n = 4000
	for range n {
		p := lb.PickWritePartition(taskQueue, pc)
		counts[p.PartitionId()]++
	}
	for i := range counts {
		require.InDelta(t, n/4, counts[i], float64(n)*0.1, "partition %d roughly uniform", i)
	}
}

func maxPollerCount(tqlb *tqLoadBalancer) int {
	res := -1
	for _, c := range tqlb.pollerCounts {
		if c > res {
			res = c
		}
	}
	return res
}

func newTestTaskQueueFamily(t *testing.T) *tqid.TaskQueueFamily {
	t.Helper()
	f, err := tqid.NewTaskQueueFamily(t.Name()+"-namespace", t.Name()+"-task-queue")
	require.NoError(t, err)
	return f
}
