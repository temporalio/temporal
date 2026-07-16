package matching

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	p3 := tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))

	// release one, and pick one, the newly picked one should have one poller
	p3.Release()
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))

	// pick one again, this time it should have 2 pollers
	tqlb.pickReadPartition(partitionCount)
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
	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 2, maxPollerCount(tqlb))

	// releasing the forced one and adding another should still be balanced
	p1.Release()
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 2, maxPollerCount(tqlb))

	tqlb.pickReadPartition(partitionCount)
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
			tqlb.pickReadPartition(partitionCount)
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
	p1 := tqlb.pickReadPartition(partitionCount)
	p2 := tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))

	partitionCount += 2 // increase partition count
	p3 := tqlb.pickReadPartition(partitionCount)
	p4 := tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))

	partitionCount -= 2 // reduce partition count
	p5 := tqlb.pickReadPartition(partitionCount)
	p6 := tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 2, maxPollerCount(tqlb))
	assert.Equal(t, 2, maxPollerCount(tqlb))
	p7 := tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 3, maxPollerCount(tqlb))

	// release all of them and it should be ok.
	p1.Release()
	p2.Release()
	p3.Release()
	p4.Release()
	p5.Release()
	p6.Release()
	p7.Release()

	tqlb.pickReadPartition(partitionCount)
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 1, maxPollerCount(tqlb))
	assert.Equal(t, 1, maxPollerCount(tqlb))
	tqlb.pickReadPartition(partitionCount)
	assert.Equal(t, 2, maxPollerCount(tqlb))
}

func TestTQLoadBalancerWeighted_ConcentratesOnBacklog(t *testing.T) {
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// partition 3 has ~9x the weight of the others (i.e. much more backlog).
	weights := []int64{100, 100, 100, 900}
	const n = 1200
	for range n {
		tqlb.pickReadPartitionWeighted(partitionCount, weights)
	}

	counts := tqlb.pollerCounts
	// the high-backlog partition gets the most pollers, by a wide margin.
	assert.Greater(t, counts[3], counts[0])
	assert.Greater(t, counts[3], counts[1])
	assert.Greater(t, counts[3], counts[2])
	// empty partitions still get some pollers (weight floor prevents starvation).
	assert.Greater(t, counts[0], 0)
	// distribution is roughly proportional to weights (sum 1200).
	assert.InDelta(t, 900, counts[3], 60)
	assert.InDelta(t, 100, counts[0], 30)
	assert.InDelta(t, 100, counts[1], 30)
	assert.InDelta(t, 100, counts[2], 30)
}

func TestTQLoadBalancerWeighted_EqualWeightsBalances(t *testing.T) {
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// equal weights should behave like fewest-outstanding-pollers: perfectly balanced.
	weights := []int64{100, 100, 100, 100}
	for range 8 {
		tqlb.pickReadPartitionWeighted(partitionCount, weights)
	}
	for _, c := range tqlb.pollerCounts {
		assert.Equal(t, 2, c)
	}
}

func TestTQLoadBalancerWeighted_FillsEachBeforeDoubling(t *testing.T) {
	partitionCount := 4
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// Even with extremely skewed weights, every partition must receive its first poller before
	// any partition receives a second: a zero-poller partition always has the minimum
	// poller/weight ratio (0), so it wins regardless of weight. A naive weighted-random picker
	// would instead pile onto partition 3 immediately.
	weights := []int64{100, 100, 100, 100000}

	// During the first partitionCount picks, no partition should ever exceed one poller.
	for range partitionCount {
		tqlb.pickReadPartitionWeighted(partitionCount, weights)
		assert.LessOrEqual(t, maxPollerCount(tqlb), 1,
			"no partition should get a 2nd poller until every partition has 1")
	}
	// After exactly partitionCount picks, every partition has exactly one poller.
	for i, c := range tqlb.pollerCounts {
		assert.Equalf(t, 1, c, "partition %d should have exactly one poller after the first round", i)
	}

	// Only now do weights take effect: the next poll goes to the highest-weight partition.
	tqlb.pickReadPartitionWeighted(partitionCount, weights)
	assert.Equal(t, 2, tqlb.pollerCounts[3],
		"once every partition has one poller, the highest-weight partition gets the next")
}

func TestTQLoadBalancerWeighted_Release(t *testing.T) {
	partitionCount := 3
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tqlb := newTaskQueueLoadBalancer(f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY))

	// Skewed weights concentrate pollers on partition 2. Releasing every token must return all
	// partitions to zero, which only holds if each weighted-path token releases the partition it
	// was actually assigned (rather than, say, always partition 0).
	weights := []int64{100, 100, 800}
	const n = 100
	tokens := make([]*pollToken, 0, n)
	for range n {
		tokens = append(tokens, tqlb.pickReadPartitionWeighted(partitionCount, weights))
	}
	assert.Greater(t, tqlb.pollerCounts[2], tqlb.pollerCounts[0], "pollers should concentrate on the high-weight partition")
	assert.Greater(t, tqlb.pollerCounts[2], tqlb.pollerCounts[1], "pollers should concentrate on the high-weight partition")

	for _, tok := range tokens {
		tok.Release()
	}
	for _, c := range tqlb.pollerCounts {
		assert.Equal(t, 0, c)
	}
}

func TestPickReadPartition_BacklogAware(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// backlog counts present and covering all read partitions -> weighted toward partition 1.
	pc := PartitionCounts{Read: 2, Write: 2, BacklogCount: []number.Compact8{0, 100}}
	for range 200 {
		lb.PickReadPartition(taskQueue, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(taskQueue)
	assert.Greater(t, tqlb.pollerCounts[1], tqlb.pollerCounts[0],
		"partition with more backlog should receive more pollers")
}

func TestPickReadPartition_NoBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// no backlog counts -> classic fewest-poller balancing across the 4 read partitions.
	pc := PartitionCounts{Read: 4, Write: 4}
	for range 8 {
		lb.PickReadPartition(taskQueue, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(taskQueue)
	for _, c := range tqlb.pollerCounts {
		assert.Equal(t, 2, c)
	}
}

func TestPickReadPartition_ShortBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// backlog counts present but shorter than the read count (e.g. lagging a scale-up) -> fall
	// back to fewest-poller balancing rather than indexing out of range.
	pc := PartitionCounts{Read: 4, Write: 4, BacklogCount: []number.Compact8{0, 100}}
	for range 8 {
		lb.PickReadPartition(taskQueue, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(taskQueue)
	for _, c := range tqlb.pollerCounts {
		assert.Equal(t, 2, c)
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
