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

func TestTQLoadBalancerWeighted_FillsEachBeforeFocussingOnBacklog(t *testing.T) {
	f, _ := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}
	tqlb := lb.getTaskQueueLoadBalancer(tq)

	// Partition 3 has a large backlog; the others are empty.
	backlogCounts := []number.Compact8{0, 0, 0, number.EncodeCompact8(1000)}
	pc := PartitionCounts{Read: 4, Write: 4, BacklogCount: backlogCounts}

	// During the first Read picks, no partition should ever exceed one poller.
	for range 4 {
		lb.PickReadPartition(tq, pc)
		assert.LessOrEqual(t, maxPollerCount(tqlb), 1,
			"no partition should get a 2nd poller until every partition has 1")
	}
	// After exactly Read picks, every partition has exactly one poller.
	for i, c := range tqlb.pollerCounts {
		assert.Equalf(t, 1, c, "partition %d should have exactly one poller after the first round", i)
	}

	// Do a lot more rounds of picking
	for range 1000 {
		lb.PickReadPartition(tq, pc)
	}

	// The high-backlog partition gets the most pollers; empty partitions still get some.
	assert.Greater(t, tqlb.pollerCounts[3], tqlb.pollerCounts[0])
	assert.Greater(t, tqlb.pollerCounts[0], 0)
}

func TestTQLoadBalancerWeighted_EqualWeightsBalances(t *testing.T) {
	f, _ := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// Equal (zero) backlog across all partitions -> equal weights (all == floor) -> balanced,
	// i.e. the weighted path degenerates to fewest-outstanding-pollers.
	pc := PartitionCounts{Read: 4, Write: 4, BacklogCount: []number.Compact8{0, 0, 0, 0}}
	for range 8 {
		lb.PickReadPartition(tq, pc)
	}
	for _, c := range lb.getTaskQueueLoadBalancer(tq).pollerCounts {
		assert.Equal(t, 2, c)
	}
}

func TestPickReadPartition_NoBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
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
		assert.Equal(t, 2, c)
	}
}

func TestPickReadPartition_IncompleteBacklogFallsBack(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// backlog counts present but shorter than the read count (e.g. lagging a scale-up) -> fall
	// back to fewest-poller balancing rather than indexing out of range.
	pc := PartitionCounts{Read: 4, Write: 4, BacklogCount: []number.Compact8{0, 100}}
	for range 8 {
		lb.PickReadPartition(tq, pc)
	}
	tqlb := lb.getTaskQueueLoadBalancer(tq)
	for _, c := range tqlb.pollerCounts {
		assert.Equal(t, 2, c)
	}
}

func TestPickWritePartition_BacklogAware(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
	taskQueue := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	lb := &defaultLoadBalancer{
		namespaceIDToName: func(namespace.ID) (namespace.Name, error) { return "fake-namespace", nil },
		taskQueueLBs:      make(map[tqid.TaskQueue]*tqLoadBalancer),
	}

	// compact8: byte 0 -> 0, byte 192 -> ~12.6M, byte 200 (cap) -> ~21M
	// (see common/number/compact8_test.go).
	backlogCap := number.DecodeCompact8(200)

	// Both partitions are below the cap: partition 0 is empty (gap ~21M) and partition 1 is
	// partially full (byte 192 ~12.6M, gap ~8.4M). Writes should favor the emptier partition 0
	// in proportion to the gaps, while partition 1 still receives a meaningful share.
	pc := PartitionCounts{
		Read:         2,
		Write:        2,
		BacklogCap:   200,
		BacklogCount: []number.Compact8{0, 192},
	}
	counts := make([]int, 2)
	const n = 3000
	for range n {
		p := lb.PickWritePartition(taskQueue, pc)
		counts[p.PartitionId()]++
	}
	assert.Greater(t, counts[0], counts[1], "emptier partition should receive more writes")
	assert.Greater(t, counts[1], 0, "the below-cap partition should still receive some writes")
	gap0 := backlogCap - number.DecodeCompact8(0)
	gap1 := backlogCap - number.DecodeCompact8(192)
	assert.InDelta(t, float64(n)*float64(gap0)/float64(gap0+gap1), counts[0], float64(n)*0.05,
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
		assert.InDelta(t, n/2, atCap[i], float64(n)*0.1, "at-cap partition %d roughly uniform", i)
	}
}

func TestPickWritePartition_NoBacklogUniform(t *testing.T) {
	f, err := tqid.NewTaskQueueFamily("fake-namespace-id", "fake-taskqueue")
	assert.NoError(t, err)
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
		assert.InDelta(t, n/4, counts[i], float64(n)*0.1, "partition %d roughly uniform", i)
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
