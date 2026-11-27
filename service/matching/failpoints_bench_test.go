package matching

import (
	"context"
	"testing"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/testx/failpoint"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These benchmarks measure the cost of the failpoint checks in matching when no failpoint fires.
// Run them with:
//
//	go test -run='^$' -bench='BenchmarkSkipSyncMatch|BenchmarkTrySyncMatch' -benchmem -count=10 -cpu=18 ./service/matching
//
// The serial rows call the function and discard its result; b.Loop keeps the call and its result
// alive, so no sink variable is needed and every row handles its result the same way. The
// "Parallel" rows use b.RunParallel, which runs GOMAXPROCS goroutines (18 with -cpu=18).
// The TestHooks rows live in failpoints_bench_nodep_test.go because they only measure the
// production stub when built without test_dep.

func BenchmarkSkipSyncMatch(b *testing.B) {
	ns := benchmarkSyncMatchNamespace()
	nsID, nsName := ns.ID(), ns.Name()
	unrelated := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint(failpoint.NewKey[bool]("matching.benchmarkUnrelated"))
	fp.On()
	unrelated.Set(fp)

	// NoCheck is the empty-loop baseline.
	b.Run("NoCheck", func(b *testing.B) {
		for b.Loop() {
		}
	})
	b.Run("UnguardedNil", func(b *testing.B) {
		for b.Loop() {
			_ = skipSyncMatch(nil, nsID, nsName)
		}
	})

	for _, tc := range []struct {
		name   string
		source *failpoint.Registry
	}{
		{name: "Off"},
		{name: "EmptyRegistry", source: failpoint.NewRegistry()},
		{name: "UnrelatedKey", source: unrelated},
	} {
		b.Run(tc.name, func(b *testing.B) {
			eng := &matchingEngineImpl{failpointSource: tc.source}
			for b.Loop() {
				if source := eng.failpointSource; source != nil {
					_ = skipSyncMatch(source, nsID, nsName)
				}
			}
		})
	}

	// Parallel means GOMAXPROCS goroutines (18 with -cpu=18) calling skipSyncMatch concurrently.
	b.Run("UnrelatedKeyParallel", func(b *testing.B) {
		eng := &matchingEngineImpl{failpointSource: unrelated}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if source := eng.failpointSource; source != nil {
					_ = skipSyncMatch(source, nsID, nsName)
				}
			}
		})
	})
}

func BenchmarkTrySyncMatch(b *testing.B) {
	for _, tc := range []struct {
		name   string
		source *failpoint.Registry
	}{
		{name: "Off"},
		{name: "EmptyRegistry", source: failpoint.NewRegistry()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkTrySyncMatch(b, tc.source, (*physicalTaskQueueManagerImpl).TrySyncMatch)
		})
	}
}

// benchmarkTrySyncMatch runs try against a manager whose matcher has a significant backlog.
func benchmarkTrySyncMatch(
	b *testing.B,
	source *failpoint.Registry,
	try func(*physicalTaskQueueManagerImpl, context.Context, *internalTask) (syncMatchOutcome, error),
) {
	ns := benchmarkSyncMatchNamespace()
	const priority = 1
	config := &taskQueueConfig{
		DefaultPriorityKey:   priority,
		BacklogNegligibleAge: func() time.Duration { return 0 },
	}

	// A significant backlog makes Offer return before it can enqueue or wait.
	matcher := &priTaskMatcher{data: matcherData{config: config}}
	matcher.data.tasks.ages = newBacklogAgeTracker()
	matcher.data.tasks.ages.record(timestamppb.New(time.Unix(1, 0)), 1)

	manager := &physicalTaskQueueManagerImpl{
		partitionMgr: &taskQueuePartitionManagerImpl{
			engine: &matchingEngineImpl{failpointSource: source},
			ns:     ns,
		},
		config:     config,
		liveness:   &liveness{},
		matcher:    matcher,
		tasksAdded: map[priorityKey]*taskTracker{priority: newTaskTracker(clock.NewEventTimeSource(), time.Second, time.Second)},
	}
	ctx := b.Context()
	task := newInternalTaskForSyncMatch(&persistencespb.TaskInfo{}, nil, 0, nil)
	if outcome, err := try(manager, ctx, task); outcome != syncMatchBacklogPresent || err != nil {
		b.Fatalf("TrySyncMatch returned (%v, %v), want (%v, nil)", outcome, err, syncMatchBacklogPresent)
	}

	for b.Loop() {
		_, _ = try(manager, ctx, task)
	}
}

func benchmarkSyncMatchNamespace() *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id:   defaultNamespaceId,
		Name: "ns-name",
	}, nil, "active")
}
