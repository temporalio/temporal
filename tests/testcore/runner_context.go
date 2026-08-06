package testcore

import (
	"sort"
	"strings"
	"sync"
	"testing"
)

// ShardRecord describes one testcore shard decision made during a runner call.
type ShardRecord struct {
	LogicalName string
	Owner       int
	Total       int
	Owned       bool
}

// RunContext owns the router and shard decisions for an imported functional
// test runner.
type RunContext struct {
	router       *clusterRouter
	physicalRoot string

	mu            sync.Mutex
	shardManifest []ShardRecord
}

var activeRunContexts = struct {
	sync.RWMutex
	contexts map[*RunContext]struct{}
}{
	contexts: make(map[*RunContext]struct{}),
}

// Run installs an isolated testcore router for run. Its cleanup is registered
// on t, so Go waits for all parallel descendants before the router closes its
// pools.
func Run(t *testing.T, factory ClusterFactory, run func()) *RunContext {
	t.Helper()
	if factory == nil {
		t.Fatal("testcore.Run requires a cluster factory")
		return nil
	}

	physicalRoot := t.Name()
	ctx := &RunContext{
		physicalRoot: physicalRoot,
	}
	activeRunContexts.Lock()
	duplicate := false
	for active := range activeRunContexts.contexts {
		if active.physicalRoot == physicalRoot {
			duplicate = true
			break
		}
	}
	if !duplicate {
		activeRunContexts.contexts[ctx] = struct{}{}
	}
	activeRunContexts.Unlock()
	if duplicate {
		t.Fatalf("testcore.Run already active for physical root %q", physicalRoot)
		return nil
	}
	ctx.router = newClusterRouter(factory, defaultRouterConfig)

	t.Cleanup(func() {
		activeRunContexts.Lock()
		delete(activeRunContexts.contexts, ctx)
		activeRunContexts.Unlock()
		if err := ctx.router.close(); err != nil {
			t.Errorf("Failed to tear down testcore runner clusters: %v", err)
		}
	})

	run()
	return ctx
}

// ShardManifest returns a snapshot of the shard decisions for t's active run.
func ShardManifest(t *testing.T) []ShardRecord {
	ctx := runContextFor(t)
	if ctx == nil {
		return nil
	}
	return ctx.shards()
}

func (c *RunContext) shards() []ShardRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := append([]ShardRecord(nil), c.shardManifest...)
	sort.Slice(records, func(i, j int) bool {
		if records[i].LogicalName != records[j].LogicalName {
			return records[i].LogicalName < records[j].LogicalName
		}
		return records[i].Owner < records[j].Owner
	})
	return records
}

func (c *RunContext) recordShard(record ShardRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shardManifest = append(c.shardManifest, record)
}

func runContextFor(t *testing.T) *RunContext {
	name := t.Name()
	activeRunContexts.RLock()
	defer activeRunContexts.RUnlock()

	var match *RunContext
	for ctx := range activeRunContexts.contexts {
		if name != ctx.physicalRoot && !strings.HasPrefix(name, ctx.physicalRoot+"/") {
			continue
		}
		if match == nil || len(ctx.physicalRoot) > len(match.physicalRoot) {
			match = ctx
		}
	}
	return match
}

func routerFor(t *testing.T) *clusterRouter {
	if ctx := runContextFor(t); ctx != nil {
		return ctx.router
	}
	return testClusterRouter
}

func clusterFactoryFor(t *testing.T) ClusterFactory {
	return routerFor(t).factory
}

// LogicalTestName returns t's name relative to its active testcore runner.
// Outside a runner, it returns t.Name().
func LogicalTestName(t *testing.T) string {
	ctx := runContextFor(t)
	if ctx == nil {
		return t.Name()
	}
	if t.Name() == ctx.physicalRoot {
		return t.Name()
	}
	return strings.TrimPrefix(t.Name(), ctx.physicalRoot+"/")
}
