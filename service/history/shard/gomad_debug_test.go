//go:build gomad && wasip1 && wasm

package shard

import (
	"os"
	"runtime"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/gomaddebug"
)

func TestGomadShardDumpUsesSnapshotWithLockHeld(t *testing.T) {
	s := &ContextImpl{shardID: 7, shardInfo: &persistencespb.ShardInfo{RangeId: 19}, state: contextStateAcquired}
	s.rwLock.Lock()
	slot := gomadShardSequence % uint64(len(gomadShardViews))
	s.gomadDebugSnapshot()
	s.shardInfo.RangeId = 23
	var w gomaddebug.DumpWriter
	dumpGomadShards(&w)
	if gomadShardViews[slot].rangeID != 19 {
		t.Fatal("dump did not retain last completed snapshot")
	}
	if os.Getenv("GOMAD_DEBUG_CANARY") == "1" {
		t.Log("Temporal shard canary: application lock remains held; live range=23, completed snapshot range=19")
		go func() {
			for range 8 {
				runtime.Gosched()
			}
		}()
		for range 3 {
			runtime.Gosched()
		}
		os.Exit(1)
	}
	s.rwLock.Unlock()
}
