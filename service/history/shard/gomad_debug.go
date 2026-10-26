//go:build gomad && wasip1 && wasm

package shard

import (
	"strconv"

	"go.temporal.io/server/common/gomaddebug"
)

type gomadShardView struct {
	sequence       uint64
	shardID        int32
	rangeID        int64
	queues         int
	tasksCompleted int
}

var (
	gomadShardViews    [32]gomadShardView
	gomadShardSequence uint64
	gomadShardProbe    = gomaddebug.DeclareProbe("temporal.shard.snapshot")
)

func init() { gomaddebug.RegisterDump("temporal.shards", dumpGomadShards) }

// The caller owns rwLock. Only scalar copies survive the unlock; inspection
// never follows live shard pointers or waits on a frozen application goroutine.
func (s *ContextImpl) gomadDebugSnapshot() {
	if s.shardInfo == nil {
		return
	}
	view := gomadShardView{
		sequence:       gomadShardSequence + 1,
		shardID:        s.shardID,
		rangeID:        s.shardInfo.RangeId,
		queues:         len(s.shardInfo.QueueStates),
		tasksCompleted: s.tasksCompletedSinceLastUpdate,
	}
	gomadShardViews[gomadShardSequence%uint64(len(gomadShardViews))] = view
	gomadShardSequence++
	if gomadShardProbe.Enabled() {
		gomadShardProbe.Emit(gomaddebug.Int64(int64(view.shardID)), gomaddebug.Int64(view.rangeID), gomaddebug.Uint64(view.sequence))
	}
}

func dumpGomadShards(w *gomaddebug.DumpWriter) {
	w.String("latest completed shard write-unlock snapshots; sequence counts snapshots, not choices; values may precede stop\nretained=32 total=")
	writeGomadShardInt(w, int64(gomadShardSequence))
	w.String("\n")
	start := uint64(0)
	if gomadShardSequence > uint64(len(gomadShardViews)) {
		start = gomadShardSequence - uint64(len(gomadShardViews))
	}
	for seq := start; seq < gomadShardSequence; seq++ {
		v := &gomadShardViews[seq%uint64(len(gomadShardViews))]
		w.String("snapshot=")
		writeGomadShardInt(w, int64(v.sequence))
		w.String(" shard=")
		writeGomadShardInt(w, int64(v.shardID))
		w.String(" range=")
		writeGomadShardInt(w, v.rangeID)
		w.String(" queues=")
		writeGomadShardInt(w, int64(v.queues))
		w.String(" tasks_completed=")
		writeGomadShardInt(w, int64(v.tasksCompleted))
		w.String("\n")
	}
}

func writeGomadShardInt(w *gomaddebug.DumpWriter, n int64) {
	var buffer [24]byte
	_, _ = w.Write(strconv.AppendInt(buffer[:0], n, 10))
}
