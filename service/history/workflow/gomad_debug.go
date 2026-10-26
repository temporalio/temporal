//go:build gomad && wasip1 && wasm

package workflow

import (
	"strconv"

	"go.temporal.io/server/common/gomaddebug"
)

type gomadMutableStateView struct {
	sequence          uint64
	seam              string
	namespace         [128]byte
	workflow          [128]byte
	run               [128]byte
	namespaceLen      int
	workflowLen       int
	runLen            int
	identityTruncated bool
	nextEventIDInDB   int64
	nextEventID       int64
	version           int64
	state             int32
	status            int32
	activities        int
	timers            int
	children          int
}

var (
	gomadMutableStateViews    [64]gomadMutableStateView
	gomadMutableStateSequence uint64
	gomadMutableStateProbe    = gomaddebug.DeclareProbe("temporal.mutable_state.snapshot")
)

func init() { gomaddebug.RegisterDump("temporal.mutable_states", dumpGomadMutableStates) }

// Mutation callers already own the workflow. The bounded copies contain no live
// workflow/cache pointers, so the stopped-world callback acquires no such locks.
func (ms *MutableStateImpl) gomadDebugSnapshot(seam string) {
	if ms.executionInfo == nil || ms.executionState == nil {
		return
	}
	v := gomadMutableStateView{
		sequence:        gomadMutableStateSequence + 1,
		seam:            seam,
		nextEventIDInDB: ms.nextEventIDInDB,
		version:         ms.currentVersion,
		state:           int32(ms.executionState.State),
		status:          int32(ms.executionState.Status),
		activities:      len(ms.pendingActivityInfoIDs),
		timers:          len(ms.pendingTimerInfoIDs),
		children:        len(ms.pendingChildExecutionInfoIDs),
	}
	v.namespaceLen = copy(v.namespace[:], ms.executionInfo.NamespaceId)
	v.workflowLen = copy(v.workflow[:], ms.executionInfo.WorkflowId)
	v.runLen = copy(v.run[:], ms.executionState.RunId)
	v.identityTruncated = v.namespaceLen != len(ms.executionInfo.NamespaceId) || v.workflowLen != len(ms.executionInfo.WorkflowId) || v.runLen != len(ms.executionState.RunId)
	if ms.hBuilder != nil {
		v.nextEventID = ms.hBuilder.NextEventID()
	}
	gomadMutableStateViews[gomadMutableStateSequence%uint64(len(gomadMutableStateViews))] = v
	gomadMutableStateSequence++
	if gomadMutableStateProbe.Enabled() {
		gomadMutableStateProbe.Emit(gomaddebug.String(ms.executionInfo.WorkflowId), gomaddebug.String(ms.executionState.RunId), gomaddebug.Int64(v.nextEventID), gomaddebug.Uint64(v.sequence))
	}
}

func dumpGomadMutableStates(w *gomaddebug.DumpWriter) {
	w.String("latest completed mutable-state mutation snapshots; sequence counts snapshots, not choices; values may precede stop\nretained=64 total=")
	writeGomadMutableStateInt(w, int64(gomadMutableStateSequence))
	w.String("\n")
	start := uint64(0)
	if gomadMutableStateSequence > uint64(len(gomadMutableStateViews)) {
		start = gomadMutableStateSequence - uint64(len(gomadMutableStateViews))
	}
	for seq := start; seq < gomadMutableStateSequence; seq++ {
		v := &gomadMutableStateViews[seq%uint64(len(gomadMutableStateViews))]
		w.String("snapshot=")
		writeGomadMutableStateInt(w, int64(v.sequence))
		w.String(" seam=")
		w.String(v.seam)
		w.String(" namespace=")
		_, _ = w.Write(v.namespace[:v.namespaceLen])
		w.String(" workflow=")
		_, _ = w.Write(v.workflow[:v.workflowLen])
		w.String(" run=")
		_, _ = w.Write(v.run[:v.runLen])
		if v.identityTruncated {
			w.String(" [identity truncated]")
		}
		w.String(" next_event=")
		writeGomadMutableStateInt(w, v.nextEventID)
		w.String(" persisted_next_event=")
		writeGomadMutableStateInt(w, v.nextEventIDInDB)
		w.String(" version=")
		writeGomadMutableStateInt(w, v.version)
		w.String(" state=")
		writeGomadMutableStateInt(w, int64(v.state))
		w.String(" status=")
		writeGomadMutableStateInt(w, int64(v.status))
		w.String(" activities=")
		writeGomadMutableStateInt(w, int64(v.activities))
		w.String(" timers=")
		writeGomadMutableStateInt(w, int64(v.timers))
		w.String(" children=")
		writeGomadMutableStateInt(w, int64(v.children))
		w.String("\n")
	}
}

func writeGomadMutableStateInt(w *gomaddebug.DumpWriter, n int64) {
	var buffer [24]byte
	_, _ = w.Write(strconv.AppendInt(buffer[:0], n, 10))
}
