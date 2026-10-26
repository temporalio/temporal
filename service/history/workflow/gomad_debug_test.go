//go:build gomad && wasip1 && wasm

package workflow

import (
	"context"
	"os"
	"runtime"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/gomaddebug"
	"go.temporal.io/server/common/locks"
)

func TestGomadMutableStateDumpUsesSnapshotWithWorkflowLockHeld(t *testing.T) {
	ms := &MutableStateImpl{
		executionInfo:   &persistencespb.WorkflowExecutionInfo{WorkflowId: "workflow", NamespaceId: "namespace"},
		executionState:  &persistencespb.WorkflowExecutionState{RunId: "run"},
		nextEventIDInDB: 11,
	}
	workflow := &ContextImpl{lock: locks.NewPrioritySemaphore(1), MutableState: ms}
	if err := workflow.Lock(context.Background(), locks.PriorityHigh); err != nil {
		t.Fatal(err)
	}
	slot := gomadMutableStateSequence % uint64(len(gomadMutableStateViews))
	ms.gomadDebugSnapshot("fixture")
	ms.nextEventIDInDB = 19
	var w gomaddebug.DumpWriter
	dumpGomadMutableStates(&w)
	if gomadMutableStateViews[slot].nextEventIDInDB != 11 {
		t.Fatal("dump did not retain last completed snapshot")
	}
	if os.Getenv("GOMAD_DEBUG_CANARY") == "1" {
		t.Log("Temporal mutable-state canary: workflow lock remains held; live persisted_next_event=19, completed snapshot persisted_next_event=11")
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
	workflow.Unlock()
}
