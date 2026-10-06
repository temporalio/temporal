package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TestReadOnlyNodeBackend_UnsupportedPanics documents the read only contract: methods only
// write and task paths reach panic rather than returning a value computed against nothing.
func TestReadOnlyNodeBackend_UnsupportedPanics(t *testing.T) {
	backend := newReadOnlyNodeBackend(&persistencespb.WorkflowMutableState{
		ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{},
		ExecutionState: &persistencespb.WorkflowExecutionState{},
	})

	for name, call := range map[string]func(){
		"NextTransitionCount":  func() { backend.NextTransitionCount() },
		"AddTasks":             func() { backend.AddTasks() },
		"DeleteCHASMPureTasks": func() { backend.DeleteCHASMPureTasks(time.Time{}) },
		"GetNamespaceEntry":    func() { backend.GetNamespaceEntry() },
		"GetCurrentVersion":    func() { backend.GetCurrentVersion() },
		"ChasmSkipPersistenceEnabled": func() {
			backend.ChasmSkipPersistenceEnabled()
		},
		"SetTimeSkippingConfig":        func() { backend.SetTimeSkippingConfig(nil) },
		"RecordTimeSkippingTransition": func() { backend.RecordTimeSkippingTransition(nil) },
		"ChasmDLQScheduledPureTaskOnValidationEnabled": func() {
			backend.ChasmDLQScheduledPureTaskOnValidationEnabled()
		},
	} {
		require.Panics(t, call, name)
	}
}

// TestReadOnlyNodeBackend_IsWorkflow reads the archetype from the record, the way the history
// service does, including treating a record with no CHASM nodes as a workflow.
func TestReadOnlyNodeBackend_IsWorkflow(t *testing.T) {
	rootOfType := func(typeID ArchetypeID) map[string]*persistencespb.ChasmNode {
		return map[string]*persistencespb.ChasmNode{
			rootEncodedPath: {Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{TypeId: typeID},
				},
			}},
		}
	}

	for name, tc := range map[string]struct {
		nodes      map[string]*persistencespb.ChasmNode
		isWorkflow bool
	}{
		"no CHASM nodes":    {nodes: nil, isWorkflow: true},
		"workflow root":     {nodes: rootOfType(WorkflowArchetypeID), isWorkflow: true},
		"non-workflow root": {nodes: rootOfType(WorkflowArchetypeID + 1), isWorkflow: false},
	} {
		t.Run(name, func(t *testing.T) {
			backend := newReadOnlyNodeBackend(&persistencespb.WorkflowMutableState{
				ChasmNodes:     tc.nodes,
				ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{},
				ExecutionState: &persistencespb.WorkflowExecutionState{},
			})
			require.Equal(t, tc.isWorkflow, backend.IsWorkflow())
		})
	}
}
