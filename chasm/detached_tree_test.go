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
		"IsWorkflow":           func() { backend.IsWorkflow() },
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
