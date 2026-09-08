package mixedbrain

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOmesFailureIncludesActionableSynopsis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "omes.log")
	log := `worker output that is not JSON
2026-08-27T14:29:28.123Z	ERROR	internal/internal_task_handlers.go:2495	Activity error.	{"Error":"failed to execute client action: Standalone activity is disabled"}
2026-08-27T14:29:29.123Z	ERROR	internal/internal_task_handlers.go:2495	Activity error.	{"Error":"failed to execute client action: failed to start standalone activity: Standalone activity is disabled"}
2026-08-27T14:29:30.123Z	INFO	internal/internal_worker_base.go:728	Task processing failed with error	{"Error":"Standalone activity is disabled"}
{"L":"ERROR","T":"2026-08-27T14:29:30.123Z","M":"iteration 3 encountered error: context deadline exceeded"}
{"L":"FATAL","T":"2026-08-27T14:34:28.123Z","M":"scenario failed: timed out while waiting for runs to complete: context deadline exceeded"}
`
	require.NoError(t, os.WriteFile(path, []byte(log), 0644))

	err := newOmesFailure("throughput_stress", path, errors.New("exit status 1"))
	require.EqualError(t, err, "Omes throughput_stress failed:\n"+
		"  final error: scenario failed: timed out while waiting for runs to complete: context deadline exceeded\n"+
		"  recurring errors:\n"+
		"    ERROR Standalone activity is disabled: 2 occurrences (first seen: 2026-08-27T14:29:28Z)\n"+
		"  full log: "+path)
}

func TestNewOmesFailureFallsBackToCommandError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "omes.log")
	require.NoError(t, os.WriteFile(path, []byte("not JSON\n"), 0644))

	err := newOmesFailure("scheduler_stress", path, errors.New("exit status 2"))
	require.Contains(t, err.Error(), "command error: exit status 2")
	require.NotContains(t, err.Error(), "recurring errors:")
}
