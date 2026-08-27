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
{"L":"ERROR","T":"2026-08-27T14:29:28.123Z","M":"iteration 1 encountered error: rpc error: code = Unimplemented desc = Standalone activity is disabled"}
{"L":"ERROR","T":"2026-08-27T14:29:29.123Z","M":"iteration 2 encountered error: rpc error: code = Unimplemented desc = Standalone activity is disabled"}
{"L":"ERROR","T":"2026-08-27T14:29:30.123Z","M":"iteration 3 encountered error: context deadline exceeded"}
{"L":"FATAL","T":"2026-08-27T14:34:28.123Z","M":"scenario failed: timed out while waiting for runs to complete: context deadline exceeded"}
`
	require.NoError(t, os.WriteFile(path, []byte(log), 0644))

	err := newOmesFailure("throughput_stress", path, errors.New("exit status 1"))
	require.EqualError(t, err, "Omes throughput_stress failed:\n"+
		"  final error: scenario failed: timed out while waiting for runs to complete: context deadline exceeded\n"+
		"  likely cause: Standalone activity is disabled (2 occurrences)\n"+
		"  first seen: 2026-08-27T14:29:28Z\n"+
		"  full log: "+path)
}

func TestNewOmesFailureFallsBackToCommandError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "omes.log")
	require.NoError(t, os.WriteFile(path, []byte("not JSON\n"), 0644))

	err := newOmesFailure("scheduler_stress", path, errors.New("exit status 2"))
	require.Contains(t, err.Error(), "command error: exit status 2")
	require.NotContains(t, err.Error(), "likely cause:")
}
