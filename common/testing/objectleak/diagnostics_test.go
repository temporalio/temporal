package objectleak

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteHeapDiagnostics(t *testing.T) {
	outputDir := t.TempDir()
	heapDumpPath := filepath.Join(outputDir, "heap.dump")
	require.NoError(t, os.WriteFile(heapDumpPath, []byte("old heap"), 0o644))
	require.NoError(t, os.Chmod(heapDumpPath, 0o644))

	err := writeHeapDumpFile(outputDir, func(heapDump *os.File) {
		_, err := heapDump.WriteString("heap")
		require.NoError(t, err)
	})
	require.NoError(t, err)

	heapDump, err := os.ReadFile(heapDumpPath)
	require.NoError(t, err)
	require.Equal(t, []byte("heap"), heapDump)

	heapDumpInfo, err := os.Stat(heapDumpPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), heapDumpInfo.Mode().Perm())

	entries, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "heap.dump", entries[0].Name())
}
