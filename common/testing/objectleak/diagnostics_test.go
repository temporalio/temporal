package objectleak

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteHeapDiagnostics(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "source.test")
	require.NoError(t, os.WriteFile(executable, []byte("executable"), 0o755))

	err := writeHeapDiagnostics(dir, executable, func(heapDump *os.File) {
		_, err := heapDump.WriteString("heap")
		require.NoError(t, err)
	})
	require.NoError(t, err)

	heapDump, err := os.ReadFile(filepath.Join(dir, "heap.dump"))
	require.NoError(t, err)
	require.Equal(t, []byte("heap"), heapDump)
	testBinary, err := os.ReadFile(filepath.Join(dir, "leakcheck.test"))
	require.NoError(t, err)
	require.Equal(t, []byte("executable"), testBinary)
}
