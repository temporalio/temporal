package objectleak

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteHeapDiagnostics(t *testing.T) {
	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "bin")
	outputDir := filepath.Join(dir, "diagnostics")
	require.NoError(t, os.Mkdir(sourceDir, 0o755))
	require.NoError(t, os.Mkdir(outputDir, 0o755))
	executable := filepath.Join(sourceDir, "source.test")
	require.NoError(t, os.WriteFile(executable, []byte("executable"), 0o755))
	heapDumpPath := filepath.Join(outputDir, "heap.dump")
	require.NoError(t, os.WriteFile(heapDumpPath, []byte("old heap"), 0o644))
	require.NoError(t, os.Chmod(heapDumpPath, 0o644))

	err := writeHeapDiagnostics(outputDir, executable, func(heapDump *os.File) {
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
	testBinary, err := os.ReadFile(filepath.Join(outputDir, "source.test"))
	require.NoError(t, err)
	require.Equal(t, []byte("executable"), testBinary)
}

func TestWriteHeapDiagnosticsPreservesExecutableInOutputDir(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "source.test")
	require.NoError(t, os.WriteFile(executable, []byte("executable"), 0o755))

	err := writeHeapDiagnostics(dir, executable, func(*os.File) {})
	require.NoError(t, err)

	testBinary, err := os.ReadFile(executable)
	require.NoError(t, err)
	require.Equal(t, []byte("executable"), testBinary)
}
