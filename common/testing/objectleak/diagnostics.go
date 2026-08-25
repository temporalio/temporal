package objectleak

import (
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
)

// WriteHeapDiagnostics writes a raw heap dump to outputDir. It may contain
// sensitive data.
func WriteHeapDiagnostics(outputDir string) error {
	return writeHeapDumpFile(outputDir, func(heapDump *os.File) {
		debug.WriteHeapDump(heapDump.Fd())
	})
}

func writeHeapDumpFile(
	outputDir string,
	writeHeapDump func(*os.File),
) error {
	heapDump, err := os.OpenFile(filepath.Join(outputDir, "heap.dump"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := heapDump.Chmod(0o600); err != nil {
		return errors.Join(err, heapDump.Close())
	}
	writeHeapDump(heapDump)
	return heapDump.Close()
}
