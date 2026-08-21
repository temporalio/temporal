package objectleak

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
)

// WriteHeapDiagnostics writes a raw heap dump and matching test executable to
// outputDir. Both files may contain sensitive data.
func WriteHeapDiagnostics(outputDir string) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	return writeHeapDiagnostics(outputDir, executable, func(heapDump *os.File) {
		debug.WriteHeapDump(heapDump.Fd())
	})
}

func writeHeapDiagnostics(
	outputDir string,
	executable string,
	writeHeapDump func(*os.File),
) error {
	heapDump, err := os.Create(filepath.Join(outputDir, "heap.dump"))
	if err != nil {
		return err
	}
	writeHeapDump(heapDump)
	if err := heapDump.Close(); err != nil {
		return err
	}

	source, err := os.Open(executable)
	if err != nil {
		return err
	}
	destination, err := os.OpenFile(filepath.Join(outputDir, "leakcheck.test"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return errors.Join(err, source.Close())
	}
	_, copyErr := io.Copy(destination, source)
	return errors.Join(copyErr, destination.Close(), source.Close())
}
