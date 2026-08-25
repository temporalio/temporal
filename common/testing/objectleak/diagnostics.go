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
	heapDump, err := os.OpenFile(filepath.Join(outputDir, "heap.dump"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := heapDump.Chmod(0o600); err != nil {
		return errors.Join(err, heapDump.Close())
	}
	writeHeapDump(heapDump)
	if err := heapDump.Close(); err != nil {
		return err
	}

	source, err := os.Open(executable)
	if err != nil {
		return err
	}
	destinationPath := filepath.Join(outputDir, filepath.Base(executable))
	if destinationInfo, err := os.Stat(destinationPath); err == nil {
		sourceInfo, err := source.Stat()
		if err != nil {
			return errors.Join(err, source.Close())
		}
		if os.SameFile(sourceInfo, destinationInfo) {
			return source.Close()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(err, source.Close())
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return errors.Join(err, source.Close())
	}
	_, copyErr := io.Copy(destination, source)
	return errors.Join(copyErr, destination.Close(), source.Close())
}
