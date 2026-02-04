package testtelemetry

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
	ctrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Dump provides file dump functionality for writing traces on test failure.
type Dump struct {
	dumpDir      string
	subscription *subscription
}

// NewDump creates a new Dump for the given test.
// Spans are buffered until Write() is called.
// The subscription is automatically cleaned up when the test ends.
func NewDump(t *testing.T, exporter *MemoryExporter, dumpDir string) *Dump {
	return &Dump{
		dumpDir:      dumpDir,
		subscription: exporter.subscribe(t),
	}
}

// Write writes all buffered spans to a file in the dump directory.
// Returns the full path of the written file, or an error if no spans or write fails.
func (d *Dump) Write(filename string) (string, error) {
	spans := d.subscription.spans()

	if len(spans) == 0 {
		return "", errors.New("no spans to export")
	}

	if err := os.MkdirAll(d.dumpDir, 0o755); err != nil {
		return "", err
	}

	filePath := filepath.Join(d.dumpDir, filename)

	// Batch all spans into a single request for efficient serialization.
	protoBytes, err := proto.Marshal(&ctrace.ExportTraceServiceRequest{
		ResourceSpans: spans,
	})
	if err != nil {
		return "", err
	}

	unmarshaler := &ptrace.ProtoUnmarshaler{}
	pdataTraces, err := unmarshaler.UnmarshalTraces(protoBytes)
	if err != nil {
		return "", err
	}

	marshaler := &ptrace.JSONMarshaler{}
	data, err := marshaler.MarshalTraces(pdataTraces)
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return "", err
	}

	return filePath, nil
}
