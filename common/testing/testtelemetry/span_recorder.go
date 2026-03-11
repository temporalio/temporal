package testtelemetry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	ctrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var _ sdktrace.SpanExporter = (*SpanRecorder)(nil)

// SpanRecorder collects spans in memory and can write them to a JSON file.
//
// It uses otlptrace.Exporter internally to convert ReadOnlySpan to proto format,
// since that conversion logic is not exported by the SDK. SpanRecorder itself
// implements the otlptrace.Client interface to intercept the converted spans.
type SpanRecorder struct {
	mu               sync.Mutex
	spans            []*tracepb.ResourceSpans
	internalExporter sdktrace.SpanExporter
}

func NewSpanRecorder() *SpanRecorder {
	e := &SpanRecorder{}
	e.internalExporter = otlptrace.NewUnstarted(e)
	return e
}

func (e *SpanRecorder) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return e.internalExporter.ExportSpans(ctx, spans)
}

func (e *SpanRecorder) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = nil
	return nil
}

// WriteToDir writes collected spans to a JSON file in the given directory.
func (e *SpanRecorder) WriteToDir(outDir, filename string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.spans) == 0 {
		return "", errors.New("no spans to export")
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}

	filePath := filepath.Join(outDir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	marshaler := &ptrace.JSONMarshaler{}
	unmarshaler := &ptrace.ProtoUnmarshaler{}

	// Write each ResourceSpan as a separate JSON line.
	for _, resourceSpan := range e.spans {
		protoBytes, err := proto.Marshal(&ctrace.ExportTraceServiceRequest{
			ResourceSpans: []*tracepb.ResourceSpans{resourceSpan},
		})
		if err != nil {
			return "", err
		}
		pdataTraces, err := unmarshaler.UnmarshalTraces(protoBytes)
		if err != nil {
			return "", err
		}
		data, err := marshaler.MarshalTraces(pdataTraces)
		if err != nil {
			return "", err
		}
		if _, err := file.Write(data); err != nil {
			return "", err
		}
		if _, err := file.WriteString("\n"); err != nil {
			return "", err
		}
	}

	return filePath, nil
}

// otlptrace.Client interface implementation — used by the internal otlptrace.Exporter
// to deliver converted proto spans back to us.

func (e *SpanRecorder) Start(ctx context.Context) error { return nil }
func (e *SpanRecorder) Stop(ctx context.Context) error  { return nil }

func (e *SpanRecorder) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, protoSpans...)
	return nil
}
