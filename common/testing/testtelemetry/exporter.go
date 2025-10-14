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

var _ sdktrace.SpanExporter = (*MemoryExporter)(nil)

type (
	// MemoryExporter is a span exporter that collects spans in memory.
	//
	// Note that internally it defers to otlptrace.Exporter to handle the actual export,
	// since the code to transform spans to the exportable format is quite complex and
	// not exported directly. By providing a custom "client", we can intercept the exported
	// spans and write them to a file on request.
	MemoryExporter struct {
		client           *memExporterClient
		internalExporter sdktrace.SpanExporter
	}
	memExporterClient struct {
		outDir    string
		spansLock sync.Mutex
		spans     []*tracepb.ResourceSpans
	}
)

func NewFileExporter(outDir string) *MemoryExporter {
	client := &memExporterClient{outDir: outDir}
	return &MemoryExporter{
		client:           client,
		internalExporter: otlptrace.NewUnstarted(client),
	}
}

func (e *MemoryExporter) ExportSpans(
	ctx context.Context,
	spans []sdktrace.ReadOnlySpan,
) error {
	return e.internalExporter.ExportSpans(ctx, spans)
}

func (e *MemoryExporter) Shutdown(ctx context.Context) error {
	return e.client.Stop(ctx)
}

func (e *MemoryExporter) Write(filename string) (string, error) {
	return e.client.Write(filename)
}

func (c *memExporterClient) Start(ctx context.Context) error {
	return nil
}

func (c *memExporterClient) Stop(ctx context.Context) error {
	c.spansLock.Lock()
	defer c.spansLock.Unlock()

	c.spans = nil
	return nil
}

func (c *memExporterClient) UploadTraces(
	ctx context.Context,
	protoSpans []*tracepb.ResourceSpans,
) error {
	c.spansLock.Lock()
	defer c.spansLock.Unlock()

	c.spans = append(c.spans, protoSpans...)
	return nil
}

func (c *memExporterClient) Write(filename string) (string, error) {
	c.spansLock.Lock()
	defer c.spansLock.Unlock()

	if len(c.spans) == 0 {
		return "", errors.New("no spans to export")
	}

	if err := os.MkdirAll(c.outDir, 0o755); err != nil {
		return "", err
	}

	filePath := filepath.Join(c.outDir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	marshaler := &ptrace.JSONMarshaler{}
	unmarshaler := &ptrace.ProtoUnmarshaler{}

	// Write each ResourceSpan as a separate line.
	for _, resourceSpan := range c.spans {
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
