package testtelemetry

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	ctrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.temporal.io/server/common/testing/freeport"
	"google.golang.org/grpc"
)

var _ ctrace.TraceServiceServer = (*MemoryCollector)(nil)

// MemoryCollector is a gRPC-based OTEL collector that collects spans in memory.
type MemoryCollector struct {
	ctrace.UnimplementedTraceServiceServer
	addr      string
	spansLock sync.RWMutex
	spans     []*trace.ResourceSpans
}

// SetupMemoryCollector configures OTEL env vars, starts an in-memory gRPC collector,
// and returns it.
func SetupMemoryCollector(tb testing.TB) *MemoryCollector {
	tb.Helper()
	tb.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	tb.Setenv("OTEL_BSP_SCHEDULE_DELAY", "100")
	tb.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
	collector, err := startMemoryCollector(tb)
	if err != nil {
		tb.Fatalf("failed to start memory collector: %v", err)
	}
	tb.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.Addr())
	return collector
}

func startMemoryCollector(tb testing.TB) (*MemoryCollector, error) {
	grpcServer := grpc.NewServer()
	l := &MemoryCollector{
		addr: fmt.Sprintf("localhost:%d", freeport.MustGetFreePort()),
	}
	ctrace.RegisterTraceServiceServer(grpcServer, l)

	listener, err := net.Listen("tcp", l.addr)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			tb.Errorf("OTEL logger failed to start: %v", err)
		}
	}()

	go func() {
		<-tb.Context().Done()
		grpcServer.Stop()
	}()

	return l, nil
}

func (l *MemoryCollector) Addr() string {
	return "http://" + l.addr
}

func (l *MemoryCollector) Spans() []*trace.ResourceSpans {
	l.spansLock.RLock()
	defer l.spansLock.RUnlock()

	return l.spans
}

func (l *MemoryCollector) Export(
	ctx context.Context,
	request *ctrace.ExportTraceServiceRequest,
) (*ctrace.ExportTraceServiceResponse, error) {
	l.spansLock.Lock()
	defer l.spansLock.Unlock()

	l.spans = append(l.spans, request.ResourceSpans...)
	return &ctrace.ExportTraceServiceResponse{}, nil
}
