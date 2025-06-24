package otellogger

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
	ctrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.temporal.io/server/common/testing/freeport"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type OTELLogger struct {
	ctrace.UnimplementedTraceServiceServer
	addr      string
	spansLock sync.RWMutex
	spans     []*trace.ResourceSpans
}

func Start(tb testing.TB) (*OTELLogger, error) {
	grpcServer := grpc.NewServer()
	l := &OTELLogger{
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

func (l *OTELLogger) Addr() string {
	return "http://" + l.addr
}

func (l *OTELLogger) Spans() []*trace.ResourceSpans {
	l.spansLock.RLock()
	defer l.spansLock.RUnlock()

	return l.spans
}

func (l *OTELLogger) Export(
	ctx context.Context,
	request *ctrace.ExportTraceServiceRequest,
) (*ctrace.ExportTraceServiceResponse, error) {
	l.spansLock.Lock()
	defer l.spansLock.Unlock()

	l.spans = append(l.spans, request.ResourceSpans...)
	return &ctrace.ExportTraceServiceResponse{}, nil
}

func (l *OTELLogger) WriteFile(path string) error {
	l.spansLock.RLock()
	defer l.spansLock.RUnlock()

	if len(l.spans) == 0 {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	marshaler := &ptrace.JSONMarshaler{}
	unmarshaler := &ptrace.ProtoUnmarshaler{}

	// Write each ResourceSpan as a separate line
	for _, resourceSpan := range l.spans {
		protoBytes, err := proto.Marshal(&ctrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{resourceSpan},
		})
		if err != nil {
			return err
		}

		pdataTraces, err := unmarshaler.UnmarshalTraces(protoBytes)
		if err != nil {
			return err
		}

		data, err := marshaler.MarshalTraces(pdataTraces)
		if err != nil {
			return err
		}

		if _, err := file.Write(data); err != nil {
			return err
		}

		if _, err := file.WriteString("\n"); err != nil {
			return err
		}
	}

	return nil
}
