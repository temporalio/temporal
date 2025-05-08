package otellogger

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
