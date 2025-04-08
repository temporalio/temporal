// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
