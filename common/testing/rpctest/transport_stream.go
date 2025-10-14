package rpctest

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ grpc.ServerTransportStream = (*MockServerTransportStream)(nil)

// MockServerTransportStream is a reusable test double that mimics gRPC's
// internal ServerTransportStream.
type MockServerTransportStream struct {
	mu       sync.Mutex
	method   string
	headers  metadata.MD
	trailers []*metadata.MD

	// Optional hook overrides. If non-nil they are invoked instead of the
	// default header / send header logic.
	SetHeaderFunc  func(metadata.MD) error
	SendHeaderFunc func(metadata.MD) error
	TrailerFunc    func(metadata.MD) error
}

func NewMockServerTransportStream(methodName string) *MockServerTransportStream {
	return &MockServerTransportStream{
		method: methodName,
	}
}

func (s *MockServerTransportStream) Method() string {
	return s.method
}

func (s *MockServerTransportStream) SetHeader(md metadata.MD) error {
	if s.SetHeaderFunc != nil {
		return s.SetHeaderFunc(md)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headers == nil {
		s.headers = metadata.New(nil)
	}
	for k, v := range md {
		s.headers[k] = append(s.headers[k], v...)
	}
	return nil
}

func (s *MockServerTransportStream) SendHeader(md metadata.MD) error {
	if s.SendHeaderFunc != nil {
		return s.SendHeaderFunc(md)
	}
	return s.SetHeader(md)
}

func (s *MockServerTransportStream) SetTrailer(md metadata.MD) error {
	if s.TrailerFunc != nil {
		return s.TrailerFunc(md)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := md.Copy()
	s.trailers = append(s.trailers, &cp)
	return nil
}

func (s *MockServerTransportStream) CapturedHeaders() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headers == nil {
		return nil
	}
	out := metadata.New(nil)
	for k, v := range s.headers {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func (s *MockServerTransportStream) CapturedTrailers() []metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metadata.MD, 0, len(s.trailers))
	for _, t := range s.trailers {
		if t == nil {
			continue
		}
		cp := metadata.New(nil)
		for k, v := range *t {
			cp[k] = append([]string(nil), v...)
		}
		out = append(out, cp)
	}
	return out
}
