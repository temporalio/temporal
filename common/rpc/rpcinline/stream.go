package rpcinline

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ grpc.ServerTransportStream = (*fakeServerStream)(nil)

// fakeServerStream captures headers and trailers set by server interceptors
// during inline RPC calls.
type fakeServerStream struct {
	method  string
	header  metadata.MD
	trailer metadata.MD
}

func (s *fakeServerStream) Method() string {
	return s.method
}

func (s *fakeServerStream) SetHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *fakeServerStream) SendHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *fakeServerStream) SetTrailer(md metadata.MD) error {
	s.trailer = metadata.Join(s.trailer, md)
	return nil
}

// inlineStream provides bidirectional streaming for in-process gRPC calls.
// It implements both grpc.ClientStream and grpc.ServerStream.
type inlineStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	method string

	// Channels for bidirectional message passing.
	// Client sends to clientToServer, server receives from it.
	// Server sends to serverToClient, client receives from it.
	clientToServer chan any
	serverToClient chan any

	// For header/trailer metadata
	mu       sync.Mutex
	header   metadata.MD
	trailer  metadata.MD
	headerCh chan struct{} // closed when header is sent

	// Error from handler
	handlerErr   error
	handlerErrMu sync.Mutex
}

func newInlineStream(ctx context.Context, method string) *inlineStream {
	ctx, cancel := context.WithCancel(ctx)
	return &inlineStream{
		ctx:            ctx,
		cancel:         cancel,
		method:         method,
		clientToServer: make(chan any, 16),
		serverToClient: make(chan any, 16),
		headerCh:       make(chan struct{}),
	}
}

// clientStream returns a grpc.ClientStream view of this stream.
func (s *inlineStream) clientStream() grpc.ClientStream {
	return &inlineClientStream{s}
}

// serverStream returns a grpc.ServerStream view of this stream.
func (s *inlineStream) serverStream() grpc.ServerStream {
	return &inlineServerStream{s}
}

func (s *inlineStream) setHandlerError(err error) {
	s.handlerErrMu.Lock()
	defer s.handlerErrMu.Unlock()
	s.handlerErr = err
}

func (s *inlineStream) getHandlerError() error {
	s.handlerErrMu.Lock()
	defer s.handlerErrMu.Unlock()
	return s.handlerErr
}

var _ grpc.ClientStream = (*inlineClientStream)(nil)

// inlineClientStream implements grpc.ClientStream for the client side.
type inlineClientStream struct {
	*inlineStream
}

func (c *inlineClientStream) Header() (metadata.MD, error) {
	select {
	case <-c.headerCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.header.Copy(), nil
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

func (c *inlineClientStream) Trailer() metadata.MD {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trailer.Copy()
}

func (c *inlineClientStream) CloseSend() error {
	close(c.clientToServer)
	return nil
}

func (c *inlineClientStream) Context() context.Context {
	return c.ctx
}

func (c *inlineClientStream) SendMsg(m any) error {
	// Clone the message to avoid data races
	msg, err := cloneMessage(m)
	if err != nil {
		return err
	}
	select {
	case c.clientToServer <- msg:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *inlineClientStream) RecvMsg(m any) error {
	select {
	case msg, ok := <-c.serverToClient:
		if !ok {
			// Channel closed, check for handler error
			if err := c.getHandlerError(); err != nil {
				return err
			}
			return io.EOF
		}
		return copyMessage(msg, m)
	case <-c.ctx.Done():
		if err := c.getHandlerError(); err != nil {
			return err
		}
		return c.ctx.Err()
	}
}

var _ grpc.ServerStream = (*inlineServerStream)(nil)

// inlineServerStream implements grpc.ServerStream for the server side.
type inlineServerStream struct {
	*inlineStream
}

func (s *inlineServerStream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *inlineServerStream) SendHeader(md metadata.MD) error {
	s.mu.Lock()
	s.header = metadata.Join(s.header, md)
	select {
	case <-s.headerCh:
		// Already sent
	default:
		close(s.headerCh)
	}
	s.mu.Unlock()
	return nil
}

func (s *inlineServerStream) SetTrailer(md metadata.MD) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
}

func (s *inlineServerStream) Context() context.Context {
	return s.ctx
}

func (s *inlineServerStream) SendMsg(m any) error {
	// Clone the message to avoid data races
	msg, err := cloneMessage(m)
	if err != nil {
		return err
	}
	select {
	case s.serverToClient <- msg:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *inlineServerStream) RecvMsg(m any) error {
	select {
	case msg, ok := <-s.clientToServer:
		if !ok {
			return io.EOF
		}
		return copyMessage(msg, m)
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// cloneMessage creates a copy of a proto message.
func cloneMessage(m any) (any, error) {
	if m == nil {
		return nil, nil
	}
	pm, ok := m.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "message is not a proto.Message")
	}
	return proto.Clone(pm), nil
}

// copyMessage copies src to dst proto message.
func copyMessage(src, dst any) error {
	if src == nil {
		return nil
	}
	srcMsg, srcOk := src.(proto.Message)
	dstMsg, dstOk := dst.(proto.Message)
	if !srcOk || !dstOk {
		return status.Error(codes.Internal, "message is not a proto.Message")
	}
	proto.Reset(dstMsg)
	proto.Merge(dstMsg, srcMsg)
	return nil
}
