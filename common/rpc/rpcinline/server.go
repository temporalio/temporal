package rpcinline

import (
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc"
)

var _ rpc.Server = (*Server)(nil)

// Server wraps a gRPC server to automatically register services for inline RPC.
// When services are registered with this server, they're also registered with
// the inline connection so that inline clients can call them directly.
type Server struct {
	// *grpc.Server is kept because external clients like the SDK still connect via real gRPC.
	*grpc.Server
	addr               string
	serverInterceptors []grpc.UnaryServerInterceptor
	clientInterceptors []grpc.UnaryClientInterceptor
	conn               *clientConn
}

// WrapServer wraps an existing rpc.Server with inline capabilities.
// Use this when you need to add inline RPC support to a server that was created
// elsewhere (e.g., by an fx module).
// Services registered with the returned Server will be available for inline RPC calls.
func WrapServer(
	server rpc.Server,
	addr string,
	serverInterceptors []grpc.UnaryServerInterceptor,
	clientInterceptors []grpc.UnaryClientInterceptor,
) *Server {
	return &Server{
		Server:             server.(*grpc.Server),
		addr:               addr,
		serverInterceptors: serverInterceptors,
		clientInterceptors: clientInterceptors,
		conn:               newClientConn(),
	}
}

// RegisterService registers the service with both the real gRPC server and the inline connection.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.Server.RegisterService(desc, impl)
	s.conn.registerServiceDesc(desc, impl, s.clientInterceptors, s.serverInterceptors)
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Conn() grpc.ClientConnInterface {
	return s.conn
}
