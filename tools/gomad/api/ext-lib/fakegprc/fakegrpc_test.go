package fakegrpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	fakegrpc "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
	pb "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc/fixture"
	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

type server struct {
	pb.UnimplementedGreeterServer
}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func interceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	resp.(*pb.HelloReply).Message += "!"
	return resp, err
}

func TestFakeGRPC(t *testing.T) {
	testutil.StressRun(func(seed int64) {
		addr := "localhost:42"
		lis, err := SIMLIB.Listen("tcp", addr)
		require.NoError(t, err)

		// start server
		s := fakegrpc.NewServer(fakegrpc.ChainUnaryInterceptor(interceptor))
		defer s.Stop()
		pb.RegisterGreeterServer(s, &server{})
		SIMAPI.Go(func() {
			require.NoError(t, s.Serve(lis))
		})

		// start client
		conn, err := fakegrpc.NewClient(addr)
		require.NoError(t, err)
		defer conn.Close()

		// make request
		greeter := pb.NewGreeterClient(conn)
		ctx, cancel := context.WithTimeout(SIMLIB.Background(), time.Second)
		defer cancel()
		reply, err := greeter.SayHello(ctx, &pb.HelloRequest{Name: "World"})
		require.NoError(t, err)
		require.Equal(t, "Hello World!", reply.Message)
	})
}
