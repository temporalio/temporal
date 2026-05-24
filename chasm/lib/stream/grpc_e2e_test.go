package stream_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/streamsegmentstore"
)

func TestStreamServiceGRPCEndToEnd(t *testing.T) {
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(stream.NewNilLibrary()))

	engine := chasmtest.NewEngine(t, registry)
	handler := stream.NewHandler(
		logger,
		streamsegmentstore.NewMemory(),
		fixedShardResolver{shard: 1},
		nil,
	)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			next grpc.UnaryHandler,
		) (any, error) {
			return next(chasm.NewEngineContext(ctx, engine), req)
		}),
	)
	streampb.RegisterStreamServiceServer(server, handler)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, listener.Close())
	})

	ctx := context.Background()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := streampb.NewStreamServiceClient(conn)

	_, err = client.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		CreatedBy:   "grpc-test",
	})
	require.NoError(t, err)

	items := []*streampb.PublishItem{
		{Data: []byte("a"), Topic: "alpha"},
		{Data: []byte("b"), Topic: "beta"},
		{Data: []byte("c"), Topic: "alpha"},
	}
	pubResp, err := client.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    1,
		Items:       items,
		PayloadHash: sha256Hash(items),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), pubResp.FirstOffset)
	require.Equal(t, int64(3), pubResp.ItemCount)

	readResp, err := client.ReadRange(ctx, &streampb.ReadRangeRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		StartOffset: 0,
		EndOffset:   3,
		Topics:      []string{"beta"},
	})
	require.NoError(t, err)
	require.Len(t, readResp.Items, 1)
	require.Equal(t, []byte("b"), readResp.Items[0].Data)
	require.Equal(t, []int64{1}, readResp.Offsets)
	require.Equal(t, int64(3), readResp.NextOffset)

	desc, err := client.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), desc.State.HeadOffset)
	require.Equal(t, int64(1), desc.PublisherCount)
}
