package failpointgrpc

import (
	"context"

	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// NewLocalClient returns a client of a FailpointService that runs in this process, such as the
// result of NewServer, so Handle and the targets of a fan-out use in-process and remote services
// alike. The client of a remote service is failpointpb.NewFailpointServiceClient. Like a remote
// call, a local call hands the outgoing metadata of its context to the service as incoming
// metadata, such as the marker that stops fan-outs from forwarding in a loop. Call options have no
// effect on a local client.
func NewLocalClient(server failpointpb.FailpointServiceServer) failpointpb.FailpointServiceClient {
	return localClient{server: server}
}

type localClient struct {
	server failpointpb.FailpointServiceServer
}

func (c localClient) InjectPanic(ctx context.Context, req *failpointpb.InjectPanicRequest, _ ...grpc.CallOption) (*failpointpb.InjectPanicResponse, error) {
	return c.server.InjectPanic(incoming(ctx), req)
}

func (c localClient) InjectReturn(ctx context.Context, req *failpointpb.InjectReturnRequest, _ ...grpc.CallOption) (*failpointpb.InjectReturnResponse, error) {
	return c.server.InjectReturn(incoming(ctx), req)
}

func (c localClient) InjectBlock(ctx context.Context, req *failpointpb.InjectBlockRequest, _ ...grpc.CallOption) (*failpointpb.InjectBlockResponse, error) {
	return c.server.InjectBlock(incoming(ctx), req)
}

func (c localClient) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest, _ ...grpc.CallOption) (*failpointpb.WaitForBlockResponse, error) {
	return c.server.WaitForBlock(incoming(ctx), req)
}

func (c localClient) DisableFailpoint(ctx context.Context, req *failpointpb.DisableFailpointRequest, _ ...grpc.CallOption) (*failpointpb.DisableFailpointResponse, error) {
	return c.server.DisableFailpoint(incoming(ctx), req)
}

func (c localClient) DisableAllFailpoints(ctx context.Context, req *failpointpb.DisableAllFailpointsRequest, _ ...grpc.CallOption) (*failpointpb.DisableAllFailpointsResponse, error) {
	return c.server.DisableAllFailpoints(incoming(ctx), req)
}

func (c localClient) ListFailpoints(ctx context.Context, req *failpointpb.ListFailpointsRequest, _ ...grpc.CallOption) (*failpointpb.ListFailpointsResponse, error) {
	return c.server.ListFailpoints(incoming(ctx), req)
}

func (c localClient) DescribeRegistry(ctx context.Context, req *failpointpb.DescribeRegistryRequest, _ ...grpc.CallOption) (*failpointpb.DescribeRegistryResponse, error) {
	return c.server.DescribeRegistry(incoming(ctx), req)
}

// incoming returns the context that a service receives for a call with ctx: the outgoing metadata
// of ctx is the incoming metadata, and no other incoming metadata of ctx is.
func incoming(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	return metadata.NewIncomingContext(ctx, md)
}
