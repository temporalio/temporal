//go:build test_dep

package failpointgrpc

import (
	"context"

	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
)

// NewRemote returns a FailpointService that forwards calls to a remote FailpointService listening
// on conn. This is the standard way for tests to control failpoints inside a subprocess node,
// where the in-process registry isn't reachable.
func NewRemote(conn *grpc.ClientConn) failpointpb.FailpointServiceServer {
	return &remote{client: failpointpb.NewFailpointServiceClient(conn)}
}

type remote struct {
	failpointpb.UnimplementedFailpointServiceServer
	client failpointpb.FailpointServiceClient
}

func (r *remote) InjectPanic(
	ctx context.Context,
	req *failpointpb.InjectPanicRequest,
) (*failpointpb.InjectPanicResponse, error) {
	return r.client.InjectPanic(ctx, req)
}

func (r *remote) InjectReturn(
	ctx context.Context,
	req *failpointpb.InjectReturnRequest,
) (*failpointpb.InjectReturnResponse, error) {
	return r.client.InjectReturn(ctx, req)
}

func (r *remote) InjectBlock(
	ctx context.Context,
	req *failpointpb.InjectBlockRequest,
) (*failpointpb.InjectBlockResponse, error) {
	return r.client.InjectBlock(ctx, req)
}

func (r *remote) WaitForBlock(
	ctx context.Context,
	req *failpointpb.WaitForBlockRequest,
) (*failpointpb.WaitForBlockResponse, error) {
	return r.client.WaitForBlock(ctx, req)
}

func (r *remote) DisableFailpoint(
	ctx context.Context,
	req *failpointpb.DisableFailpointRequest,
) (*failpointpb.DisableFailpointResponse, error) {
	return r.client.DisableFailpoint(ctx, req)
}

func (r *remote) DisableAllFailpoints(
	ctx context.Context,
	req *failpointpb.DisableAllFailpointsRequest,
) (*failpointpb.DisableAllFailpointsResponse, error) {
	return r.client.DisableAllFailpoints(ctx, req)
}

func (r *remote) ListFailpoints(
	ctx context.Context,
	req *failpointpb.ListFailpointsRequest,
) (*failpointpb.ListFailpointsResponse, error) {
	return r.client.ListFailpoints(ctx, req)
}

func (r *remote) DescribeRegistry(
	ctx context.Context,
	req *failpointpb.DescribeRegistryRequest,
) (*failpointpb.DescribeRegistryResponse, error) {
	return r.client.DescribeRegistry(ctx, req)
}
