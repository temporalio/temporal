//go:build test_dep

package failpointserver

import (
	"context"

	"go.temporal.io/server/api/failpointservice/v1"
	"google.golang.org/grpc"
)

// NewRemote returns a FailpointService that forwards calls to a remote FailpointService listening
// on conn. This is the standard way for tests to control failpoints inside a subprocess node,
// where the in-process registry isn't reachable.
func NewRemote(conn *grpc.ClientConn) failpointservice.FailpointServiceServer {
	return &remote{client: failpointservice.NewFailpointServiceClient(conn)}
}

type remote struct {
	failpointservice.UnimplementedFailpointServiceServer
	client failpointservice.FailpointServiceClient
}

func (r *remote) InjectPanic(
	ctx context.Context,
	req *failpointservice.InjectPanicRequest,
) (*failpointservice.InjectPanicResponse, error) {
	return r.client.InjectPanic(ctx, req)
}

func (r *remote) InjectReturn(
	ctx context.Context,
	req *failpointservice.InjectReturnRequest,
) (*failpointservice.InjectReturnResponse, error) {
	return r.client.InjectReturn(ctx, req)
}

func (r *remote) InjectBlock(
	ctx context.Context,
	req *failpointservice.InjectBlockRequest,
) (*failpointservice.InjectBlockResponse, error) {
	return r.client.InjectBlock(ctx, req)
}

func (r *remote) WaitForBlock(
	ctx context.Context,
	req *failpointservice.WaitForBlockRequest,
) (*failpointservice.WaitForBlockResponse, error) {
	return r.client.WaitForBlock(ctx, req)
}

func (r *remote) DisableFailpoint(
	ctx context.Context,
	req *failpointservice.DisableFailpointRequest,
) (*failpointservice.DisableFailpointResponse, error) {
	return r.client.DisableFailpoint(ctx, req)
}

func (r *remote) DisableAllFailpoints(
	ctx context.Context,
	req *failpointservice.DisableAllFailpointsRequest,
) (*failpointservice.DisableAllFailpointsResponse, error) {
	return r.client.DisableAllFailpoints(ctx, req)
}

func (r *remote) ListFailpoints(
	ctx context.Context,
	req *failpointservice.ListFailpointsRequest,
) (*failpointservice.ListFailpointsResponse, error) {
	return r.client.ListFailpoints(ctx, req)
}
