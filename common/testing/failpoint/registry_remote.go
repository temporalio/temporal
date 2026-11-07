//go:build test_dep

package failpoint

import (
	"context"

	"go.temporal.io/server/api/failpointservice/v1"
	"google.golang.org/grpc"
)

// NewRemoteRegistry returns a Registry that forwards FailpointService gRPC calls to a remote
// FailpointService listening on conn. This is the standard way for tests to control failpoints
// inside a subprocess node, where the in-process Go-object registry isn't reachable.
//
// The low-level Get/Set/Delete methods panic on a remote registry: failpoint objects can't
// cross process boundaries, and silently no-oping them would let a test inject a *Failpoint[T]
// that's never wired up and then hang forever in Wait. Use the gRPC service methods
// (InjectBlock, InjectReturn, InjectPanic, WaitForBlock, DisableFailpoint, etc.) — or, better,
// the higher-level Handle API — instead.
func NewRemoteRegistry(conn *grpc.ClientConn) Registry {
	return &remoteRegistry{client: failpointservice.NewFailpointServiceClient(conn)}
}

type remoteRegistry struct {
	failpointservice.UnimplementedFailpointServiceServer
	client failpointservice.FailpointServiceClient
}

const remoteRegistryDirectAccessMsg = "remote failpoint registry: Get/Set/Delete are not " +
	"supported across process boundaries; use the FailpointService gRPC methods or Handle"

func (r *remoteRegistry) Get(key Key) any     { panic(remoteRegistryDirectAccessMsg) }
func (r *remoteRegistry) Set(key Key, fp any) { panic(remoteRegistryDirectAccessMsg) }
func (r *remoteRegistry) Delete(key Key)      { panic(remoteRegistryDirectAccessMsg) }

func (r *remoteRegistry) InjectPanic(
	ctx context.Context,
	req *failpointservice.InjectPanicRequest,
) (*failpointservice.InjectPanicResponse, error) {
	return r.client.InjectPanic(ctx, req)
}

func (r *remoteRegistry) InjectReturn(
	ctx context.Context,
	req *failpointservice.InjectReturnRequest,
) (*failpointservice.InjectReturnResponse, error) {
	return r.client.InjectReturn(ctx, req)
}

func (r *remoteRegistry) InjectBlock(
	ctx context.Context,
	req *failpointservice.InjectBlockRequest,
) (*failpointservice.InjectBlockResponse, error) {
	return r.client.InjectBlock(ctx, req)
}

func (r *remoteRegistry) WaitForBlock(
	ctx context.Context,
	req *failpointservice.WaitForBlockRequest,
) (*failpointservice.WaitForBlockResponse, error) {
	return r.client.WaitForBlock(ctx, req)
}

func (r *remoteRegistry) DisableFailpoint(
	ctx context.Context,
	req *failpointservice.DisableFailpointRequest,
) (*failpointservice.DisableFailpointResponse, error) {
	return r.client.DisableFailpoint(ctx, req)
}

func (r *remoteRegistry) DisableAllFailpoints(
	ctx context.Context,
	req *failpointservice.DisableAllFailpointsRequest,
) (*failpointservice.DisableAllFailpointsResponse, error) {
	return r.client.DisableAllFailpoints(ctx, req)
}

func (r *remoteRegistry) ListFailpoints(
	ctx context.Context,
	req *failpointservice.ListFailpointsRequest,
) (*failpointservice.ListFailpointsResponse, error) {
	return r.client.ListFailpoints(ctx, req)
}
