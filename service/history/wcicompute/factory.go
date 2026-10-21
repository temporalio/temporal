package wcicompute

import (
	"context"
	"slices"

	"go.temporal.io/auto-scaled-workers/wci/computeservice"
	"go.temporal.io/server/api/workercontrollerservice/v1"
	"go.temporal.io/server/chasm/lib/wci"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
)

// Factory picks where a provider type runs: in history, on the WCI process, or nowhere yet.
type Factory struct {
	local     workercontrollerservice.WorkerControllerComputeServiceClient
	remote    workercontrollerservice.WorkerControllerComputeServiceClient
	inProcess dynamicconfig.TypedPropertyFn[[]string]
	remoteFor dynamicconfig.TypedPropertyFn[[]string]
}

func NewFactory(dc *dynamicconfig.Collection, remote *RemoteClient) *Factory {
	return &Factory{
		local:     localClient{server: computeservice.NewServer(dc)},
		remote:    remote,
		inProcess: wci.InProcessComputeProviders.Get(dc),
		remoteFor: wci.RemoteComputeProviders.Get(dc),
	}
}

// For returns the client for providerType; in-process takes precedence over remote.
func (f *Factory) For(providerType string) (workercontrollerservice.WorkerControllerComputeServiceClient, error) {
	switch {
	case slices.Contains(f.inProcess(), providerType):
		return f.local, nil
	case slices.Contains(f.remoteFor(), providerType):
		return f.remote, nil
	default:
		return nil, wci.ErrProviderNotSupportedOnCHASM
	}
}

// localClient serves the compute service inside history, with no network hop.
type localClient struct {
	server *computeservice.Server
}

func (c localClient) ValidateConfig(ctx context.Context, req *workercontrollerservice.ValidateConfigRequest, _ ...grpc.CallOption) (*workercontrollerservice.ValidateConfigResponse, error) {
	return c.server.ValidateConfig(ctx, req)
}

func (c localClient) InvokeWorker(ctx context.Context, req *workercontrollerservice.InvokeWorkerRequest, _ ...grpc.CallOption) (*workercontrollerservice.InvokeWorkerResponse, error) {
	return c.server.InvokeWorker(ctx, req)
}

func (c localClient) UpdateWorkerSetSize(ctx context.Context, req *workercontrollerservice.UpdateWorkerSetSizeRequest, _ ...grpc.CallOption) (*workercontrollerservice.UpdateWorkerSetSizeResponse, error) {
	return c.server.UpdateWorkerSetSize(ctx, req)
}
