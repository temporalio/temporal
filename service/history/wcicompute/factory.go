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
	remote    remoteService
	inProcess dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string]
	remoteFor dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string]
}

func NewFactory(dc *dynamicconfig.Collection, remote *RemoteClient) *Factory {
	return &Factory{
		local:     localClient{server: computeservice.NewServer(dc)},
		remote:    remote,
		inProcess: wci.InProcessComputeProviders.Get(dc),
		remoteFor: wci.RemoteComputeProviders.Get(dc),
	}
}

// remoteService is the WCI process's compute service, plus whether history can reach it at all.
type remoteService interface {
	workercontrollerservice.WorkerControllerComputeServiceClient
	Configured() bool
}

// Supports matches For: a remote type counts only while history can resolve the WCI service, so a
// WCI that could never be served stays a workflow instead of retrying forever on CHASM.
func (f *Factory) Supports(namespaceName, providerType string) bool {
	if slices.Contains(f.inProcess(namespaceName), providerType) {
		return true
	}
	return slices.Contains(f.remoteFor(namespaceName), providerType) && f.remote.Configured()
}

// For returns the client for providerType; in-process takes precedence over remote.
func (f *Factory) For(namespaceName, providerType string) (workercontrollerservice.WorkerControllerComputeServiceClient, error) {
	switch {
	case slices.Contains(f.inProcess(namespaceName), providerType):
		return f.local, nil
	case slices.Contains(f.remoteFor(namespaceName), providerType):
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
