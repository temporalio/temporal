package wcicompute

import (
	"context"
	"sync"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/workercontrollerservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// workerControllerService is ASW's wci.ServiceName. History resolves it only when its own services
// config lists it; until then every remote call is Unavailable.
const workerControllerService primitives.ServiceName = "worker-controller-workers"

// RemoteClient calls the compute service on the WCI process. Any host can serve any call, since
// the cloud identity is process-wide, so it round-robins over the members.
type RemoteClient struct {
	// configured is false while history's services config doesn't list the WCI service.
	configured func() bool
	hosts      func() ([]membership.HostInfo, error)
	dial       func(address string) (*grpc.ClientConn, error)
	next       atomic.Uint64

	connsMu sync.Mutex
	conns   map[string]*grpc.ClientConn
}

var _ workercontrollerservice.WorkerControllerComputeServiceClient = (*RemoteClient)(nil)

type remoteClientParams struct {
	fx.In

	Lifecycle      fx.Lifecycle
	Monitor        membership.Monitor
	TLS            encryption.TLSConfigProvider `optional:"true"`
	Logger         log.Logger
	MetricsHandler metrics.Handler
}

func NewRemoteClient(p remoteClientParams) *RemoteClient {
	c := newRemoteClient(
		func() bool {
			_, err := p.Monitor.GetResolver(workerControllerService)
			return err == nil
		},
		func() ([]membership.HostInfo, error) {
			resolver, err := p.Monitor.GetResolver(workerControllerService)
			if err != nil {
				return nil, err
			}
			return resolver.AvailableMembers(), nil
		},
		func(address string) (*grpc.ClientConn, error) {
			if p.TLS == nil {
				return rpc.Dial(address, nil, p.Logger, p.MetricsHandler)
			}
			tlsConfig, err := p.TLS.GetInternodeClientConfig()
			if err != nil {
				return nil, err
			}
			return rpc.Dial(address, tlsConfig, p.Logger, p.MetricsHandler)
		},
	)
	p.Lifecycle.Append(fx.StopHook(c.close))
	return c
}

func newRemoteClient(configured func() bool, hosts func() ([]membership.HostInfo, error), dial func(string) (*grpc.ClientConn, error)) *RemoteClient {
	return &RemoteClient{configured: configured, hosts: hosts, dial: dial, conns: map[string]*grpc.ClientConn{}}
}

func (c *RemoteClient) Configured() bool {
	return c.configured()
}

func (c *RemoteClient) ValidateConfig(ctx context.Context, req *workercontrollerservice.ValidateConfigRequest, opts ...grpc.CallOption) (*workercontrollerservice.ValidateConfigResponse, error) {
	client, err := c.client()
	if err != nil {
		return nil, err
	}
	return client.ValidateConfig(ctx, req, opts...)
}

func (c *RemoteClient) InvokeWorker(ctx context.Context, req *workercontrollerservice.InvokeWorkerRequest, opts ...grpc.CallOption) (*workercontrollerservice.InvokeWorkerResponse, error) {
	client, err := c.client()
	if err != nil {
		return nil, err
	}
	return client.InvokeWorker(ctx, req, opts...)
}

func (c *RemoteClient) UpdateWorkerSetSize(ctx context.Context, req *workercontrollerservice.UpdateWorkerSetSizeRequest, opts ...grpc.CallOption) (*workercontrollerservice.UpdateWorkerSetSizeResponse, error) {
	client, err := c.client()
	if err != nil {
		return nil, err
	}
	return client.UpdateWorkerSetSize(ctx, req, opts...)
}

// client picks the next host; a resolution or dial failure is Unavailable, so the task retries.
func (c *RemoteClient) client() (workercontrollerservice.WorkerControllerComputeServiceClient, error) {
	hosts, err := c.hosts()
	if err != nil {
		return nil, serviceerror.NewUnavailablef("resolving %s: %v", workerControllerService, err)
	}
	if len(hosts) == 0 {
		return nil, serviceerror.NewUnavailablef("no %s hosts available", workerControllerService)
	}
	host := hosts[(c.next.Add(1)-1)%uint64(len(hosts))]
	conn, err := c.conn(host.GetAddress())
	if err != nil {
		return nil, serviceerror.NewUnavailablef("dialing %s: %v", host.GetAddress(), err)
	}
	return workercontrollerservice.NewWorkerControllerComputeServiceClient(conn), nil
}

func (c *RemoteClient) conn(address string) (*grpc.ClientConn, error) {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	if conn, ok := c.conns[address]; ok && conn.GetState() != connectivity.Shutdown {
		return conn, nil
	}
	conn, err := c.dial(address)
	if err != nil {
		return nil, err
	}
	c.conns[address] = conn
	return conn, nil
}

func (c *RemoteClient) close() {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	for address, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, address)
	}
}
