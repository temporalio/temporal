// Package wcicompute runs WCI provider operations for CHASM through the compute service, in process
// or on the WCI process. It is the only SRV package that links compute_provider, and history wires
// it; chasm/lib/wci sees only the ComputeClient interface.
package wcicompute

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/computeservice"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/api/workercontrollerservice/v1"
	"go.temporal.io/server/chasm/lib/wci"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewRemoteClient),
	fx.Provide(NewFactory),
	fx.Provide(NewClient),
)

// Client is CHASM's ComputeClient. It keeps what protects history (the per-namespace concurrency
// limit) and leaves where a provider runs to the Factory.
type Client struct {
	factory     *Factory
	concurrency dynamicconfig.IntPropertyFnWithNamespaceFilter

	limitersMu sync.Mutex
	limiters   map[string]chan struct{}
}

var _ wci.ComputeClient = (*Client)(nil)

func NewClient(dc *dynamicconfig.Collection, factory *Factory) wci.ComputeClient {
	return &Client{
		factory:     factory,
		concurrency: wci.ComputeConcurrencyPerNamespace.Get(dc),
		limiters:    map[string]chan struct{}{},
	}
}

func (c *Client) Supports(namespaceName, providerType string) bool {
	return c.factory.Supports(namespaceName, providerType)
}

func (c *Client) ValidateSpec(ctx context.Context, rc compute.RequestContext, spec *iface.WorkerControllerInstanceSpec) error {
	for id, group := range spec.ScalingGroupSpecs {
		err := c.call(ctx, rc, &group.Compute, func(svc workercontrollerservice.WorkerControllerComputeServiceClient, req *workercontrollerservice.ComputeRequestContext, provider *workercontrollerservice.ComputeProviderConfig) (*workercontrollerservice.ComputeFailure, error) {
			resp, err := svc.ValidateConfig(ctx, &workercontrollerservice.ValidateConfigRequest{Context: req, Provider: provider})
			return resp.GetFailure(), err
		})
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
	}
	return nil
}

func (c *Client) InvokeWorker(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec, requestID string) error {
	return c.call(ctx, rc, cfg, func(svc workercontrollerservice.WorkerControllerComputeServiceClient, req *workercontrollerservice.ComputeRequestContext, provider *workercontrollerservice.ComputeProviderConfig) (*workercontrollerservice.ComputeFailure, error) {
		resp, err := svc.InvokeWorker(ctx, &workercontrollerservice.InvokeWorkerRequest{Context: req, Provider: provider, RequestId: requestID})
		return resp.GetFailure(), err
	})
}

func (c *Client) UpdateWorkerSetSize(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec, size int32) error {
	return c.call(ctx, rc, cfg, func(svc workercontrollerservice.WorkerControllerComputeServiceClient, req *workercontrollerservice.ComputeRequestContext, provider *workercontrollerservice.ComputeProviderConfig) (*workercontrollerservice.ComputeFailure, error) {
		resp, err := svc.UpdateWorkerSetSize(ctx, &workercontrollerservice.UpdateWorkerSetSizeRequest{Context: req, Provider: provider, WorkerSetSize: size})
		return resp.GetFailure(), err
	})
}

// call returns a classified provider error for a definite failure, and the RPC error, which the
// task treats as retryable, when the outcome is unknown.
func (c *Client) call(
	ctx context.Context,
	rc compute.RequestContext,
	spec *iface.ComputeProviderSpec,
	fn func(workercontrollerservice.WorkerControllerComputeServiceClient, *workercontrollerservice.ComputeRequestContext, *workercontrollerservice.ComputeProviderConfig) (*workercontrollerservice.ComputeFailure, error),
) error {
	svc, err := c.factory.For(rc.NamespaceName, string(spec.ProviderType))
	if err != nil {
		return err
	}
	release, err := c.acquire(ctx, rc.NamespaceName)
	if err != nil {
		return err
	}
	defer release()

	failure, err := fn(svc, computeservice.RequestContextToProto(rc), &workercontrollerservice.ComputeProviderConfig{
		ProviderType: string(spec.ProviderType),
		Config:       spec.Config,
	})
	if err != nil {
		return err
	}
	if failure.GetFailureClass() == workercontrollerservice.FAILURE_CLASS_UNSUPPORTED {
		// Bare, as before: callers match this sentinel by status and message after it crosses gRPC.
		return wci.ErrProviderNotSupportedOnCHASM
	}
	return computeservice.FailureFromProto(failure)
}

// acquire waits for a per-namespace slot; the wait is bounded by the task's context.
func (c *Client) acquire(ctx context.Context, namespace string) (func(), error) {
	c.limitersMu.Lock()
	sem, ok := c.limiters[namespace]
	if !ok {
		sem = make(chan struct{}, max(c.concurrency(namespace), 1))
		c.limiters[namespace] = sem
	}
	c.limitersMu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
