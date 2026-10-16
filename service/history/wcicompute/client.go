// Package wcicompute runs WCI provider operations for CHASM. It is the only SRV package that links
// compute_provider, and history wires it; chasm/lib/wci sees only the ComputeClient interface.
package wcicompute

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/auto-scaled-workers/wci/compute"
	computeprovider "go.temporal.io/auto-scaled-workers/wci/workflow/compute_provider"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/chasm/lib/wci"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/sdk"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewClient),
)

// Client runs allow-listed providers in-process; cloud providers wait for the delegated client (step 7).
type Client struct {
	dc          *dynamicconfig.Collection
	inProcess   dynamicconfig.TypedPropertyFn[[]string]
	concurrency dynamicconfig.IntPropertyFnWithNamespaceFilter

	limitersMu sync.Mutex
	limiters   map[string]chan struct{}
}

var _ wci.ComputeClient = (*Client)(nil)

func NewClient(dc *dynamicconfig.Collection) wci.ComputeClient {
	return &Client{
		dc:          dc,
		inProcess:   wci.InProcessComputeProviders.Get(dc),
		concurrency: wci.ComputeConcurrencyPerNamespace.Get(dc),
		limiters:    map[string]chan struct{}{},
	}
}

func (c *Client) ValidateSpec(ctx context.Context, rc compute.RequestContext, spec *iface.WorkerControllerInstanceSpec) error {
	for id, group := range spec.ScalingGroupSpecs {
		err := c.call(ctx, rc, &group.Compute, func(p compute.ComputeProvider, cfg compute.ComputeProviderConfig) error {
			return p.ValidateConfig(ctx, rc, cfg)
		})
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
	}
	return nil
}

func (c *Client) InvokeWorker(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec) error {
	return c.call(ctx, rc, cfg, func(p compute.ComputeProvider, config compute.ComputeProviderConfig) error {
		return p.InvokeWorker(ctx, rc, config)
	})
}

func (c *Client) UpdateWorkerSetSize(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec, size int32) error {
	return c.call(ctx, rc, cfg, func(p compute.ComputeProvider, config compute.ComputeProviderConfig) error {
		return p.UpdateWorkerSetSize(ctx, rc, config, size)
	})
}

func (c *Client) call(
	ctx context.Context,
	rc compute.RequestContext,
	spec *iface.ComputeProviderSpec,
	fn func(compute.ComputeProvider, compute.ComputeProviderConfig) error,
) error {
	if !slices.Contains(c.inProcess(), string(spec.ProviderType)) {
		return wci.ErrProviderNotSupportedOnCHASM
	}
	provider, err := computeprovider.GetComputeProvider(ctx, spec.ProviderType, rc.NamespaceName, c.dc)
	if err != nil {
		return err
	}
	if provider == nil {
		// Not enabled for the namespace (workercontroller.compute_providers.enabled).
		return wci.ErrProviderNotSupportedOnCHASM
	}
	config := compute.ComputeProviderConfig{}
	if err := sdk.PreferProtoDataConverter.FromPayload(spec.Config, &config); err != nil {
		return compute.NewProviderError(compute.FailureRejected, fmt.Errorf("invalid compute config: %w", err))
	}

	release, err := c.acquire(ctx, rc.NamespaceName)
	if err != nil {
		return err
	}
	defer release()
	return fn(provider, config)
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
