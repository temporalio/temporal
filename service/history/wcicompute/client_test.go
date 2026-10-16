package wcicompute

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/chasm/lib/wci"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/sdk"
)

func newTestClient(t *testing.T, settings dynamicconfig.StaticClient) *Client {
	t.Helper()
	if settings == nil {
		settings = dynamicconfig.StaticClient{}
	}
	settings[client.WorkerControllerEnabledComputeProviders.Key()] = []string{"test-invoke", "aws-lambda"}
	return NewClient(dynamicconfig.NewCollection(settings, log.NewNoopLogger())).(*Client)
}

var rc = compute.RequestContext{NamespaceName: "ns", DeploymentName: "dep", DeploymentBuildID: "build"}

func spec(t *testing.T, providerType iface.ComputeProviderType, config map[string]any) *iface.ComputeProviderSpec {
	t.Helper()
	payload, err := sdk.PreferProtoDataConverter.ToPayload(config)
	require.NoError(t, err)
	return &iface.ComputeProviderSpec{ProviderType: providerType, Config: payload}
}

func TestInProcessAllowList(t *testing.T) {
	c := newTestClient(t, nil)

	require.NoError(t, c.InvokeWorker(t.Context(), rc, spec(t, "test-invoke", map[string]any{})))
	require.ErrorIs(t, c.InvokeWorker(t.Context(), rc, spec(t, "aws-lambda", map[string]any{})), wci.ErrProviderNotSupportedOnCHASM)
}

func TestValidateSpecReachesProvider(t *testing.T) {
	c := newTestClient(t, nil)

	// test-invoke rejects this key in ValidateConfig, proving the call reached the provider.
	err := c.ValidateSpec(t.Context(), rc, &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{
		"g": {Compute: *spec(t, "test-invoke", map[string]any{"illegal_field": "x"})},
	}})
	require.ErrorContains(t, err, "g: illegal_field")
}

func TestConcurrencyLimitWaitsOnContext(t *testing.T) {
	c := newTestClient(t, dynamicconfig.StaticClient{wci.ComputeConcurrencyPerNamespace.Key(): 1})

	release, err := c.acquire(t.Context(), "ns")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = c.acquire(ctx, "ns")
	require.ErrorIs(t, err, context.DeadlineExceeded, "a full namespace waits, bounded by the task context")

	other, err := c.acquire(t.Context(), "other-ns")
	require.NoError(t, err, "limits are per namespace")
	other()

	release()
	again, err := c.acquire(t.Context(), "ns")
	require.NoError(t, err)
	again()
}
