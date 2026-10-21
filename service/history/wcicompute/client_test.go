package wcicompute

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/computeservice"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/api/workercontrollerservice/v1"
	"go.temporal.io/server/chasm/lib/wci"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testDC(settings dynamicconfig.StaticClient) *dynamicconfig.Collection {
	if settings == nil {
		settings = dynamicconfig.StaticClient{}
	}
	if _, ok := settings[client.WorkerControllerEnabledComputeProviders.Key()]; !ok {
		settings[client.WorkerControllerEnabledComputeProviders.Key()] = []string{"test-invoke", "aws-lambda"}
	}
	return dynamicconfig.NewCollection(settings, log.NewNoopLogger())
}

func newTestClient(t *testing.T, settings dynamicconfig.StaticClient, remote *RemoteClient) *Client {
	t.Helper()
	dc := testDC(settings)
	if remote == nil {
		remote = newRemoteClient(func() bool { return true }, func() ([]membership.HostInfo, error) { return nil, nil }, nil)
	}
	return NewClient(dc, NewFactory(dc, remote)).(*Client)
}

// serveRemote runs the compute service on loopback, as the WCI process would.
func serveRemote(t *testing.T, dc *dynamicconfig.Collection) *RemoteClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	workercontrollerservice.RegisterWorkerControllerComputeServiceServer(server, computeservice.NewServer(dc))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	remote := newRemoteClient(
		func() bool { return true },
		func() ([]membership.HostInfo, error) {
			return []membership.HostInfo{membership.NewHostInfoFromAddress(listener.Addr().String())}, nil
		},
		func(address string) (*grpc.ClientConn, error) {
			return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	)
	t.Cleanup(remote.close)
	return remote
}

var rc = compute.RequestContext{NamespaceID: "ns-id", NamespaceName: "ns", DeploymentName: "dep", DeploymentBuildID: "build"}

func spec(t *testing.T, providerType iface.ComputeProviderType, config map[string]any) *iface.ComputeProviderSpec {
	t.Helper()
	payload, err := sdk.PreferProtoDataConverter.ToPayload(config)
	require.NoError(t, err)
	return &iface.ComputeProviderSpec{ProviderType: providerType, Config: payload}
}

func TestFactoryRouting(t *testing.T) {
	c := newTestClient(t, dynamicconfig.StaticClient{
		wci.RemoteComputeProviders.Key(): []string{"aws-lambda", "test-invoke"},
	}, nil)

	_, err := c.factory.For("test-invoke")
	require.NoError(t, err)
	svc, _ := c.factory.For("test-invoke")
	require.IsType(t, localClient{}, svc, "in-process wins over remote")

	svc, err = c.factory.For("aws-lambda")
	require.NoError(t, err)
	require.IsType(t, &RemoteClient{}, svc)

	_, err = c.factory.For("gcp-cloud-run")
	require.ErrorIs(t, err, wci.ErrProviderNotSupportedOnCHASM, "in neither list: stays a workflow")
}

func TestSupports(t *testing.T) {
	settings := dynamicconfig.StaticClient{wci.RemoteComputeProviders.Key(): []string{"aws-lambda"}}
	configured := newTestClient(t, settings, nil)
	require.True(t, configured.Supports("test-invoke"), "in process")
	require.True(t, configured.Supports("aws-lambda"), "served by the WCI process")
	require.False(t, configured.Supports("gcp-cloud-run"), "in neither list")

	notReachable := newTestClient(t, settings, newRemoteClient(func() bool { return false }, nil, nil))
	require.False(t, notReachable.Supports("aws-lambda"), "history can't resolve the WCI service, so it stays a workflow")
	require.True(t, notReachable.Supports("test-invoke"), "in-process types don't depend on it")
}

func TestInProcessAllowList(t *testing.T) {
	c := newTestClient(t, nil, nil)

	require.NoError(t, c.InvokeWorker(t.Context(), rc, spec(t, "test-invoke", map[string]any{}), "1"))
	require.ErrorIs(t, c.InvokeWorker(t.Context(), rc, spec(t, "aws-lambda", map[string]any{}), "1"), wci.ErrProviderNotSupportedOnCHASM)
}

func TestNotEnabledIsUnsupported(t *testing.T) {
	c := newTestClient(t, dynamicconfig.StaticClient{
		wci.InProcessComputeProviders.Key():                  []string{"test-worker-set"},
		client.WorkerControllerEnabledComputeProviders.Key(): []string{"test-invoke"},
	}, nil)

	err := c.UpdateWorkerSetSize(t.Context(), rc, spec(t, "test-worker-set", map[string]any{}), 2)
	require.ErrorIs(t, err, wci.ErrProviderNotSupportedOnCHASM, "allow-listed but not enabled for the namespace")
}

func TestValidateSpecReachesProvider(t *testing.T) {
	c := newTestClient(t, nil, nil)

	// test-invoke rejects this key in ValidateConfig, proving the call reached the provider.
	err := c.ValidateSpec(t.Context(), rc, &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{
		"g": {Compute: *spec(t, "test-invoke", map[string]any{"illegal_field": "x"})},
	}})
	require.ErrorContains(t, err, "g: illegal_field")
	var pErr *compute.ProviderError
	require.ErrorAs(t, err, &pErr, "a provider failure stays classified")
}

func TestRemoteMatchesLocal(t *testing.T) {
	remoteSettings := dynamicconfig.StaticClient{
		wci.InProcessComputeProviders.Key(): []string{},
		wci.RemoteComputeProviders.Key():    []string{"test-invoke"},
	}
	remote := newTestClient(t, remoteSettings, serveRemote(t, testDC(nil)))
	local := newTestClient(t, nil, nil)

	for name, c := range map[string]*Client{"local": local, "remote": remote} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, c.InvokeWorker(t.Context(), rc, spec(t, "test-invoke", map[string]any{}), "42"))

			err := c.ValidateSpec(t.Context(), rc, &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{
				"g": {Compute: *spec(t, "test-invoke", map[string]any{"illegal_field": "x"})},
			}})
			require.ErrorContains(t, err, "g: illegal_field found in config")
			var pErr *compute.ProviderError
			require.ErrorAs(t, err, &pErr)
		})
	}
}

func TestRemoteUnavailableIsRetryable(t *testing.T) {
	settings := dynamicconfig.StaticClient{
		wci.InProcessComputeProviders.Key(): []string{},
		wci.RemoteComputeProviders.Key():    []string{"test-invoke"},
	}
	noHosts := newTestClient(t, settings, nil)
	err := noHosts.InvokeWorker(t.Context(), rc, spec(t, "test-invoke", map[string]any{}), "1")
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable, "no WCI host: retry")
	var pErr *compute.ProviderError
	require.NotErrorAs(t, err, &pErr, "an unknown outcome is not a provider failure")

	unresolvable := newTestClient(t, settings, newRemoteClient(func() bool { return false }, func() ([]membership.HostInfo, error) {
		return nil, membership.ErrUnknownService
	}, nil))
	require.ErrorAs(t, unresolvable.InvokeWorker(t.Context(), rc, spec(t, "test-invoke", map[string]any{}), "1"), &unavailable,
		"history not configured with the WCI service: retry")
}

type recordingService struct {
	workercontrollerservice.WorkerControllerComputeServiceClient
	invoke *workercontrollerservice.InvokeWorkerRequest
	resize *workercontrollerservice.UpdateWorkerSetSizeRequest
}

func (*recordingService) Configured() bool { return true }

func (r *recordingService) InvokeWorker(_ context.Context, req *workercontrollerservice.InvokeWorkerRequest, _ ...grpc.CallOption) (*workercontrollerservice.InvokeWorkerResponse, error) {
	r.invoke = req
	return &workercontrollerservice.InvokeWorkerResponse{Failure: &workercontrollerservice.ComputeFailure{FailureClass: workercontrollerservice.FAILURE_CLASS_THROTTLED, Message: "slow down"}}, nil
}

func (r *recordingService) UpdateWorkerSetSize(_ context.Context, req *workercontrollerservice.UpdateWorkerSetSizeRequest, _ ...grpc.CallOption) (*workercontrollerservice.UpdateWorkerSetSizeResponse, error) {
	r.resize = req
	return &workercontrollerservice.UpdateWorkerSetSizeResponse{}, nil
}

func TestRequestFieldsAndFailureClassReachTheCaller(t *testing.T) {
	recorder := &recordingService{}
	c := &Client{
		factory:     &Factory{remote: recorder, inProcess: func() []string { return nil }, remoteFor: func() []string { return []string{"aws-lambda"} }},
		concurrency: func(string) int { return 1 },
		limiters:    map[string]chan struct{}{},
	}

	err := c.InvokeWorker(t.Context(), rc, spec(t, "aws-lambda", map[string]any{}), "42")
	require.Equal(t, "42", recorder.invoke.GetRequestId())
	require.Equal(t, "ns-id", recorder.invoke.GetContext().GetNamespaceId())
	require.Equal(t, "aws-lambda", recorder.invoke.GetProvider().GetProviderType())
	var pErr *compute.ProviderError
	require.ErrorAs(t, err, &pErr)
	require.Equal(t, compute.FailureThrottled, pErr.Class)

	require.NoError(t, c.UpdateWorkerSetSize(t.Context(), rc, spec(t, "aws-lambda", map[string]any{}), 7))
	require.EqualValues(t, 7, recorder.resize.GetWorkerSetSize())
}

func TestConcurrencyLimitWaitsOnContext(t *testing.T) {
	c := newTestClient(t, dynamicconfig.StaticClient{wci.ComputeConcurrencyPerNamespace.Key(): 1}, nil)

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
