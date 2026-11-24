package failpointgrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// callCtx returns a context for one fan-out call, so a regression fails instead of hanging.
func callCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// failingService is a FailpointService with its own registry whose every request fails with code.
type failingService struct {
	failpointpb.UnimplementedFailpointServiceServer
	id   string
	code codes.Code
}

func (f failingService) DescribeRegistry(context.Context, *failpointpb.DescribeRegistryRequest) (*failpointpb.DescribeRegistryResponse, error) {
	return &failpointpb.DescribeRegistryResponse{RegistryId: f.id}, nil
}

func (f failingService) InjectReturn(context.Context, *failpointpb.InjectReturnRequest) (*failpointpb.InjectReturnResponse, error) {
	return nil, status.Error(f.code, "boom")
}

func (f failingService) ListFailpoints(context.Context, *failpointpb.ListFailpointsRequest) (*failpointpb.ListFailpointsResponse, error) {
	return nil, status.Error(f.code, "boom")
}

// fanOutTargets returns a local target and two peers, each backed by its own registry.
func fanOutTargets() (failpointgrpc.Target, []failpointgrpc.Target, []failpoint.Registry) {
	registries := []failpoint.Registry{failpoint.NewRegistry(), failpoint.NewRegistry(), failpoint.NewRegistry()}
	local := failpointgrpc.Target{Host: "local", Service: failpointgrpc.NewServer(registries[0])}
	peers := []failpointgrpc.Target{
		{Host: "peer-a", Service: failpointgrpc.NewServer(registries[1])},
		{Host: "peer-b", Service: failpointgrpc.NewServer(registries[2])},
	}
	return local, peers, registries
}

func staticPeers(peers ...failpointgrpc.Target) func(context.Context) ([]failpointgrpc.Target, error) {
	return func(context.Context) ([]failpointgrpc.Target, error) { return peers, nil }
}

func TestFanOut_AppliesToLocalAndPeers(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))

	_, err := fanOut.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1", ErrorMessage: "injected"})
	require.NoError(t, err)
	for _, registry := range registries {
		require.NotNil(t, registry.Get(key1))
	}

	_, err = fanOut.DisableFailpoint(callCtx(t), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)
	for _, registry := range registries {
		require.Empty(t, registry.Keys())
	}

	_, err = fanOut.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
	require.NoError(t, err)
	_, err = fanOut.InjectPanic(callCtx(t), &failpointpb.InjectPanicRequest{Key: "2", PanicValue: "boom"})
	require.NoError(t, err)
	for _, registry := range registries {
		require.Len(t, registry.Keys(), 2)
	}

	_, err = fanOut.DisableAllFailpoints(callCtx(t), &failpointpb.DisableAllFailpointsRequest{})
	require.NoError(t, err)
	for _, registry := range registries {
		require.Empty(t, registry.Keys())
	}
}

func TestFanOut_AggregatesErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		failing  []failpointgrpc.Target
		wantCode codes.Code
		wantMsgs []string
	}{
		{
			name:     "one peer fails",
			failing:  []failpointgrpc.Target{{Host: "bad-a", Service: failingService{id: "a", code: codes.Unavailable}}},
			wantCode: codes.Unavailable,
			wantMsgs: []string{"1 of 4 targets", "bad-a: boom"},
		},
		{
			name: "peers fail with the same code",
			failing: []failpointgrpc.Target{
				{Host: "bad-a", Service: failingService{id: "a", code: codes.Unavailable}},
				{Host: "bad-b", Service: failingService{id: "b", code: codes.Unavailable}},
			},
			wantCode: codes.Unavailable,
			wantMsgs: []string{"2 of 5 targets", "bad-a: boom", "bad-b: boom"},
		},
		{
			name: "peers fail with different codes",
			failing: []failpointgrpc.Target{
				{Host: "bad-a", Service: failingService{id: "a", code: codes.Unavailable}},
				{Host: "bad-b", Service: failingService{id: "b", code: codes.Internal}},
			},
			wantCode: codes.Unknown,
			wantMsgs: []string{"2 of 5 targets", "bad-a: boom", "bad-b: boom"},
		},
		{
			name:     "peer could not be set up",
			failing:  []failpointgrpc.Target{{Host: "bad-a", Err: status.Error(codes.Unavailable, "dial failed")}},
			wantCode: codes.Unavailable,
			wantMsgs: []string{"1 of 4 targets", "bad-a: dial failed"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			local, peers, registries := fanOutTargets()
			fanOut := failpointgrpc.NewFanOut(local, staticPeers(append(peers, tc.failing...)...))

			_, err := fanOut.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
			require.Equal(t, tc.wantCode, status.Code(err))
			for _, msg := range tc.wantMsgs {
				require.ErrorContains(t, err, msg)
			}
			// The targets that succeeded keep the change.
			for _, registry := range registries {
				require.NotNil(t, registry.Get(key1))
			}

			_, err = fanOut.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
			require.Equal(t, tc.wantCode, status.Code(err))
		})
	}
}

func TestFanOut_StalledDescribe(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	stalled := failpointgrpc.Target{Host: "stalled", Service: stalledService{}}
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(append(peers, stalled)...),
		failpointgrpc.WithDescribeTimeout(time.Second))

	_, err := fanOut.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.ErrorContains(t, err, "1 of 4 targets")
	require.ErrorContains(t, err, "stalled:")
	// The healthy targets got the request.
	for _, registry := range registries {
		require.NotNil(t, registry.Get(key1))
	}
}

func TestFanOut_PeerLookupFails(t *testing.T) {
	t.Parallel()
	local, _, registries := fanOutTargets()
	fanOut := failpointgrpc.NewFanOut(local, func(context.Context) ([]failpointgrpc.Target, error) {
		return nil, errors.New("no membership")
	})

	_, err := fanOut.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "no membership")
	require.Empty(t, registries[0].Keys())
}

func TestFanOut_ListFailpointsSetsHosts(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	// peer-c serves the same registry as peer-a.
	peers = append(peers, failpointgrpc.Target{Host: "peer-c", Service: failpointgrpc.NewServer(registries[1])})
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))

	_, err := fanOut.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
	require.NoError(t, err)

	resp, err := fanOut.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	hosts := make([][]string, 0, len(resp.Failpoints))
	for _, info := range resp.Failpoints {
		require.Equal(t, "1", info.Key)
		hosts = append(hosts, info.Hosts)
	}
	require.ElementsMatch(t, [][]string{{"local"}, {"peer-a", "peer-c"}, {"peer-b"}}, hosts)
}

// countingService counts the InjectBlock requests to a FailpointService and runs afterInject
// after each.
type countingService struct {
	failpointpb.FailpointServiceServer
	injects     *atomic.Int32
	afterInject func()
}

func (c countingService) InjectBlock(ctx context.Context, req *failpointpb.InjectBlockRequest) (*failpointpb.InjectBlockResponse, error) {
	resp, err := c.FailpointServiceServer.InjectBlock(ctx, req)
	c.injects.Add(1)
	c.afterInject()
	return resp, err
}

// TestFanOut_SharedRegistry covers targets that serve the same registry, as the services of one
// process do. Applying InjectBlock to the registry a second time would release the execution that
// reached the block after the first.
func TestFanOut_SharedRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	executeDone := make(chan struct{})
	var fp *failpoint.Failpoint[error]
	t.Cleanup(func() {
		if fp != nil {
			fp.Disable()
			<-executeDone
		}
	})

	var once sync.Once
	var hitErr error
	// An execution reaches the block as soon as the first target has applied the injection.
	hitBlock := func() {
		once.Do(func() {
			fp = registry.Get(key1).(*failpoint.Failpoint[error])
			go func() {
				defer close(executeDone)
				_, _ = fp.Execute()
			}()
			hitErr = fp.AwaitBlock(callCtx(t))
		})
	}
	var injects atomic.Int32
	local := failpointgrpc.Target{Host: "entry", Service: countingService{failpointgrpc.NewServer(registry), &injects, hitBlock}}
	peer := failpointgrpc.Target{Host: "peer", Service: countingService{failpointgrpc.NewServer(registry), &injects, hitBlock}}
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peer))

	_, err := fanOut.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
	require.NoError(t, err)
	require.NoError(t, hitErr)
	require.Equal(t, int32(1), injects.Load())
	require.Never(t, func() bool {
		select {
		case <-executeDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond, "the execution must stay blocked")
}

// receive receives from ch, or fails the test if nothing arrives in time.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for a result")
		var zero T
		return zero
	}
}

// failingWait is a FailpointService whose WaitForBlock fails.
type failingWait struct {
	failpointpb.FailpointServiceServer
}

func (failingWait) WaitForBlock(context.Context, *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	return nil, status.Error(codes.Unavailable, "connection lost")
}

// stalledService is a FailpointService whose DescribeRegistry doesn't return until its context
// is done.
type stalledService struct {
	failpointpb.UnimplementedFailpointServiceServer
}

func (stalledService) DescribeRegistry(ctx context.Context, _ *failpointpb.DescribeRegistryRequest) (*failpointpb.DescribeRegistryResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// waitNotifier reports the result of every WaitForBlock request to a FailpointService.
type waitNotifier struct {
	failpointpb.FailpointServiceServer
	done chan<- error
}

func (w waitNotifier) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	resp, err := w.FailpointServiceServer.WaitForBlock(ctx, req)
	w.done <- err
	return resp, err
}

// waitRecorder is a FailpointService with its own registry whose WaitForBlock waits until its
// context is done and records why.
type waitRecorder struct {
	failpointpb.UnimplementedFailpointServiceServer
	done chan error
}

func (w waitRecorder) DescribeRegistry(context.Context, *failpointpb.DescribeRegistryRequest) (*failpointpb.DescribeRegistryResponse, error) {
	return &failpointpb.DescribeRegistryResponse{RegistryId: "recorder"}, nil
}

func (w waitRecorder) WaitForBlock(ctx context.Context, _ *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	<-ctx.Done()
	w.done <- ctx.Err()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestFanOut_WaitForBlock(t *testing.T) {
	t.Parallel()

	// blockedExecution starts an execution of the block failpoint for key1 in registry and
	// releases it when the test ends.
	blockedExecution := func(t *testing.T, registry failpoint.Registry) {
		fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
		require.True(t, ok)
		executeDone := make(chan struct{})
		t.Cleanup(func() {
			fp.Disable()
			<-executeDone
		})
		go func() {
			defer close(executeDone)
			_, _ = fp.Execute()
		}()
	}

	t.Run("succeeds when any target reports the block and cancels the others", func(t *testing.T) {
		t.Parallel()
		local, peers, registries := fanOutTargets()
		recorder := waitRecorder{done: make(chan error, 1)}
		fanOut := failpointgrpc.NewFanOut(local, staticPeers(append(peers, failpointgrpc.Target{Host: "recorder", Service: recorder})...))
		_, err := fanOut.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
		require.ErrorContains(t, err, "recorder") // The recorder doesn't implement InjectBlock.

		// Only peer-b reaches the block; the other targets never do.
		blockedExecution(t, registries[2])

		_, err = fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
		require.NoError(t, err)
		select {
		case err := <-recorder.done:
			require.ErrorIs(t, err, context.Canceled)
		case <-callCtx(t).Done():
			require.FailNow(t, "the losing wait was not cancelled")
		}
	})

	t.Run("ignores a target that is not configured to block", func(t *testing.T) {
		t.Parallel()
		local, peers, registries := fanOutTargets()
		// peer-a is stale: its failpoint returns instead of blocking. The test proceeds once its
		// wait has failed.
		staleDone := make(chan error, 1)
		peers[0].Service = waitNotifier{FailpointServiceServer: peers[0].Service, done: staleDone}
		fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))
		_, err := fanOut.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
		require.NoError(t, err)
		_, err = peers[0].Service.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
		require.NoError(t, err)

		waitDone := make(chan error, 1)
		go func() {
			_, err := fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
			waitDone <- err
		}()
		require.Equal(t, codes.FailedPrecondition, status.Code(receive(t, staleDone)))
		require.Empty(t, waitDone, "the stale peer's failure must not end the wait")

		blockedExecution(t, registries[2])
		require.NoError(t, receive(t, waitDone))
	})

	t.Run("does not wait for slow peers or a slow peer lookup", func(t *testing.T) {
		t.Parallel()
		local, _, registries := fanOutTargets()
		_, err := local.Service.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
		require.NoError(t, err)
		blockedExecution(t, registries[0])

		fanOut := failpointgrpc.NewFanOut(local, staticPeers(failpointgrpc.Target{Host: "stalled", Service: stalledService{}}))
		_, err = fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
		require.NoError(t, err)

		// The lookup is cancelled once the wait succeeds.
		lookupDone := make(chan error, 1)
		fanOut = failpointgrpc.NewFanOut(local, func(ctx context.Context) ([]failpointgrpc.Target, error) {
			<-ctx.Done()
			lookupDone <- ctx.Err()
			return nil, ctx.Err()
		})
		_, err = fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
		require.NoError(t, err)
		require.ErrorIs(t, receive(t, lookupDone), context.Canceled)
	})

	t.Run("a failed wait on another endpoint of the same registry does not hide the block", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		local := failpointgrpc.Target{Host: "entry", Service: failpointgrpc.NewServer(registry)}
		// The peer serves the same registry, but its wait fails.
		peer := failpointgrpc.Target{Host: "peer", Service: failingWait{failpointgrpc.NewServer(registry)}}
		fanOut := failpointgrpc.NewFanOut(local, staticPeers(peer))
		_, err := local.Service.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
		require.NoError(t, err)
		blockedExecution(t, registry)

		_, err = fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
		require.NoError(t, err)
	})

	t.Run("respects the context", func(t *testing.T) {
		t.Parallel()
		local, peers, _ := fanOutTargets()
		fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))
		_, err := fanOut.InjectBlock(callCtx(t), &failpointpb.InjectBlockRequest{Key: "1"})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err = fanOut.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Key: "1"})
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})

	t.Run("fails when no target can report the block", func(t *testing.T) {
		t.Parallel()
		local, peers, _ := fanOutTargets()
		fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))
		_, err := peers[0].Service.InjectReturn(callCtx(t), &failpointpb.InjectReturnRequest{Key: "1"})
		require.NoError(t, err)

		_, err = fanOut.WaitForBlock(callCtx(t), &failpointpb.WaitForBlockRequest{Key: "1"})
		require.Equal(t, codes.Unknown, status.Code(err)) // NotFound and FailedPrecondition.
		require.ErrorContains(t, err, "3 of 3 targets")
	})
}

// serveFailpoints serves service on a local port and returns a client for it. headers, if not nil,
// receives the authorization headers of the requests the server receives while it has room.
func serveFailpoints(t *testing.T, service failpointpb.FailpointServiceServer, headers chan<- []string) failpointpb.FailpointServiceServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		select {
		case headers <- md.Get("authorization"):
		default:
		}
		return handler(ctx, req)
	}))
	failpointpb.RegisterFailpointServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return failpointgrpc.NewRemote(conn)
}

// TestFanOut_PeerFanOuts covers fan-outs that are each other's peers, such as several entry points
// of one system: a forwarded request must not be forwarded again, and carries the caller's
// authorization header to peers that authorize it, but not to the other services.
func TestFanOut_PeerFanOuts(t *testing.T) {
	t.Parallel()
	registryA, registryB, registryC := failpoint.NewRegistry(), failpoint.NewRegistry(), failpoint.NewRegistry()
	var remoteA, remoteB, remoteC failpointpb.FailpointServiceServer
	fanOutA := failpointgrpc.NewFanOut(
		failpointgrpc.Target{Host: "a", Service: failpointgrpc.NewServer(registryA)},
		staticPeersFunc(func() []failpointgrpc.Target {
			return []failpointgrpc.Target{
				{Host: "b", Service: remoteB, ForwardHeaders: true},
				{Host: "c", Service: remoteC},
			}
		}),
		failpointgrpc.WithForwardedHeaders("authorization"),
	)
	fanOutB := failpointgrpc.NewFanOut(
		failpointgrpc.Target{Host: "b", Service: failpointgrpc.NewServer(registryB)},
		staticPeersFunc(func() []failpointgrpc.Target {
			return []failpointgrpc.Target{{Host: "a", Service: remoteA, ForwardHeaders: true}}
		}),
		failpointgrpc.WithForwardedHeaders("authorization"),
	)
	headersB, headersC := make(chan []string, 100), make(chan []string, 100)
	remoteA = serveFailpoints(t, fanOutA, nil)
	remoteB = serveFailpoints(t, fanOutB, headersB)
	remoteC = serveFailpoints(t, failpointgrpc.NewServer(registryC), headersC)

	ctx := metadata.AppendToOutgoingContext(callCtx(t), "authorization", "Bearer token")
	_, err := remoteA.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Key: "1"})
	require.NoError(t, err)
	for _, registry := range []failpoint.Registry{registryA, registryB, registryC} {
		require.NotNil(t, registry.Get(key1))
	}
	// DescribeRegistry and InjectReturn reached B with the credentials and C without them.
	require.Len(t, headersB, 2)
	for len(headersB) > 0 {
		require.Equal(t, []string{"Bearer token"}, <-headersB)
	}
	require.Len(t, headersC, 2)
	for len(headersC) > 0 {
		require.Empty(t, <-headersC)
	}

	// Each fan-out reports its own failpoint once; without the forwarded marker, B would
	// report A's failpoint again (and the forwarding would never end).
	resp, err := remoteB.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	hosts := make([][]string, 0, len(resp.Failpoints))
	for _, info := range resp.Failpoints {
		hosts = append(hosts, info.Hosts)
	}
	require.ElementsMatch(t, [][]string{{"a"}, {"b"}}, hosts)
}

func staticPeersFunc(peers func() []failpointgrpc.Target) func(context.Context) ([]failpointgrpc.Target, error) {
	return func(context.Context) ([]failpointgrpc.Target, error) { return peers(), nil }
}

func TestFanOut_ListFailpointsReturnsBuggifyKeys(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	peers = append(peers, failpointgrpc.Target{Host: "peer-c", Service: failpointgrpc.NewServer(registries[1])})
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))
	// Only the registries of local and peer-a run with BUGGIFY, and only peer-a's key was hit.
	for i, hits := range []int{0, 2} {
		require.NoError(t, registries[i].SetBuggify(failpoint.BuggifyConfig{Seed: 1, Activation: 1}))
		for range hits {
			_ = failpoint.Buggify(registrySource{registry: registries[i]}, failpoint.KeyFromName("on", ""))
		}
	}

	resp, err := fanOut.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.BuggifyKeys, 1)
	require.True(t, proto.Equal(&failpointpb.BuggifyKeyInfo{
		Key: "on", Activated: true, Hits: 2, Hosts: []string{"peer-a", "peer-c"},
	}, resp.BuggifyKeys[0]), resp.BuggifyKeys[0])
}

func TestFanOut_ListFailpointsBoundsBuggifyKeys(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	for _, registry := range registries {
		manyBuggifyKeys(t, registry)
	}
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))

	resp, err := fanOut.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Less(t, proto.Size(resp), 2<<20)
	require.Equal(t, int64(3*12003), int64(len(resp.BuggifyKeys))+resp.BuggifyKeysOmitted)
	for _, info := range resp.BuggifyKeys[:9] {
		require.True(t, info.Activated)
	}
}

func TestFanOut_ListFailpointsBudgetsBuggifyKeysAgainstTheResponse(t *testing.T) {
	t.Parallel()
	local, peers, registries := fanOutTargets()
	// Each registry's response fits, but the failpoints of all of them take most of the room.
	for _, registry := range registries {
		for i := range 3000 {
			key := failpoint.KeyFromName(fmt.Sprintf("b%s%d", strings.Repeat("y", 200), i), "")
			registry.Set(key, failpoint.NewFailpoint[error](key))
		}
		manyBuggifyKeys(t, registry)
	}
	fanOut := failpointgrpc.NewFanOut(local, staticPeers(peers...))

	resp, err := fanOut.ListFailpoints(callCtx(t), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 9000)
	require.LessOrEqual(t, proto.Size(resp), 2<<20)
	require.Equal(t, int64(3*12003), int64(len(resp.BuggifyKeys))+resp.BuggifyKeysOmitted)
}
