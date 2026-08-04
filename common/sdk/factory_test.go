package sdk

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// unreachableAddress refuses connections, and binding it needs privileges, so no
// test here can accidentally make it reachable by claiming a released port.
const unreachableAddress = "127.0.0.1:1"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newFactory(hostPort string, logger log.Logger) *clientFactory {
	return NewClientFactory(hostPort, nil, metrics.NoopMetricsHandler,
		logger, dynamicconfig.GetIntPropertyFn(0))
}

// newSeededFactory injects the system client instead of dialing one: both
// sdkclient.Dial and NewClientFromExisting need the SDK's concrete client, which
// a mock cannot stand in for.
func newSeededFactory(system sdkclient.Client) *clientFactory {
	f := newFactory("membership://frontend", log.NewNoopLogger())
	f.once.Do(func() {})
	f.systemSdkClient = system
	return f
}

func track(f *clientFactory, client sdkclient.Client) *trackedClient {
	tracked := &trackedClient{Client: client, factory: f}
	f.derivedClients[tracked] = struct{}{}
	return tracked
}

// newDialedFactory points a factory at a real listener so NewClient can build
// clients through the SDK. An empty gRPC server is enough, because Dial tolerates
// an Unimplemented GetSystemInfo. The returned stop releases both, so callers in
// a loop do not hold one listener per iteration.
func newDialedFactory(t *testing.T) (*clientFactory, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()

	// A noop logger's Fatal does nothing, so a failed dial would otherwise be
	// silent. Passed to the constructor so the SDK's own logger is covered too.
	f := newFactory(listener.Addr().String(),
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	return f, func() {
		f.Close()
		server.Stop()
	}
}

// The SDK closes the connection shared by these clients only once every one of
// them is closed, so the ones callers left open have to be released first.
func TestClientFactoryClose_ReleasesDerivedClientsBeforeSystem(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	system := mocksdk.NewMockClient(ctrl)
	derived := mocksdk.NewMockClient(ctrl)
	f := newSeededFactory(system)
	track(f, derived)

	gomock.InOrder(derived.EXPECT().Close(), system.EXPECT().Close())

	f.Close()

	require.Empty(t, f.derivedClients)
}

func TestClientFactoryClose_Idempotent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	system := mocksdk.NewMockClient(ctrl)
	f := newSeededFactory(system)
	system.EXPECT().Close()

	f.Close()
	f.Close()

	// Close leaves the field set, so a caller after shutdown gets a client whose
	// calls fail rather than a nil dereference.
	require.NotNil(t, f.GetSystemClient())
}

// The SDK's guard against repeated Close is a plain write, so the factory and the
// caller must never both close the same client.
func TestClientFactoryClose_SkipsClientClosedByOwner(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	system := mocksdk.NewMockClient(ctrl)
	derived := mocksdk.NewMockClient(ctrl)
	f := newSeededFactory(system)
	tracked := track(f, derived)

	derived.EXPECT().Close()
	system.EXPECT().Close()

	tracked.Close()
	require.Empty(t, f.derivedClients)
	tracked.Close()
	f.Close()
}

func TestNewClient_TracksDerivedClient(t *testing.T) {
	t.Parallel()

	f, stop := newDialedFactory(t)
	t.Cleanup(stop)
	client := f.NewClient(sdkclient.Options{Namespace: "ns"})

	require.IsType(t, &trackedClient{}, client)
	require.Len(t, f.derivedClients, 1)

	client.Close()
	require.Empty(t, f.derivedClients, "closing must release the factory's reference")
}

// Deriving from the closed system client would fetch capabilities over its
// connection and fail, so the closed factory must not reach for it.
func TestNewClient_AfterCloseIsNotTracked(t *testing.T) {
	t.Parallel()

	f, stop := newDialedFactory(t)
	t.Cleanup(stop)
	f.Close()

	client := f.NewClient(sdkclient.Options{Namespace: "ns"})

	f.clientsLock.Lock()
	dialed := f.systemSdkClient != nil
	f.clientsLock.Unlock()
	require.False(t, dialed, "a shut down factory must not dial a system client")

	require.IsType(t, &trackedClient{}, client)
	require.NotSame(t, f.GetSystemClient(), client, "must not hand out the shared client")
	require.Empty(t, f.derivedClients)

	// The factory already closed this client, so a caller's Close must release
	// nothing: the SDK's guard against repeated Close is a plain write.
	var wg sync.WaitGroup
	wg.Go(client.Close)
	wg.Go(client.Close)
	wg.Wait()
}

// sdkworker.New panics unless it gets the SDK's concrete client, so NewWorker has
// to unwrap what NewClient returned.
func TestNewWorker_AcceptsDerivedClient(t *testing.T) {
	t.Parallel()

	f, stop := newDialedFactory(t)
	t.Cleanup(stop)
	client := f.NewClient(sdkclient.Options{Namespace: "ns"})
	require.IsType(t, &trackedClient{}, client)

	require.NotPanics(t, func() {
		require.NotNil(t, f.NewWorker(client, "task-queue", sdkworker.Options{}))
	})
}

// NewClient holds clientsLock across NewClientFromExisting because the SDK reads
// the shared reference count there while Close writes it.
func TestNewClient_DoesNotRaceClose(t *testing.T) {
	t.Parallel()

	// Repeated because the two accesses only overlap in a narrow window.
	for range 100 {
		f, stop := newDialedFactory(t)
		require.NotNil(t, f.GetSystemClient())

		var wg sync.WaitGroup
		wg.Go(func() { f.NewClient(sdkclient.Options{Namespace: "ns"}) })
		wg.Go(f.Close)
		wg.Wait()
		stop()
	}
}

// A closed factory must not dial: the retry policy would chase a frontend that is
// gone for a minute and then Fatal, aborting the process midway through shutdown.
func TestGetSystemClient_AfterCloseDoesNotDial(t *testing.T) {
	t.Parallel()

	f := newFactory(unreachableAddress, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	f.Close()

	got := make(chan sdkclient.Client, 1)
	go func() { got <- f.GetSystemClient() }()

	select {
	case client := <-got:
		require.NotNil(t, client)
	case <-time.After(10 * time.Second):
		t.Fatal("GetSystemClient on a closed factory must not dial")
	}
}

// A Close landing after the first attempt must still stop the factory reaching for
// a frontend that is going away.
func TestGetSystemClient_CloseDuringDialStopsRetrying(t *testing.T) {
	t.Parallel()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	dialFailed := logger.Expect(testlogger.Warn, "error creating sdk client")
	f := newFactory(unreachableAddress, logger)

	got := make(chan sdkclient.Client, 1)
	go func() { got <- f.GetSystemClient() }()

	// Only close once an attempt has already failed, so the check under test is
	// the one inside the retry rather than the one before it.
	await.RequireTrue(t, dialFailed.Matched, 10*time.Second, 10*time.Millisecond)
	f.Close()

	select {
	case client := <-got:
		require.NotNil(t, client)
	case <-time.After(10 * time.Second):
		t.Fatal("Close during a dial must stop the factory retrying")
	}
}

// setSystemClient publishes the shared client under clientsLock because Close
// reads the same field under that lock.
func TestGetSystemClient_DoesNotRacePublishWithClose(t *testing.T) {
	t.Parallel()

	// Repeated because Close only overlaps the publish when it lands while the
	// first dial is still in flight.
	for range 100 {
		f, stop := newDialedFactory(t)

		var client sdkclient.Client
		var wg sync.WaitGroup
		wg.Go(func() { client = f.GetSystemClient() })
		wg.Go(f.Close)
		wg.Wait()

		require.NotNil(t, client)
		stop()
	}
}
