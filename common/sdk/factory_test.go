package sdk

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// newFactoryWithSystemClient returns a factory whose system client is already
// set, so no dial happens: sdkclient.Dial and NewClientFromExisting both require
// the SDK's concrete client, which a mock cannot stand in for.
func newFactoryWithSystemClient(system *mocks.Client) *clientFactory {
	f := NewClientFactory("membership://frontend", nil, metrics.NoopMetricsHandler,
		log.NewNoopLogger(), dynamicconfig.GetIntPropertyFn(0))
	f.once.Do(func() {})
	f.systemSdkClient = system
	return f
}

func (f *clientFactory) trackForTest(client *mocks.Client) *trackedClient {
	tracked := &trackedClient{Client: client, factory: f}
	f.derivedClients[tracked] = struct{}{}
	return tracked
}

// Clients from NewClient share the system client's connection, and the SDK only
// closes it once every one of them is closed. Releasing the clients callers left
// open, before the system client, is what lets that reference count reach zero.
func TestClientFactoryClose_ReleasesDerivedClientsBeforeSystem(t *testing.T) {
	system := &mocks.Client{}
	derived := &mocks.Client{}
	f := newFactoryWithSystemClient(system)
	f.trackForTest(derived)

	derivedClosed := false
	derived.On("Close").Once().Run(func(mock.Arguments) { derivedClosed = true })
	system.On("Close").Once().Run(func(mock.Arguments) {
		require.True(t, derivedClosed, "derived clients must be released first")
	})

	f.Close()

	derived.AssertExpectations(t)
	system.AssertExpectations(t)
	require.Empty(t, f.derivedClients)
}

func TestClientFactoryClose_Idempotent(t *testing.T) {
	system := &mocks.Client{}
	f := newFactoryWithSystemClient(system)
	system.On("Close").Once()

	f.Close()
	f.Close()

	system.AssertExpectations(t)
}

// A client its owner already closed must not be closed again by the factory: the
// SDK's guard against repeated Close is only safe sequentially.
func TestClientFactoryClose_SkipsClientClosedByOwner(t *testing.T) {
	system := &mocks.Client{}
	derived := &mocks.Client{}
	f := newFactoryWithSystemClient(system)
	tracked := f.trackForTest(derived)

	derived.On("Close").Once()
	system.On("Close").Once()

	tracked.Close()
	require.Empty(t, f.derivedClients)
	tracked.Close() // second close by the owner must be a no-op
	f.Close()

	derived.AssertExpectations(t)
	system.AssertExpectations(t)
}
