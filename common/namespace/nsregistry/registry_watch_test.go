package nsregistry_test

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsregistry"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

// callbackTracker tracks state change callbacks for testing.
type callbackTracker struct {
	count   atomic.Int32
	ch      chan struct{}
	mu      sync.Mutex
	events  []callbackEvent
	deleted []callbackEvent
}

type callbackEvent struct {
	ns      *namespace.Namespace
	deleted bool
}

func newCallbackTracker(bufferSize int) *callbackTracker {
	return &callbackTracker{
		ch: make(chan struct{}, bufferSize),
	}
}

func (t *callbackTracker) callback() namespace.StateChangeCallbackFn {
	return func(ns *namespace.Namespace, deletedFromDb bool) {
		t.count.Add(1)
		event := callbackEvent{ns: ns, deleted: deletedFromDb}
		t.mu.Lock()
		t.events = append(t.events, event)
		if deletedFromDb {
			t.deleted = append(t.deleted, event)
		}
		t.mu.Unlock()
		t.ch <- struct{}{}
	}
}

func (t *callbackTracker) getCount() int32 {
	return t.count.Load()
}

func (t *callbackTracker) getEvents() []callbackEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.events)
}

func (t *callbackTracker) getDeleted() []callbackEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.deleted)
}

type (
	registryWatchSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		regPersistence *nsregistry.MockPersistence
		registry       namespace.Registry
		logger         *testlogger.TestLogger
		captureHandler *metricstest.CaptureHandler
		capture        *metricstest.Capture
	}
)

func TestRegistryWatchSuite(t *testing.T) {
	s := new(registryWatchSuite)
	suite.Run(t, s)
}

func (s *registryWatchSuite) SetupSuite() {}

func (s *registryWatchSuite) TearDownSuite() {}

func (s *registryWatchSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.regPersistence = nsregistry.NewMockPersistence(s.controller)
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.captureHandler = metricstest.NewCaptureHandler()
	s.capture = s.captureHandler.StartCapture()
	s.registry = s.newRegistry()
}

func (s *registryWatchSuite) TearDownTest() {
	s.registry.Stop()
	s.captureHandler.StopCapture(s.capture)
}

func (s *registryWatchSuite) newRegistry() namespace.Registry {
	return s.newRegistryWithResolverFactory(namespace.NewDefaultReplicationResolverFactory())
}

func (s *registryWatchSuite) newRegistryWithResolverFactory(
	resolverFactory namespace.ReplicationResolverFactory,
) namespace.Registry {
	return nsregistry.NewRegistry(
		s.regPersistence,
		true,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetBoolPropertyFn(false),
		s.captureHandler,
		s.logger,
		resolverFactory,
	)
}

func (s *registryWatchSuite) newNamespaceResponse(
	id namespace.ID,
	name string,
	activeCluster string,
	notificationVersion int64,
) *persistence.GetNamespaceResponse {
	return &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    id.String(),
				Name:  name,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
				Data:  make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(int32(1)),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeCluster,
				Clusters:          []string{activeCluster},
			},
		},
		NotificationVersion: notificationVersion,
	}
}

// defaultListRequest returns the standard ListNamespacesRequest used by the registry.
func (s *registryWatchSuite) defaultListRequest() *persistence.ListNamespacesRequest {
	return &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}
}

// expectWatchAndList sets up common mock expectations for WatchNamespaces and ListNamespaces.
func (s *registryWatchSuite) expectWatchAndList(
	watchCh chan *persistence.NamespaceWatchEvent,
	namespaces ...*persistence.GetNamespaceResponse,
) {
	s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
		&persistence.ListNamespacesResponse{
			Namespaces:    namespaces,
			NextPageToken: nil,
		}, nil,
	)
}

func (s *registryWatchSuite) waitForCallback(ch <-chan struct{}, description string) {
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		s.FailNow("Timed out waiting for callback: " + description)
	}
}

// TestWatchEvents verifies the registry can start with a watch-enabled persistence
// implementation, receives the initial refresh callback, and processes namespace
// create, update, and delete events received over the watch channel.
func (s *registryWatchSuite) TestWatchEvents() {
	ns1ID := namespace.NewID()
	ns1Record := s.newNamespaceResponse(ns1ID, "initial-namespace", cluster.TestCurrentClusterName, 1)

	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "created-via-watch", cluster.TestCurrentClusterName, 2)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, ns1Record)

	tracker := newCallbackTracker(4)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// --- Initial refresh ---
	s.waitForCallback(tracker.ch, "initial refresh")

	ns, err := s.registry.GetNamespace("initial-namespace")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())
	s.Equal(cluster.TestCurrentClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	events := tracker.getEvents()
	s.Len(events, 1)
	s.Equal("initial-namespace", events[0].ns.Name().String())
	s.Equal(int64(1), events[0].ns.NotificationVersion())
	s.False(events[0].deleted)

	// --- Create event ---
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}
	s.waitForCallback(tracker.ch, "create event")

	ns, err = s.registry.GetNamespace("created-via-watch")
	s.NoError(err)
	s.Equal(ns2ID, ns.ID())

	ns, err = s.registry.GetNamespaceByID(ns2ID)
	s.NoError(err)
	s.Equal(namespace.Name("created-via-watch"), ns.Name())

	events = tracker.getEvents()
	s.Len(events, 2)
	s.Equal("created-via-watch", events[1].ns.Name().String())
	s.False(events[1].deleted)

	// --- Update event (change active cluster to trigger state change callback) ---
	ns1UpdatedRecord := s.newNamespaceResponse(ns1ID, "initial-namespace", cluster.TestAlternativeClusterName, 3)

	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: ns1UpdatedRecord,
	}
	s.waitForCallback(tracker.ch, "update event")

	ns, err = s.registry.GetNamespace("initial-namespace")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())
	s.Equal(cluster.TestAlternativeClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	ns, err = s.registry.GetNamespaceByID(ns1ID)
	s.NoError(err)
	s.Equal(cluster.TestAlternativeClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	events = tracker.getEvents()
	s.Len(events, 3)
	s.Equal("initial-namespace", events[2].ns.Name().String())
	s.Equal(int64(3), events[2].ns.NotificationVersion())
	s.Equal(cluster.TestAlternativeClusterName, events[2].ns.ActiveClusterName(namespace.EmptyBusinessID))
	s.False(events[2].deleted)

	// --- Delete event ---
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:        persistence.NamespaceWatchEventTypeDelete,
		NamespaceID: ns2ID,
	}
	s.waitForCallback(tracker.ch, "delete event")

	// Deleted namespace should no longer be in cache
	_, err = s.registry.GetNamespaceWithOptions(
		"created-via-watch",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	_, err = s.registry.GetNamespaceByIDWithOptions(
		ns2ID,
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	// The other namespace should still be accessible
	ns, err = s.registry.GetNamespace("initial-namespace")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())

	events = tracker.getEvents()
	s.Len(events, 4)
	s.Equal("created-via-watch", events[3].ns.Name().String())
	s.True(events[3].deleted)

	// Verify refresh latency metric was recorded (1 initial refresh).
	s.Len(s.capture.Snapshot()[metrics.NamespaceRegistryRefreshLatency.Name()], 1)
}

// TestWatchStaleUpdateIgnored verifies that update events with a NotificationVersion
// less than or equal to the cached version are ignored.
func (s *registryWatchSuite) TestWatchStaleUpdateIgnored() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 2)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 3)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(3)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	ns, err := s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(cluster.TestCurrentClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	// Send valid update (NotificationVersion 3), changing cluster to trigger callback
	validRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestAlternativeClusterName, 3)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: validRecord,
	}
	s.waitForCallback(tracker.ch, "valid update")

	ns, err = s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(cluster.TestAlternativeClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	// Send stale update (NotificationVersion 1 < current version 3)
	// Uses different retention as a marker to verify it wasn't applied
	staleRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)
	staleRecord.Namespace.Config.Retention = timestamp.DurationFromDays(int32(99))
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: staleRecord,
	}

	// Send another valid update to flush the channel and verify stale was skipped
	finalRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 4)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: finalRecord,
	}
	s.waitForCallback(tracker.ch, "final update")

	// Verify only 3 callbacks total (initial + valid + final, not stale)
	s.Equal(int32(3), tracker.getCount())

	// Verify namespace has the final update's retention (1 day), not the stale one (99 days)
	ns, err = s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(24*time.Hour, ns.Retention())
}

// TestWatchNamespaceRename verifies that when a namespace is renamed via an update event,
// the old name is removed from the cache and the new name works.
func (s *registryWatchSuite) TestWatchNamespaceRename() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "old-name", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Verify old name works
	ns, err := s.registry.GetNamespace("old-name")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	// Send rename event (same ID, different name)
	renamedRecord := s.newNamespaceResponse(nsID, "new-name", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: renamedRecord,
	}
	s.waitForCallback(tracker.ch, "rename event")

	// Verify old name no longer works
	_, err = s.registry.GetNamespaceWithOptions(
		"old-name",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	// Verify new name works
	ns, err = s.registry.GetNamespace("new-name")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Initial refresh
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(namespace.Name("old-name"), events[0].ns.Name())
	s.False(events[0].deleted)
	// Rename event
	s.Equal(nsID, events[1].ns.ID())
	s.Equal(namespace.Name("new-name"), events[1].ns.Name())
	s.False(events[1].deleted)
}

// TestWatchInitialRefreshPaginated verifies initial refresh correctly handles
// multiple pages of namespaces.
func (s *registryWatchSuite) TestWatchInitialRefreshPaginated() {
	ns1ID := namespace.NewID()
	ns1Record := s.newNamespaceResponse(ns1ID, "namespace-1", cluster.TestCurrentClusterName, 1)

	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "namespace-2", cluster.TestCurrentClusterName, 2)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)

	s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh, nil)

	// First page returns one namespace and a token
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
		&persistence.ListNamespacesResponse{
			Namespaces:    []*persistence.GetNamespaceResponse{ns1Record},
			NextPageToken: []byte("page2"),
		}, nil,
	)

	// Second page returns second namespace and no token
	s.regPersistence.EXPECT().ListNamespaces(
		gomock.Any(), &persistence.ListNamespacesRequest{
			PageSize:       nsregistry.CacheRefreshPageSize,
			IncludeDeleted: true,
			NextPageToken:  []byte("page2"),
		},
	).Return(
		&persistence.ListNamespacesResponse{
			Namespaces:    []*persistence.GetNamespaceResponse{ns2Record},
			NextPageToken: nil,
		}, nil,
	)

	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// Wait for both callbacks (one per namespace)
	s.waitForCallback(tracker.ch, "first namespace")
	s.waitForCallback(tracker.ch, "second namespace")

	// Verify both namespaces are accessible
	ns, err := s.registry.GetNamespace("namespace-1")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())

	ns, err = s.registry.GetNamespace("namespace-2")
	s.NoError(err)
	s.Equal(ns2ID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Verify both namespaces received via callbacks (don't rely on exact order)
	names := []string{events[0].ns.Name().String(), events[1].ns.Name().String()}
	s.Contains(names, "namespace-1")
	s.Contains(names, "namespace-2")
	s.False(events[0].deleted)
	s.False(events[1].deleted)
}

// TestWatchInitialRefreshPaginationError verifies the registry handles errors
// when fetching subsequent pages during initial refresh.
func (s *registryWatchSuite) TestWatchInitialRefreshPaginationError() {
	ns1ID := namespace.NewID()
	ns1Record := s.newNamespaceResponse(ns1ID, "namespace-1", cluster.TestCurrentClusterName, 1)

	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	// Expect two watch calls: first fails during pagination, second succeeds
	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	// First attempt: first page succeeds, second page fails
	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{ns1Record},
				NextPageToken: []byte("page2"),
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(
			gomock.Any(), &persistence.ListNamespacesRequest{
				PageSize:       nsregistry.CacheRefreshPageSize,
				IncludeDeleted: true,
				NextPageToken:  []byte("page2"),
			},
		).Return(nil, errors.New("simulated pagination error")),
		// Second attempt succeeds fully
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{ns1Record},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Error refreshing namespaces after watch start")
	registry := s.newRegistry()

	tracker := newCallbackTracker(1)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "namespace after retry")

	// Verify namespace is accessible after retry
	ns, err := registry.GetNamespace("namespace-1")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())

	s.Equal(int32(1), tracker.getCount())

	// Verify the callback received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 1)
	s.Equal(ns1ID, events[0].ns.ID())
	s.Equal(namespace.Name("namespace-1"), events[0].ns.Name())
	s.False(events[0].deleted)

	// Verify refresh failure metric was recorded (1 failed attempt before retry succeeded).
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryRefreshFailures.Name()], 1)
	// Verify refresh latency metric was recorded (failed attempt + successful retry).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}

// TestWatchMultipleCallbacks verifies all registered callbacks receive events.
func (s *registryWatchSuite) TestWatchMultipleCallbacks() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	// Register 3 separate trackers
	tracker1 := newCallbackTracker(2)
	tracker2 := newCallbackTracker(2)
	tracker3 := newCallbackTracker(2)

	s.registry.RegisterStateChangeCallback("callback1", tracker1.callback())
	s.registry.RegisterStateChangeCallback("callback2", tracker2.callback())
	s.registry.RegisterStateChangeCallback("callback3", tracker3.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// Wait for all 3 callbacks for initial refresh
	s.waitForCallback(tracker1.ch, "callback 1 initial")
	s.waitForCallback(tracker2.ch, "callback 2 initial")
	s.waitForCallback(tracker3.ch, "callback 3 initial")

	// Verify all callbacks received the same namespace for initial refresh.
	for _, tracker := range []*callbackTracker{tracker1, tracker2, tracker3} {
		events := tracker.getEvents()
		s.Len(events, 1)
		s.Equal(nsID, events[0].ns.ID())
		s.Equal(namespace.Name("test-namespace"), events[0].ns.Name())
		s.False(events[0].deleted)
	}

	// Send create event
	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "new-namespace", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}

	// Wait for all 3 callbacks for create event
	s.waitForCallback(tracker1.ch, "callback 1 create")
	s.waitForCallback(tracker2.ch, "callback 2 create")
	s.waitForCallback(tracker3.ch, "callback 3 create")

	// Each callback should have been called twice (initial + create)
	s.Equal(int32(2), tracker1.getCount())
	s.Equal(int32(2), tracker2.getCount())
	s.Equal(int32(2), tracker3.getCount())

	// Verify all callbacks received the same namespace for create event.
	for _, tracker := range []*callbackTracker{tracker1, tracker2, tracker3} {
		events := tracker.getEvents()
		s.Len(events, 2)
		s.Equal(ns2ID, events[1].ns.ID())
		s.Equal(namespace.Name("new-namespace"), events[1].ns.Name())
		s.False(events[1].deleted)
	}
}

// TestWatchUpdateWithoutStateChange verifies updates that don't change "state"
// (per namespaceStateChanged) don't trigger callbacks.
func (s *registryWatchSuite) TestWatchUpdateWithoutStateChange() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 2)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")
	s.Equal(int32(1), tracker.getCount())

	// Verify initial retention
	ns, err := s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(24*time.Hour, ns.Retention())

	// Send update that only changes retention (not a "state" field)
	updatedRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 2)
	updatedRecord.Namespace.Config.Retention = timestamp.DurationFromDays(int32(7))
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: updatedRecord,
	}

	// Send another update that does change state (active cluster) to flush the channel
	stateChangeRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestAlternativeClusterName, 3)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: stateChangeRecord,
	}

	s.waitForCallback(tracker.ch, "state change update")

	// Should have 2 callbacks total: initial + state change (not the retention-only update)
	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)

	// First event: initial namespace with original cluster
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(cluster.TestCurrentClusterName, events[0].ns.ActiveClusterName(namespace.EmptyBusinessID))
	s.False(events[0].deleted)

	// Second event: state change with new cluster (the retention-only update was skipped)
	s.Equal(nsID, events[1].ns.ID())
	s.Equal(cluster.TestAlternativeClusterName, events[1].ns.ActiveClusterName(namespace.EmptyBusinessID))
	s.False(events[1].deleted)
}

// TestWatchDeleteNonExistentNamespace verifies deleting a namespace not in cache
// is handled gracefully without triggering callbacks.
func (s *registryWatchSuite) TestWatchDeleteNonExistentNamespace() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "existing-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 2)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")
	s.Equal(int32(1), tracker.getCount())

	// Send delete event for a non-existent namespace
	nonExistentID := namespace.NewID()
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:        persistence.NamespaceWatchEventTypeDelete,
		NamespaceID: nonExistentID,
	}

	// Send a create event to flush the channel and verify processing continued
	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "new-namespace", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}

	s.waitForCallback(tracker.ch, "create event")

	// Should have 2 callbacks: initial + create (NOT the non-existent delete)
	s.Equal(int32(2), tracker.getCount())

	// Verify existing namespace is still accessible
	ns, err := s.registry.GetNamespace("existing-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	// Verify new namespace was created
	ns, err = s.registry.GetNamespace("new-namespace")
	s.NoError(err)
	s.Equal(ns2ID, ns.ID())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Initial refresh
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(namespace.Name("existing-namespace"), events[0].ns.Name())
	s.False(events[0].deleted)
	// Create event (delete of non-existent did not trigger callback)
	s.Equal(ns2ID, events[1].ns.ID())
	s.Equal(namespace.Name("new-namespace"), events[1].ns.Name())
	s.False(events[1].deleted)
}

// TestWatchFallbackToPolling verifies the registry falls back to polling
// when WatchNamespaces returns ErrWatchNotSupported.
func (s *registryWatchSuite) TestWatchFallbackToPolling() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(nil, persistence.ErrWatchNotSupported)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
		&persistence.ListNamespacesResponse{
			Namespaces:    []*persistence.GetNamespaceResponse{nsRecord},
			NextPageToken: nil,
		}, nil,
	)

	s.logger.Expect(testlogger.Error, "Error starting namespace watch")

	registry := s.newRegistry()

	tracker := newCallbackTracker(1)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	ns, err := registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	s.Equal(int32(1), tracker.getCount())

	// Verify the callback received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 1)
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(namespace.Name("test-namespace"), events[0].ns.Name())
	s.False(events[0].deleted)
}

// TestWatchUnregisterCallback verifies unregistering a callback stops it
// from receiving future events.
func (s *registryWatchSuite) TestWatchUnregisterCallback() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(1)
	s.registry.RegisterStateChangeCallback("test-callback", tracker.callback())

	// Register a second callback that stays active to use as a synchronization point.
	syncTracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("sync-callback", syncTracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")
	s.waitForCallback(syncTracker.ch, "sync initial refresh")
	s.Equal(int32(1), tracker.getCount())
	s.Equal(int32(1), syncTracker.getCount())

	// Unregister the first callback
	s.registry.UnregisterStateChangeCallback("test-callback")

	// Send create event
	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "new-namespace", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}

	// Wait for the sync callback to receive the event, confirming event processing is complete.
	s.waitForCallback(syncTracker.ch, "sync create event")

	// Verify unregistered callback count is still 1 (only initial, not the create)
	s.Equal(int32(1), tracker.getCount())

	// Verify the sync callback received both events
	s.Equal(int32(2), syncTracker.getCount())

	// Verify the namespace was still created
	ns, err := s.registry.GetNamespace("new-namespace")
	s.NoError(err)
	s.Equal(ns2ID, ns.ID())
}

// TestWatchChannelCloses verifies that when the watch channel closes unexpectedly,
// the registry reconnects and performs a full refresh.
func (s *registryWatchSuite) TestWatchChannelCloses() {
	nsID := namespace.NewID()
	nsRecordV1 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)
	nsRecordV2 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 2)
	// Change IsGlobalNamespace to trigger state change callback after reconnect
	nsRecordV2.IsGlobalNamespace = true

	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	// Initial refresh returns V1, refresh after reconnect returns V2 (to trigger callback)
	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV1},
				NextPageToken: nil,
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV2},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Namespace watch failed, restarting")
	registry := s.newRegistry()

	tracker := newCallbackTracker(2)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Close the watch channel to trigger reconnect
	close(watchCh1)

	s.waitForCallback(tracker.ch, "refresh after reconnect")

	ns, err := registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)

	// First event: initial namespace (non-global)
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(namespace.Name("test-namespace"), events[0].ns.Name())
	s.False(events[0].ns.IsGlobalNamespace())
	s.False(events[0].deleted)

	// Second event: after reconnect (now global)
	s.Equal(nsID, events[1].ns.ID())
	s.Equal(namespace.Name("test-namespace"), events[1].ns.Name())
	s.True(events[1].ns.IsGlobalNamespace())
	s.False(events[1].deleted)

	// Verify reconnection metric was recorded.
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryWatchReconnections.Name()], 1)
	// Verify refresh latency metric was recorded (initial + after reconnect).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}

// TestWatchEventWithError verifies that when a watch event contains an error, the registry reconnects and continues
// operating.
func (s *registryWatchSuite) TestWatchEventWithError() {
	nsID := namespace.NewID()
	nsRecordV1 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)
	nsRecordV2 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 2)
	// Change IsGlobalNamespace to trigger state change callback after reconnect
	nsRecordV2.IsGlobalNamespace = true

	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV1},
				NextPageToken: nil,
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV2},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Namespace watch failed, restarting")
	registry := s.newRegistry()

	tracker := newCallbackTracker(2)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Send event with error to trigger reconnect
	watchCh1 <- &persistence.NamespaceWatchEvent{
		Err: errors.New("simulated watch error"),
	}

	s.waitForCallback(tracker.ch, "refresh after reconnect")

	ns, err := registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Initial refresh (non-global)
	s.Equal(nsID, events[0].ns.ID())
	s.False(events[0].ns.IsGlobalNamespace())
	s.False(events[0].deleted)
	// After reconnect (now global)
	s.Equal(nsID, events[1].ns.ID())
	s.True(events[1].ns.IsGlobalNamespace())
	s.False(events[1].deleted)

	// Verify reconnection metric was recorded.
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryWatchReconnections.Name()], 1)
	// Verify refresh latency metric was recorded (initial + after reconnect).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}

// TestWatchEmptyInitialRefresh verifies the registry handles an empty initial
// refresh (no namespaces in the database).
func (s *registryWatchSuite) TestWatchEmptyInitialRefresh() {
	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh)

	tracker := newCallbackTracker(1)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// Verify no callbacks fired (no namespaces)
	s.Equal(int32(0), tracker.getCount())

	// Verify GetNamespace returns not found
	_, err := s.registry.GetNamespaceWithOptions(
		"nonexistent",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	// Send create event for new namespace
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "new-namespace", cluster.TestCurrentClusterName, 1)

	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: nsRecord,
	}

	s.waitForCallback(tracker.ch, "create event")

	// Verify namespace now accessible
	ns, err := s.registry.GetNamespace("new-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())
}

// TestWatchUpdateForUnknownNamespace verifies that an update event for a namespace
// not in the cache adds it (acts like create).
func (s *registryWatchSuite) TestWatchUpdateForUnknownNamespace() {
	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh)

	tracker := newCallbackTracker(1)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// Send Update event for namespace not in cache
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "unknown-namespace", cluster.TestCurrentClusterName, 5)

	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeUpdate,
		Response: nsRecord,
	}

	s.waitForCallback(tracker.ch, "update event for unknown namespace")

	// Verify namespace was added to cache
	ns, err := s.registry.GetNamespace("unknown-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())
	s.Equal(int64(5), ns.NotificationVersion())

	s.Equal(int32(1), tracker.getCount())

	// Verify the callback received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 1)
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(namespace.Name("unknown-namespace"), events[0].ns.Name())
	s.Equal(int64(5), events[0].ns.NotificationVersion())
	s.False(events[0].deleted)
}

// TestWatchCreateForExistingNamespace verifies that a create event for an existing
// namespace updates it if the version is higher.
func (s *registryWatchSuite) TestWatchCreateForExistingNamespace() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Send Create event with higher version (changing cluster to trigger callback)
	updatedRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestAlternativeClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: updatedRecord,
	}

	s.waitForCallback(tracker.ch, "create event for existing namespace")

	ns, err := s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(int64(2), ns.NotificationVersion())
	s.Equal(cluster.TestAlternativeClusterName, ns.ActiveClusterName(namespace.EmptyBusinessID))

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Initial refresh
	s.Equal(nsID, events[0].ns.ID())
	s.Equal(cluster.TestCurrentClusterName, events[0].ns.ActiveClusterName(namespace.EmptyBusinessID))
	s.False(events[0].deleted)
	// Create event (acts as update)
	s.Equal(nsID, events[1].ns.ID())
	s.Equal(cluster.TestAlternativeClusterName, events[1].ns.ActiveClusterName(namespace.EmptyBusinessID))
	s.False(events[1].deleted)
}

// TestWatchReconnectSucceedsAfterRetry verifies that when watch reconnect fails
// initially, it retries and eventually succeeds.
func (s *registryWatchSuite) TestWatchReconnectSucceedsAfterRetry() {
	nsID := namespace.NewID()
	nsRecordV1 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)
	nsRecordV2 := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 2)
	// Change IsGlobalNamespace to trigger state change callback after reconnect
	nsRecordV2.IsGlobalNamespace = true

	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	// First call succeeds, second fails, third succeeds
	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(nil, errors.New("temporary failure")),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV1},
				NextPageToken: nil,
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{nsRecordV2},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Namespace watch failed, restarting")
	s.logger.Expect(testlogger.Error, "Error starting namespace watch")
	registry := s.newRegistry()

	tracker := newCallbackTracker(2)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Close channel to trigger reconnect (which will fail once, then succeed)
	close(watchCh1)

	s.waitForCallback(tracker.ch, "refresh after retry")

	ns, err := registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 2)
	// Initial refresh (non-global)
	s.Equal(nsID, events[0].ns.ID())
	s.False(events[0].ns.IsGlobalNamespace())
	s.False(events[0].deleted)
	// After retry reconnect (now global)
	s.Equal(nsID, events[1].ns.ID())
	s.True(events[1].ns.IsGlobalNamespace())
	s.False(events[1].deleted)

	// Verify reconnection metric was recorded.
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryWatchReconnections.Name()], 1)
	// Verify refresh latency metric was recorded (initial + after reconnect).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}

// TestWatchNamespaceDeletedDuringRefresh verifies that when a namespace is removed
// from the database between refreshes, it gets removed from the cache and a delete
// callback is fired.
func (s *registryWatchSuite) TestWatchNamespaceDeletedDuringRefresh() {
	ns1ID := namespace.NewID()
	ns1Record := s.newNamespaceResponse(ns1ID, "namespace-1", cluster.TestCurrentClusterName, 1)

	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "namespace-2", cluster.TestCurrentClusterName, 2)

	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	// Initial refresh returns both namespaces
	// Refresh after reconnect returns only namespace-1 (namespace-2 was deleted)
	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{ns1Record, ns2Record},
				NextPageToken: nil,
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{ns1Record},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Namespace watch failed, restarting")
	registry := s.newRegistry()

	tracker := newCallbackTracker(4)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	// Wait for initial callbacks (2 namespaces)
	s.waitForCallback(tracker.ch, "initial namespace-1")
	s.waitForCallback(tracker.ch, "initial namespace-2")

	// Verify both namespaces accessible
	_, err := registry.GetNamespace("namespace-1")
	s.NoError(err)
	_, err = registry.GetNamespace("namespace-2")
	s.NoError(err)

	// Close channel to trigger reconnect and refresh
	close(watchCh1)

	// Wait for delete callback
	s.waitForCallback(tracker.ch, "delete namespace-2")

	// Verify namespace-1 still accessible
	ns, err := registry.GetNamespace("namespace-1")
	s.NoError(err)
	s.Equal(ns1ID, ns.ID())

	// Verify namespace-2 is gone
	_, err = registry.GetNamespaceWithOptions(
		"namespace-2",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	// Verify we got a delete callback for namespace-2
	deleted := tracker.getDeleted()
	var deleteCallbackFound bool
	for _, event := range deleted {
		if event.ns.Name().String() == "namespace-2" {
			deleteCallbackFound = true
			break
		}
	}
	s.True(deleteCallbackFound, "expected delete callback for namespace-2")

	// Verify the callbacks received the expected namespace data.
	events := tracker.getEvents()
	s.Len(events, 3)
	// Initial refresh - both namespaces (order may vary)
	initialNames := []string{events[0].ns.Name().String(), events[1].ns.Name().String()}
	s.Contains(initialNames, "namespace-1")
	s.Contains(initialNames, "namespace-2")
	s.False(events[0].deleted)
	s.False(events[1].deleted)
	// Delete callback for namespace-2
	s.Equal(namespace.Name("namespace-2"), events[2].ns.Name())
	s.True(events[2].deleted)

	// Verify reconnection metric was recorded.
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryWatchReconnections.Name()], 1)
	// Verify refresh latency metric was recorded (initial + after reconnect).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}

// TestWatchStop verifies the registry stops gracefully without panics.
func (s *registryWatchSuite) TestWatchStop() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	tracker := newCallbackTracker(1)
	s.registry.RegisterStateChangeCallback("test", tracker.callback())

	s.registry.Start()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Verify namespace accessible before stop
	ns, err := s.registry.GetNamespace("test-namespace")
	s.NoError(err)
	s.Equal(nsID, ns.ID())

	// Stop should not panic
	s.registry.Stop()

	// Calling Stop again should be safe (idempotent)
	s.registry.Stop()
}

// TestWatchUnknownEventType verifies that unknown watch event types are handled
// gracefully (logged as warning) without causing errors or callbacks.
func (s *registryWatchSuite) TestWatchUnknownEventType() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "test-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 2)
	s.expectWatchAndList(watchCh, nsRecord)

	s.logger.Expect(testlogger.Warn, "Unknown namespace watch event type")
	registry := s.newRegistry()

	tracker := newCallbackTracker(2)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")
	s.Equal(int32(1), tracker.getCount())

	// Send unknown event type (type 99 is not defined)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type: persistence.NamespaceWatchEventType(99),
	}

	// Send a valid event to flush the channel
	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "new-namespace", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}

	s.waitForCallback(tracker.ch, "create event")

	// Should have 2 callbacks: initial + create (unknown type should not trigger callback)
	s.Equal(int32(2), tracker.getCount())

	// Verify new namespace was created
	ns, err := registry.GetNamespace("new-namespace")
	s.NoError(err)
	s.Equal(ns2ID, ns.ID())
}

// TestWatchRegisterCallbackAfterStart verifies that a callback registered after
// the registry has started receives catch-up calls for existing namespaces
// and then receives events for new namespace changes.
func (s *registryWatchSuite) TestWatchRegisterCallbackAfterStart() {
	nsID := namespace.NewID()
	nsRecord := s.newNamespaceResponse(nsID, "existing-namespace", cluster.TestCurrentClusterName, 1)

	watchCh := make(chan *persistence.NamespaceWatchEvent, 1)
	s.expectWatchAndList(watchCh, nsRecord)

	// Register a setup callback to detect when initial refresh completes.
	setupTracker := newCallbackTracker(1)
	s.registry.RegisterStateChangeCallback("setup-callback", setupTracker.callback())

	s.registry.Start()
	defer s.registry.Stop()

	// Wait for initial refresh to complete
	s.waitForCallback(setupTracker.ch, "setup initial refresh")

	// Unregister setup callback before registering late callback
	s.registry.UnregisterStateChangeCallback("setup-callback")

	// Now register a callback after startup
	tracker := newCallbackTracker(2)
	s.registry.RegisterStateChangeCallback("late-callback", tracker.callback())

	// Should receive catch-up callback for existing namespace
	s.waitForCallback(tracker.ch, "catch-up callback")
	s.Equal(int32(1), tracker.getCount())

	events := tracker.getEvents()
	s.Len(events, 1)
	s.Equal("existing-namespace", events[0].ns.Name().String())
	s.False(events[0].deleted)

	// Send new namespace event
	ns2ID := namespace.NewID()
	ns2Record := s.newNamespaceResponse(ns2ID, "new-namespace", cluster.TestCurrentClusterName, 2)
	watchCh <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: ns2Record,
	}

	s.waitForCallback(tracker.ch, "new namespace callback")
	s.Equal(int32(2), tracker.getCount())

	events = tracker.getEvents()
	s.Len(events, 2)
	s.Equal("new-namespace", events[1].ns.Name().String())
}

// TestWatchProcessEventError verifies that when processWatchEvent fails to build
// a Namespace object from a watch event (e.g., when namespace.FromPersistentState
// returns an error due to nil resolver), the error is logged and the watch reconnects.
func (s *registryWatchSuite) TestWatchProcessEventError() {
	// Create a "bad" namespace ID that will trigger nil resolver
	badNsID := namespace.NewID()

	// Initial namespace (good)
	goodNsID := namespace.NewID()
	goodNsRecordV1 := s.newNamespaceResponse(goodNsID, "good-namespace", cluster.TestCurrentClusterName, 1)
	goodNsRecordV2 := s.newNamespaceResponse(goodNsID, "good-namespace", cluster.TestCurrentClusterName, 2)
	// Change IsGlobalNamespace to trigger state change callback after reconnect
	goodNsRecordV2.IsGlobalNamespace = true

	// Watch channels for initial connection and reconnect
	watchCh1 := make(chan *persistence.NamespaceWatchEvent, 1)
	watchCh2 := make(chan *persistence.NamespaceWatchEvent, 1)

	// Custom resolver factory that returns nil for badNsID, causing FromPersistentState to fail.
	customFactory := func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
		if detail != nil && detail.Info != nil && detail.Info.Id == badNsID.String() {
			return nil
		}
		return namespace.NewDefaultReplicationResolverFactory()(detail)
	}

	// Setup mocks: expect two watch attempts (initial + reconnect after error)
	gomock.InOrder(
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh1, nil),
		s.regPersistence.EXPECT().WatchNamespaces(gomock.Any()).Return(watchCh2, nil),
	)

	// Setup mocks: expect two list calls (initial + refresh after reconnect)
	gomock.InOrder(
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{goodNsRecordV1},
				NextPageToken: nil,
			}, nil,
		),
		s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), s.defaultListRequest()).Return(
			&persistence.ListNamespacesResponse{
				Namespaces:    []*persistence.GetNamespaceResponse{goodNsRecordV2},
				NextPageToken: nil,
			}, nil,
		),
	)

	s.logger.Expect(testlogger.Error, "Namespace watch failed, restarting")
	registry := s.newRegistryWithResolverFactory(customFactory)

	tracker := newCallbackTracker(2)
	registry.RegisterStateChangeCallback("test", tracker.callback())

	registry.Start()
	defer registry.Stop()

	s.waitForCallback(tracker.ch, "initial refresh")

	// Verify initial namespace accessible
	ns, err := registry.GetNamespace("good-namespace")
	s.NoError(err)
	s.Equal(goodNsID, ns.ID())

	// Create bad namespace record that will trigger error in processWatchEvent.
	// The custom resolver factory returns nil for this namespace ID.
	badNsRecord := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    badNsID.String(),
				Name:  "bad-namespace",
				State: enumspb.NAMESPACE_STATE_REGISTERED,
				Data:  make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
		NotificationVersion: 10,
	}

	// Send watch event with bad namespace - this should cause processWatchEvent to fail
	watchCh1 <- &persistence.NamespaceWatchEvent{
		Type:     persistence.NamespaceWatchEventTypeCreate,
		Response: badNsRecord,
	}

	// Wait for reconnect and refresh callback
	s.waitForCallback(tracker.ch, "refresh after reconnect")

	// Verify registry still works after reconnect
	ns, err = registry.GetNamespace("good-namespace")
	s.NoError(err)
	s.Equal(goodNsID, ns.ID())

	s.Equal(int32(2), tracker.getCount())

	// Verify reconnection metric was recorded.
	snapshot := s.capture.Snapshot()
	s.Len(snapshot[metrics.NamespaceRegistryWatchReconnections.Name()], 1)
	// Verify refresh latency metric was recorded (initial + after reconnect).
	s.Len(snapshot[metrics.NamespaceRegistryRefreshLatency.Name()], 2)
}
