package matching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/streambatcher"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// These tests run user data updates through the real KeyedBatcher, applyUserDataUpdateBatch and a
// SQLite task store with version CAS. Each update carries a marker in its clock's wall clock so
// the test can tell which update reached persistence.
//
// requireAcknowledgedPersisted checks the safety property: an update is acknowledged only if its
// payload was persisted. Any correct fix must pass it. The other assertions also check the policy
// that applyUserDataUpdateBatch chose: all updates in a batch are written in one persistence
// request, and when several updates are for the same task queue only the last one is written. The
// others get errUserDataUpdateSuperseded, a retryable error, without a version check. The tests
// do not run the matching client's retries. Where they check a retry, they send the update again.

// udbTimeSource counts NewTimer calls. streambatcher.Batcher.loop creates one idle timer while
// waiting for a batch, then a maxWait and a gap timer for the first item, and one more gap timer
// for each further item. The count tells the test when the batcher has received an item.
// The count is incremented after the timer is registered, so a test that sees the count can
// advance the clock past the timer's deadline.
type udbTimeSource struct {
	*clock.EventTimeSource
	newTimers atomic.Int32
}

func (c *udbTimeSource) NewTimer(d time.Duration) (<-chan time.Time, clock.Timer) {
	ch, timer := c.EventTimeSource.NewTimer(d)
	c.newTimers.Add(1)
	return ch, timer
}

// udbCall is one UpdateTaskQueueUserData call that reached the task manager.
type udbCall struct {
	Markers map[string]int64 // task queue -> marker of the update
	Err     string
}

// udbTaskManager records UpdateTaskQueueUserData calls. A call whose index is in failCall returns
// that error without reaching the store. A call whose index is in failAfterCall reaches the store
// and then returns that error.
type udbTaskManager struct {
	persistence.TaskManager
	failCall      map[int]error
	failAfterCall map[int]error
	mu            sync.Mutex
	calls         []udbCall
}

func (m *udbTaskManager) UpdateTaskQueueUserData(ctx context.Context, req *persistence.UpdateTaskQueueUserDataRequest) error {
	call := udbCall{Markers: map[string]int64{}}
	for tq, u := range req.Updates {
		call.Markers[tq] = u.UserData.GetData().GetClock().GetWallClock()
	}
	m.mu.Lock()
	err, inject := m.failCall[len(m.calls)]
	errAfter, injectAfter := m.failAfterCall[len(m.calls)]
	m.mu.Unlock()
	if !inject {
		err = m.TaskManager.UpdateTaskQueueUserData(ctx, req)
		if injectAfter {
			err = errAfter
		}
	}
	call.Err = errKind(err)
	m.mu.Lock()
	m.calls = append(m.calls, call)
	m.mu.Unlock()
	return err
}

func (m *udbTaskManager) recordedCalls() []udbCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]udbCall(nil), m.calls...)
}

type udbEnv struct {
	t          *testing.T
	ts         *udbTimeSource
	store      *udbTaskManager
	engine     *matchingEngineImpl
	timers     int32 // timers the batcher is expected to have created once it has the current items
	batchItems int
}

// newUDBEnv creates an engine on a fresh SQLite store. seed maps task queue names to the marker
// stored at version 1.
func newUDBEnv(t *testing.T, failCall map[int]error, seed map[string]int64) *udbEnv {
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(config.SQL{
		PluginName:        "sqlite",
		DatabaseName:      uuid.NewString(),
		ConnectAddr:       "127.0.0.1",
		ConnectProtocol:   "tcp",
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}, resolver.NewNoopResolver(), "test", logger, metrics.NoopMetricsHandler, serialization.NewSerializer())
	taskStore, err := factory.NewTaskStore()
	require.NoError(t, err)
	t.Cleanup(factory.Close)

	env := &udbEnv{
		t:  t,
		ts: &udbTimeSource{EventTimeSource: clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))},
		store: &udbTaskManager{
			TaskManager: persistence.NewTaskManager(taskStore, serialization.NewSerializer()),
			failCall:    failCall,
		},
		timers: 1, // the idle timer created when the batcher goroutine starts
	}
	env.engine = &matchingEngineImpl{taskManager: env.store, timeSource: env.ts, logger: logger}
	env.engine.userDataUpdateBatcher = streambatcher.NewKeyedBatcherWithPerItemResults(
		env.engine.applyUserDataUpdateBatch, userDataBatcherOptions, env.ts)
	// Let the batcher goroutine exit. closeBatch has waited for its idle timer.
	t.Cleanup(func() { env.ts.Advance(userDataBatcherOptions.IdleTime) })

	for tq, marker := range seed {
		require.NoError(t, env.store.TaskManager.UpdateTaskQueueUserData(context.Background(), &persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: {UserData: &persistencespb.VersionedTaskQueueUserData{Version: 0, Data: udbData(marker)}},
			},
		}))
	}
	return env
}

func udbData(marker int64) *persistencespb.TaskQueueUserData {
	return &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: marker, ClusterId: 1}}
}

func udbRequest(tq string, version, marker int64) *matchingservice.UpdateTaskQueueUserDataRequest {
	return &matchingservice.UpdateTaskQueueUserDataRequest{
		NamespaceId: defaultNamespaceId,
		TaskQueue:   tq,
		UserData:    &persistencespb.VersionedTaskQueueUserData{Version: version, Data: udbData(marker)},
	}
}

// udbStored is the version and marker of persisted or cached user data.
type udbStored struct {
	Version int64
	Marker  int64
}

func udbStoredOf(d *persistencespb.VersionedTaskQueueUserData) udbStored {
	return udbStored{Version: d.GetVersion(), Marker: d.GetData().GetClock().GetWallClock()}
}

func (env *udbEnv) persisted(tq string) udbStored {
	resp, err := env.store.TaskManager.GetTaskQueueUserData(context.Background(), &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: defaultNamespaceId, TaskQueue: tq,
	})
	require.NoError(env.t, err)
	return udbStoredOf(resp.UserData)
}

// errKind reduces an error to its type, so that results can be compared as values.
func errKind(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, errUserDataUpdateSuperseded) {
		return "Superseded"
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return "InvalidArgument"
	}
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return "Unavailable"
	}
	if persistence.IsConflictErr(err) {
		return "ConditionFailed"
	}
	return fmt.Sprintf("%T", err)
}

func (env *udbEnv) waitTimers(n int32) {
	env.t.Helper()
	await.RequireTrue(env.t, func() bool { return env.ts.newTimers.Load() >= n }, 10*time.Second, time.Millisecond)
	require.Equal(env.t, n, env.ts.newTimers.Load())
}

// add runs fn on a new goroutine. fn must add exactly one item to the batcher. add returns once the
// batcher has received the item, so all items added before closeBatch are in one batch.
func (env *udbEnv) add(fn func()) <-chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		fn()
		done <- struct{}{}
	}()
	if env.batchItems == 0 {
		env.timers += 2 // maxWait and gap
	} else {
		env.timers++ // gap
	}
	env.batchItems++
	env.waitTimers(env.timers)
	return done
}

// closeBatch processes the current batch, waits for dones and for the batcher to become idle.
func (env *udbEnv) closeBatch(dones ...<-chan struct{}) {
	env.ts.Advance(userDataBatcherOptions.MinDelay)
	for _, done := range dones {
		await.Rcv(env.t, done)
	}
	env.batchItems = 0
	env.timers++ // idle
	env.waitTimers(env.timers)
}

func (env *udbEnv) runBatch(fns ...func()) {
	var dones []<-chan struct{}
	for _, fn := range fns {
		dones = append(dones, env.add(fn))
	}
	env.closeBatch(dones...)
}

// updateRPCs sends reqs to the engine. With oneBatch, all requests are in one batch. Otherwise
// each request is in its own batch, in order.
func (env *udbEnv) updateRPCs(oneBatch bool, reqs ...*matchingservice.UpdateTaskQueueUserDataRequest) []string {
	errs := make([]error, len(reqs))
	fns := make([]func(), len(reqs))
	for i, req := range reqs {
		fns[i] = func() { _, errs[i] = env.engine.UpdateTaskQueueUserData(context.Background(), req) }
	}
	if oneBatch {
		env.runBatch(fns...)
	} else {
		for _, fn := range fns {
			env.runBatch(fn)
		}
	}
	kinds := make([]string, len(errs))
	for i, err := range errs {
		kinds[i] = errKind(err)
	}
	return kinds
}

// requireAcknowledgedPersisted checks that each request that returned success is the persisted
// user data of its task queue at the next version.
func (env *udbEnv) requireAcknowledgedPersisted(reqs []*matchingservice.UpdateTaskQueueUserDataRequest, kinds []string) {
	env.t.Helper()
	for i, req := range reqs {
		if kinds[i] == "ok" {
			want := udbStored{Version: req.GetUserData().GetVersion() + 1, Marker: req.GetUserData().GetData().GetClock().GetWallClock()}
			require.Equal(env.t, want, env.persisted(req.GetTaskQueue()), "request %d was acknowledged but is not persisted", i)
		}
	}
}

// TestUserDataBatchSameTaskQueue sends two updates for the same task queue in one batch. Only the
// second update is written, and the first gets a retryable error. The test then sends the first
// update again in a new batch, as a client retry would, and checks that it is compared with the
// current version. As a control, the same two updates in separate batches are both version checked.
func TestUserDataBatchSameTaskQueue(t *testing.T) {
	t.Parallel()

	type result struct {
		RPCs      []string
		Calls     []udbCall
		Persisted udbStored
	}
	testCases := []struct {
		name     string
		versionB int64
		want     result
		// resendA is the result of sending the first update again after the batch.
		resendA string
	}{
		{
			name:     "same base version",
			versionB: 1,
			want: result{
				RPCs:      []string{"Superseded", "ok"},
				Calls:     []udbCall{{Markers: map[string]int64{"tq": 200}, Err: "ok"}},
				Persisted: udbStored{Version: 2, Marker: 200},
			},
			resendA: "InvalidArgument",
		},
		{
			// The second update is selected even though its version check fails.
			name:     "stale second update",
			versionB: 7,
			want: result{
				RPCs:      []string{"Superseded", "InvalidArgument"},
				Calls:     []udbCall{{Markers: map[string]int64{"tq": 200}, Err: "ConditionFailed"}},
				Persisted: udbStored{Version: 1, Marker: 1},
			},
			resendA: "ok",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newUDBEnv(t, nil, map[string]int64{"tq": 1})
			reqs := []*matchingservice.UpdateTaskQueueUserDataRequest{udbRequest("tq", 1, 100), udbRequest("tq", tc.versionB, 200)}
			rpcs := env.updateRPCs(true, reqs...)
			env.requireAcknowledgedPersisted(reqs, rpcs)
			require.Equal(t, tc.want, result{RPCs: rpcs, Calls: env.store.recordedCalls(), Persisted: env.persisted("tq")})

			resend := env.updateRPCs(true, reqs[0])
			env.requireAcknowledgedPersisted(reqs[:1], resend)
			require.Equal(t, []string{tc.resendA}, resend)
		})
	}
	t.Run("separate batches", func(t *testing.T) {
		t.Parallel()
		env := newUDBEnv(t, nil, map[string]int64{"tq": 1})
		reqs := []*matchingservice.UpdateTaskQueueUserDataRequest{udbRequest("tq", 1, 100), udbRequest("tq", 1, 200)}
		rpcs := env.updateRPCs(false, reqs...)
		env.requireAcknowledgedPersisted(reqs, rpcs)
		require.Equal(t, result{
			RPCs:      []string{"ok", "InvalidArgument"},
			Calls:     []udbCall{{Markers: map[string]int64{"tq": 100}, Err: "ok"}, {Markers: map[string]int64{"tq": 200}, Err: "ConditionFailed"}},
			Persisted: udbStored{Version: 2, Marker: 100},
		}, result{RPCs: rpcs, Calls: env.store.recordedCalls(), Persisted: env.persisted("tq")})
	})
}

// TestUserDataBatchDistinctTaskQueues checks that updates for different task queues in one batch
// are written in a single persistence call.
func TestUserDataBatchDistinctTaskQueues(t *testing.T) {
	t.Parallel()
	env := newUDBEnv(t, nil, map[string]int64{"tq1": 1, "tq2": 2})
	reqs := []*matchingservice.UpdateTaskQueueUserDataRequest{udbRequest("tq1", 1, 100), udbRequest("tq2", 1, 200)}

	rpcs := env.updateRPCs(true, reqs...)

	env.requireAcknowledgedPersisted(reqs, rpcs)
	require.Equal(t, []string{"ok", "ok"}, rpcs)
	require.Equal(t, []udbCall{{Markers: map[string]int64{"tq1": 100, "tq2": 200}, Err: "ok"}}, env.store.recordedCalls())
}

// TestUserDataBatchMultipleDuplicates checks a batch with several updates for one task queue and
// an update for another task queue. Each update gets its own result, and the batch is written in
// one persistence call. All calls for a batch are recorded before its results are returned, so a
// second call would show in the recorded calls.
func TestUserDataBatchMultipleDuplicates(t *testing.T) {
	t.Parallel()
	env := newUDBEnv(t, nil, map[string]int64{"tq1": 1, "tq2": 2})
	reqs := []*matchingservice.UpdateTaskQueueUserDataRequest{
		udbRequest("tq1", 1, 100),
		udbRequest("tq2", 1, 200),
		udbRequest("tq1", 1, 300),
		udbRequest("tq1", 1, 400),
	}

	rpcs := env.updateRPCs(true, reqs...)

	env.requireAcknowledgedPersisted(reqs, rpcs)
	require.Equal(t, []string{"Superseded", "ok", "Superseded", "ok"}, rpcs)
	require.Equal(t, []udbCall{{Markers: map[string]int64{"tq1": 400, "tq2": 200}, Err: "ok"}}, env.store.recordedCalls())
}

// TestUserDataBatchPersistenceErrors checks the results of a batch with a repeated task queue when
// the persistence call returns an error. The handler maps the error with the Applied and
// Conflicting flags that the store set for each update, so the results also check those flags.
func TestUserDataBatchPersistenceErrors(t *testing.T) {
	t.Parallel()

	injected := serviceerror.NewUnavailable("injected")
	type result struct {
		RPCs      []string
		Calls     []udbCall
		Persisted map[string]udbStored
	}
	testCases := []struct {
		name          string
		versionB      int64
		failCall      map[int]error
		failAfterCall map[int]error
		want          result
	}{
		{
			name:     "conflict on another task queue",
			versionB: 7,
			want: result{
				// The SQLite version check fails for tq2 and the transaction is rolled back, so
				// nothing is written. Conflicting is set only for tq2, which gets InvalidArgument.
				// The last tq1 update is not applied and not conflicting, so it gets Unavailable.
				RPCs:      []string{"Superseded", "InvalidArgument", "Unavailable"},
				Calls:     []udbCall{{Markers: map[string]int64{"tq1": 300, "tq2": 200}, Err: "ConditionFailed"}},
				Persisted: map[string]udbStored{"tq1": {Version: 1, Marker: 1}, "tq2": {Version: 1, Marker: 2}},
			},
		},
		{
			name:     "error before write",
			versionB: 1,
			failCall: map[int]error{0: injected},
			want: result{
				// The call does not reach the store. No update is applied, so all get errors.
				RPCs:      []string{"Superseded", "Unavailable", "Unavailable"},
				Calls:     []udbCall{{Markers: map[string]int64{"tq1": 300, "tq2": 200}, Err: "Unavailable"}},
				Persisted: map[string]udbStored{"tq1": {Version: 1, Marker: 1}, "tq2": {Version: 1, Marker: 2}},
			},
		},
		{
			name:          "error after write",
			versionB:      1,
			failAfterCall: map[int]error{0: injected},
			want: result{
				// The store writes both updates and sets Applied, then the test injects an error.
				// The applied updates succeed despite the error. The superseded update was not
				// applied, so it still gets an error.
				RPCs:      []string{"Superseded", "ok", "ok"},
				Calls:     []udbCall{{Markers: map[string]int64{"tq1": 300, "tq2": 200}, Err: "Unavailable"}},
				Persisted: map[string]udbStored{"tq1": {Version: 2, Marker: 300}, "tq2": {Version: 2, Marker: 200}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newUDBEnv(t, tc.failCall, map[string]int64{"tq1": 1, "tq2": 2})
			env.store.failAfterCall = tc.failAfterCall
			reqs := []*matchingservice.UpdateTaskQueueUserDataRequest{
				udbRequest("tq1", 1, 100),
				udbRequest("tq2", tc.versionB, 200),
				udbRequest("tq1", 1, 300),
			}
			rpcs := env.updateRPCs(true, reqs...)
			env.requireAcknowledgedPersisted(reqs, rpcs)
			require.Equal(t, tc.want, result{
				RPCs:      rpcs,
				Calls:     env.store.recordedCalls(),
				Persisted: map[string]udbStored{"tq1": env.persisted("tq1"), "tq2": env.persisted("tq2")},
			})
		})
	}
}

// TestUserDataBatchCanceledCaller checks that an update stays in its batch after its caller's
// context is canceled, and that the superseded update still gets an error.
func TestUserDataBatchCanceledCaller(t *testing.T) {
	t.Parallel()
	env := newUDBEnv(t, nil, map[string]int64{"tq": 1})

	ctxB, cancelB := context.WithCancel(context.Background())
	var errA, errB error
	doneA := env.add(func() { _, errA = env.engine.UpdateTaskQueueUserData(context.Background(), udbRequest("tq", 1, 100)) })
	doneB := env.add(func() { _, errB = env.engine.UpdateTaskQueueUserData(ctxB, udbRequest("tq", 1, 200)) })
	cancelB()
	await.Rcv(t, doneB)
	env.closeBatch(doneA)

	require.Equal(t, []string{"Superseded", "Canceled"}, []string{errKind(errA), errKind(errB)})
	require.Equal(t, []udbCall{{Markers: map[string]int64{"tq": 200}, Err: "ok"}}, env.store.recordedCalls())
	require.Equal(t, udbStored{Version: 2, Marker: 200}, env.persisted("tq"))
}

// udbMatchingClient sends user data updates from managers to the engine, without retries, and
// records replication requests.
type udbMatchingClient struct {
	matchingservice.MatchingServiceClient
	engine     *matchingEngineImpl
	mu         sync.Mutex
	replicated []int64
}

func (c *udbMatchingClient) UpdateTaskQueueUserData(ctx context.Context, in *matchingservice.UpdateTaskQueueUserDataRequest, _ ...grpc.CallOption) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
	return c.engine.UpdateTaskQueueUserData(ctx, in)
}

func (c *udbMatchingClient) ReplicateTaskQueueUserData(_ context.Context, in *matchingservice.ReplicateTaskQueueUserDataRequest, _ ...grpc.CallOption) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replicated = append(c.replicated, in.GetUserData().GetClock().GetWallClock())
	return &matchingservice.ReplicateTaskQueueUserDataResponse{}, nil
}

// TestUserDataBatchManagers runs two user data managers for the same root partition, both loaded
// at the same version, whose updates land in one batch. Only the manager whose update is persisted
// succeeds, replicates, and advances its cache. The other manager gets an error, cannot overwrite
// the persisted data from its old cached data, and can update again after it refreshes.
func TestUserDataBatchManagers(t *testing.T) {
	t.Parallel()
	env := newUDBEnv(t, nil, map[string]int64{defaultRootTqID: 1})
	ctx := context.Background()
	controller := gomock.NewController(t)

	globalNS := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: defaultNamespaceId, Name: "test-ns"},
		nil,
		&persistencespb.NamespaceReplicationConfig{ActiveClusterName: "active", Clusters: []string{"active", "standby"}},
		1,
	)
	registry := namespace.NewMockRegistry(controller)
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(globalNS, nil).AnyTimes()
	registry.EXPECT().GetNamespaceName(gomock.Any()).Return(globalNS.Name(), nil).AnyTimes()

	client := &udbMatchingClient{engine: env.engine}
	partition := defaultTqId().Partition()
	newManager := func() *userDataManagerImpl {
		m := newUserDataManager(env.store, client,
			func(unloadCause) { t.Error("unexpected onFatalErr") }, nil, nil,
			partition, newTaskQueueConfig(partition.TaskQueue(), defaultTestConfig(), globalNS.Name()),
			log.NewNoopLogger(), registry)
		require.NoError(t, m.loadUserDataFromDB(ctx))
		m.setUserDataState(userDataEnabled, nil)
		return m
	}
	first, second := newManager(), newManager()
	cached := func(m *userDataManagerImpl) udbStored {
		d, _, err := m.GetUserData()
		require.NoError(t, err)
		return udbStoredOf(d)
	}
	// update returns the marker of the data that updateFn was called with, the new version and
	// the error kind.
	update := func(m *userDataManagerImpl, marker int64, replicate bool) (int64, int64, string) {
		var base int64
		version, err := m.UpdateUserData(ctx, UserDataUpdateOptions{Source: "test"},
			func(cur *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
				base = cur.GetClock().GetWallClock()
				return udbData(marker), replicate, nil
			})
		return base, version, errKind(err)
	}

	// Both managers update from version 1 in one batch.
	type managerResult struct {
		Base, Version int64
		Err           string
		Cache         udbStored
	}
	var firstRes, secondRes managerResult
	env.runBatch(
		func() { firstRes.Base, firstRes.Version, firstRes.Err = update(first, 100, true) },
		func() { secondRes.Base, secondRes.Version, secondRes.Err = update(second, 200, true) },
	)
	firstRes.Cache, secondRes.Cache = cached(first), cached(second)
	require.Equal(t, managerResult{Base: 1, Version: 0, Err: "Superseded", Cache: udbStored{Version: 1, Marker: 1}}, firstRes)
	require.Equal(t, managerResult{Base: 1, Version: 2, Err: "ok", Cache: udbStored{Version: 2, Marker: 200}}, secondRes)
	require.Equal(t, udbStored{Version: 2, Marker: 200}, env.persisted(defaultRootTqID))
	client.mu.Lock()
	require.Equal(t, []int64{200}, client.replicated)
	client.mu.Unlock()

	// The first manager updates again from its cached version 1. CAS rejects it.
	var stale managerResult
	env.runBatch(func() { stale.Base, stale.Version, stale.Err = update(first, 300, false) })
	stale.Cache = cached(first)
	require.Equal(t, managerResult{Base: 1, Version: 0, Err: "InvalidArgument", Cache: udbStored{Version: 1, Marker: 1}}, stale)
	require.Equal(t, udbStored{Version: 2, Marker: 200}, env.persisted(defaultRootTqID))

	// After a refresh, the first manager has the persisted data and its update succeeds.
	require.NoError(t, first.refreshUserDataFromDB(ctx))
	require.Equal(t, udbStored{Version: 2, Marker: 200}, cached(first))
	var refreshed managerResult
	env.runBatch(func() { refreshed.Base, refreshed.Version, refreshed.Err = update(first, 300, false) })
	refreshed.Cache = cached(first)
	require.Equal(t, managerResult{Base: 200, Version: 3, Err: "ok", Cache: udbStored{Version: 3, Marker: 300}}, refreshed)
	require.Equal(t, udbStored{Version: 3, Marker: 300}, env.persisted(defaultRootTqID))
}
