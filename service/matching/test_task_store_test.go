package matching

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/temporal/environment"
)

const (
	testSQLiteClusterName = "matching_test_sqlite"
	// Matching stores tasks in per-subqueue rows. Scan a generous number so
	// getTaskCount/minTaskID/maxTaskID see every task the tests may write.
	testMaxSubqueues = 32
	// Large enough that matching unit tests (up to ~15k tasks) fit in one page.
	// Fair SQL GetTasks page tokens omit pass on sqlite, so we also paginate by
	// the last decoded task rather than NextPageToken.
	testGetTasksPage = 100000
)

var (
	_ persistence.TaskManager     = (*testTaskManager)(nil)
	_ persistence.FairTaskManager = (*testTaskManager)(nil)
)

// testTaskManager is a matching-test facade: real TaskManager over in-memory SQLite,
// plus helpers to inspect queues, inject faults, and bump range IDs.
type testTaskManager struct {
	persistence.TaskManager

	factory  *sql.Factory
	fairness bool
	logger   log.Logger

	sync.Mutex
	stats          map[dbTaskQueueKey]*testQueuePersistenceStats
	faultInjection map[string]float32 // "op:error" -> fraction of time
	delayInjection time.Duration

	// forceUserData, when set, is returned by GetTaskQueueUserData instead of SQLite.
	// Used by tests that need to simulate the database going backwards in version.
	forcedUserData map[userDataKey]*persistencespb.VersionedTaskQueueUserData
}

type dbTaskQueueKey struct {
	persistenceName string
	namespaceID     string
	taskType        enumspb.TaskQueueType
}

type userDataKey struct {
	namespaceID string
	taskQueue   string
}

type testQueuePersistenceStats struct {
	createTaskCount      int
	createTaskBatchCount int
	getTasksCount        int
	getUserDataCount     int
	createCount          int
	updateCount          int
}

func newTestTaskManager(logger log.Logger) *testTaskManager {
	return newTestTaskManagerWithFairness(logger, false)
}

func newTestFairTaskManager(logger log.Logger) *testTaskManager {
	return newTestTaskManagerWithFairness(logger, true)
}

func newTestTaskManagerWithFairness(logger log.Logger, fairness bool) *testTaskManager {
	cfg := newMatchingSQLiteMemoryConfig()
	serializer := serialization.NewSerializer()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
		serializer,
	)
	var (
		store persistence.TaskStore
		err   error
	)
	if fairness {
		store, err = factory.NewFairTaskStore()
	} else {
		store, err = factory.NewTaskStore()
	}
	if err != nil {
		factory.Close()
		panic(fmt.Sprintf("failed to create sqlite task store: %v", err))
	}
	return &testTaskManager{
		TaskManager:    persistence.NewTaskManager(store, serializer),
		factory:        factory,
		fairness:       fairness,
		logger:         logger,
		stats:          make(map[dbTaskQueueKey]*testQueuePersistenceStats),
		forcedUserData: make(map[userDataKey]*persistencespb.VersionedTaskQueueUserData),
	}
}

func newMatchingSQLiteMemoryConfig() *config.SQL {
	return &config.SQL{
		ConnectAddr:     environment.GetLocalhostIP(),
		ConnectProtocol: "tcp",
		PluginName:      "sqlite",
		DatabaseName:    uuid.NewString(),
		ConnectAttributes: map[string]string{
			"mode":  "memory",
			"cache": "private",
		},
	}
}

func (m *testTaskManager) Close() {
	m.Lock()
	factory := m.factory
	m.factory = nil
	m.Unlock()
	if factory != nil {
		factory.Close()
	}
}

func (m *testTaskManager) GetName() string {
	return "test-sqlite"
}

func (m *testTaskManager) statsFor(q *PhysicalTaskQueueKey) testQueuePersistenceStats {
	return m.statsSnapshot(dbTaskQueueKey{persistenceName: q.PersistenceName(), namespaceID: q.NamespaceId(), taskType: q.TaskType()})
}

func (m *testTaskManager) statsSnapshot(key dbTaskQueueKey) testQueuePersistenceStats {
	m.Lock()
	defer m.Unlock()
	st, ok := m.stats[key]
	if !ok {
		return testQueuePersistenceStats{}
	}
	return *st
}

func (m *testTaskManager) statsPtrLocked(key dbTaskQueueKey) *testQueuePersistenceStats {
	st, ok := m.stats[key]
	if !ok {
		st = &testQueuePersistenceStats{}
		m.stats[key] = st
	}
	return st
}

func (m *testTaskManager) incStat(key dbTaskQueueKey, fn func(*testQueuePersistenceStats)) {
	m.Lock()
	defer m.Unlock()
	fn(m.statsPtrLocked(key))
}

func (m *testTaskManager) queueKeyFromInfo(info *persistencespb.TaskQueueInfo) dbTaskQueueKey {
	return dbTaskQueueKey{
		persistenceName: info.GetName(),
		namespaceID:     info.GetNamespaceId(),
		taskType:        info.GetTaskType(),
	}
}

func (m *testTaskManager) delay() {
	if m.delayInjection > 0 && rand.Int31n(128) >= 13 {
		time.Sleep(time.Duration(rand.Float32() * float32(m.delayInjection))) // nolint:forbidigo
	}
}

// all calls to addFault should be done before starting to call methods on testTaskManager
func (m *testTaskManager) addFault(method, err string, fraction float32) {
	m.Lock()
	defer m.Unlock()
	if m.faultInjection == nil {
		m.faultInjection = make(map[string]float32)
	}
	m.faultInjection[method+":"+err] = fraction
}

func (m *testTaskManager) fault(method, err string) bool {
	m.Lock()
	frac := m.faultInjection[method+":"+err]
	m.Unlock()
	return rand.Float32() < frac
}

func (m *testTaskManager) CreateTaskQueue(
	ctx context.Context,
	request *persistence.CreateTaskQueueRequest,
) (*persistence.CreateTaskQueueResponse, error) {
	if request.TaskQueueInfo != nil && request.TaskQueueInfo.LastUpdateTime == nil {
		request.TaskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	}
	m.delay()
	defer m.delay()
	m.incStat(m.queueKeyFromInfo(request.TaskQueueInfo), func(st *testQueuePersistenceStats) { st.createCount++ })
	return m.TaskManager.CreateTaskQueue(ctx, request)
}

func (m *testTaskManager) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.UpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	if request.TaskQueueInfo != nil && request.TaskQueueInfo.LastUpdateTime == nil {
		request.TaskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	}
	m.delay()
	defer m.delay()
	m.incStat(m.queueKeyFromInfo(request.TaskQueueInfo), func(st *testQueuePersistenceStats) { st.updateCount++ })
	resp, err := m.TaskManager.UpdateTaskQueue(ctx, request)
	if err != nil && ctx.Err() != nil {
		return &persistence.UpdateTaskQueueResponse{}, nil
	}
	return resp, err
}

func (m *testTaskManager) CreateTasks(
	ctx context.Context,
	request *persistence.CreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	m.delay()
	defer m.delay()

	if m.fault("CreateTasks", "ConditionFailed") {
		return nil, &persistence.ConditionFailedError{Msg: "Fake ConditionFailedError"}
	} else if m.fault("CreateTasks", "Unavailable") {
		return nil, serviceerror.NewUnavailable("Fake Unavailable")
	} else if m.fault("CreateTasks", "PersistenceLimit") {
		return nil, persistence.ErrPersistenceNamespaceShardLimitExceeded
	} else if m.fault("CreateTasks", "ConcurrentLimit") {
		return nil, &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "Fake concurrent request limit exceeded",
		}
	}

	resp, err := m.TaskManager.CreateTasks(ctx, request)
	if err != nil {
		return nil, err
	}
	n := len(request.Tasks)
	m.incStat(m.queueKeyFromInfo(request.TaskQueueInfo.Data), func(st *testQueuePersistenceStats) {
		st.createTaskCount += n
		st.createTaskBatchCount++
	})
	return resp, nil
}

func (m *testTaskManager) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.GetTasksResponse, error) {
	m.delay()
	defer m.delay()

	if m.fault("GetTasks", "Unavailable") {
		return nil, serviceerror.NewUnavailablef("GetTasks operation failed")
	}

	resp, err := m.TaskManager.GetTasks(ctx, request)
	if err != nil {
		return nil, err
	}
	m.incStat(dbTaskQueueKey{
		persistenceName: request.TaskQueue,
		namespaceID:     request.NamespaceID,
		taskType:        request.TaskType,
	}, func(st *testQueuePersistenceStats) { st.getTasksCount++ })
	return resp, nil
}

func (m *testTaskManager) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	m.delay()
	defer m.delay()

	// Matching unit tests were written against an in-memory fake that deleted every
	// matching row and returned UnknownNumRowsAffected (same as Cassandra). SQL honors
	// Limit and returns the row count, which leaves tasks around and changes GC.
	// Loop until the range is empty so tests keep the old semantics.
	const page = 10000
	for {
		req := *request
		if req.Limit <= 0 {
			req.Limit = page
		}
		n, err := m.TaskManager.CompleteTasksLessThan(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return persistence.UnknownNumRowsAffected, nil
			}
			return 0, err
		}
		if n == 0 || n == persistence.UnknownNumRowsAffected || n < req.Limit {
			break
		}
	}
	return persistence.UnknownNumRowsAffected, nil
}

func (m *testTaskManager) UpdateTaskQueueUserData(
	ctx context.Context,
	request *persistence.UpdateTaskQueueUserDataRequest,
) error {
	if m.fairness {
		panic("userdata calls should not to go fair task manager")
	}
	// Matching tests seed user data with arbitrary versions. SQL requires version 0 to
	// insert and CAS on later writes. Retry as upsert so seeds behave like the old fake.
	for attempt := 0; attempt < 5; attempt++ {
		err := m.TaskManager.UpdateTaskQueueUserData(ctx, request)
		if err == nil {
			return nil
		}
		if !m.fixUserDataVersions(ctx, request, err) {
			return err
		}
	}
	return m.TaskManager.UpdateTaskQueueUserData(ctx, request)
}

func (m *testTaskManager) fixUserDataVersions(
	ctx context.Context,
	request *persistence.UpdateTaskQueueUserDataRequest,
	updateErr error,
) bool {
	if _, ok := updateErr.(*persistence.ConditionFailedError); !ok && !persistence.IsConflictErr(updateErr) {
		// unique constraint comes back as ConditionFailed or Unavailable wrapping sqlite 1555
		if updateErr == nil {
			return false
		}
		msg := updateErr.Error()
		if !strings.Contains(msg, "UNIQUE constraint") && !strings.Contains(msg, "already exists") {
			return false
		}
	}
	fixed := false
	for tq, update := range request.Updates {
		resp, err := m.TaskManager.GetTaskQueueUserData(ctx, &persistence.GetTaskQueueUserDataRequest{
			NamespaceID: request.NamespaceID,
			TaskQueue:   tq,
		})
		if err != nil {
			if common.IsNotFoundError(err) {
				update.UserData.Version = 0
				fixed = true
			}
			continue
		}
		if update.UserData.GetVersion() != resp.UserData.GetVersion() {
			update.UserData.Version = resp.UserData.GetVersion()
			fixed = true
		}
	}
	return fixed
}

func (m *testTaskManager) GetTaskQueueUserData(
	ctx context.Context,
	request *persistence.GetTaskQueueUserDataRequest,
) (*persistence.GetTaskQueueUserDataResponse, error) {
	if m.fairness {
		panic("userdata calls should not to go fair task manager")
	}
	m.incStat(dbTaskQueueKey{
		persistenceName: request.TaskQueue,
		namespaceID:     request.NamespaceID,
		taskType:        enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}, func(st *testQueuePersistenceStats) { st.getUserDataCount++ })

	m.Lock()
	forced, ok := m.forcedUserData[userDataKey{namespaceID: request.NamespaceID, taskQueue: request.TaskQueue}]
	m.Unlock()
	if ok {
		return &persistence.GetTaskQueueUserDataResponse{UserData: common.CloneProto(forced)}, nil
	}
	return m.TaskManager.GetTaskQueueUserData(ctx, request)
}

func (m *testTaskManager) forceUserData(namespaceID, taskQueue string, data *persistencespb.VersionedTaskQueueUserData) {
	m.Lock()
	defer m.Unlock()
	m.forcedUserData[userDataKey{namespaceID: namespaceID, taskQueue: taskQueue}] = common.CloneProto(data)
}

func (m *testTaskManager) getQueueDataByKey(dbq *PhysicalTaskQueueKey) *testQueueData {
	q := &testQueueData{mgr: m, key: dbq}
	q.reload()
	return q
}

type testQueueData struct {
	sync.Mutex
	mgr           *testTaskManager
	key           *PhysicalTaskQueueKey
	rangeID       int64
	loadedRangeID int64
	info          *persistencespb.TaskQueueInfo
}

func (q *testQueueData) reload() {
	resp, err := q.mgr.TaskManager.GetTaskQueue(context.Background(), &persistence.GetTaskQueueRequest{
		NamespaceID: q.key.NamespaceId(),
		TaskQueue:   q.key.PersistenceName(),
		TaskType:    q.key.TaskType(),
	})
	if err != nil {
		q.rangeID = 0
		q.loadedRangeID = 0
		q.info = nil
		return
	}
	q.rangeID = resp.RangeID
	q.loadedRangeID = resp.RangeID
	q.info = common.CloneProto(resp.TaskQueueInfo)
}

func (q *testQueueData) Lock() {
	q.Mutex.Lock()
	q.reload()
}

func (q *testQueueData) Unlock() {
	if q.rangeID != q.loadedRangeID && q.loadedRangeID != 0 {
		info := common.CloneProto(q.info)
		if info == nil {
			info = &persistencespb.TaskQueueInfo{
				NamespaceId: q.key.NamespaceId(),
				Name:        q.key.PersistenceName(),
				TaskType:    q.key.TaskType(),
			}
		}
		if info.LastUpdateTime == nil {
			info.LastUpdateTime = timestamp.TimeNowPtrUtc()
		}
		_, err := q.mgr.TaskManager.UpdateTaskQueue(context.Background(), &persistence.UpdateTaskQueueRequest{
			RangeID:       q.rangeID,
			TaskQueueInfo: info,
			PrevRangeID:   q.loadedRangeID,
		})
		if err == nil {
			q.loadedRangeID = q.rangeID
		}
	}
	q.Mutex.Unlock()
}

func (q *testQueueData) RangeID() int64 {
	q.Lock()
	defer q.Unlock()
	return q.rangeID
}

func (q *testQueueData) persistenceStats() testQueuePersistenceStats {
	return q.mgr.statsFor(q.key)
}

func (m *testTaskManager) getAllTasks(q *PhysicalTaskQueueKey) []*persistencespb.AllocatedTaskInfo {
	var all []*persistencespb.AllocatedTaskInfo
	for subqueue := 0; subqueue < testMaxSubqueues; subqueue++ {
		minPass := int64(0)
		minID := int64(0)
		if m.fairness {
			minPass = 1
		}
		for {
			req := &persistence.GetTasksRequest{
				NamespaceID:        q.NamespaceId(),
				TaskQueue:          q.PersistenceName(),
				TaskType:           q.TaskType(),
				Subqueue:           subqueue,
				InclusiveMinPass:   minPass,
				InclusiveMinTaskID: minID,
				ExclusiveMaxTaskID: math.MaxInt64,
				PageSize:           testGetTasksPage,
			}
			resp, err := m.TaskManager.GetTasks(context.Background(), req)
			if err != nil || len(resp.Tasks) == 0 {
				break
			}
			all = append(all, resp.Tasks...)
			if len(resp.Tasks) < testGetTasksPage {
				break
			}
			last := resp.Tasks[len(resp.Tasks)-1]
			minID = last.GetTaskId() + 1
			if m.fairness {
				minPass = last.GetTaskPass()
			}
		}
	}
	return all
}

func (m *testTaskManager) getTaskCount(q *PhysicalTaskQueueKey) int {
	return len(m.getAllTasks(q))
}

func (m *testTaskManager) minTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tasks := m.getAllTasks(dbq)
	if len(tasks) == 0 {
		return 0, false
	}
	minID := tasks[0].GetTaskId()
	for _, t := range tasks[1:] {
		if t.GetTaskId() < minID {
			minID = t.GetTaskId()
		}
	}
	return minID, true
}

func (m *testTaskManager) maxTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tasks := m.getAllTasks(dbq)
	if len(tasks) == 0 {
		return 0, false
	}
	maxID := tasks[0].GetTaskId()
	for _, t := range tasks[1:] {
		if t.GetTaskId() > maxID {
			maxID = t.GetTaskId()
		}
	}
	return maxID, true
}

func (m *testTaskManager) getCreateTaskCount(q *PhysicalTaskQueueKey) int {
	return m.statsFor(q).createTaskCount
}

func (m *testTaskManager) getCreateTaskBatchCount(q *PhysicalTaskQueueKey) int {
	return m.statsFor(q).createTaskBatchCount
}

func (m *testTaskManager) getGetTasksCount(q *PhysicalTaskQueueKey) int {
	return m.statsFor(q).getTasksCount
}

func (m *testTaskManager) getGetUserDataCount(q *PhysicalTaskQueueKey) int {
	return m.statsFor(q).getUserDataCount
}

func (m *testTaskManager) getUpdateCount(q *PhysicalTaskQueueKey) int {
	return m.statsFor(q).updateCount
}

