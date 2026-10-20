package matching

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type userDataChasmTestClient struct {
	tquserdatapb.TaskQueueUserDataServiceClient
	response     *tquserdatapb.GetTaskQueueUserDataResponse
	readErr      error
	writeErr     error
	writeRequest *tquserdatapb.UpsertTaskQueueUserDataRequest
	enforceCAS   bool
	readCalls    int
}

func (c *userDataChasmTestClient) GetTaskQueueUserData(context.Context, *tquserdatapb.GetTaskQueueUserDataRequest, ...grpc.CallOption) (*tquserdatapb.GetTaskQueueUserDataResponse, error) {
	c.readCalls++
	return c.response, c.readErr
}

func (c *userDataChasmTestClient) UpsertTaskQueueUserData(_ context.Context, req *tquserdatapb.UpsertTaskQueueUserDataRequest, _ ...grpc.CallOption) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
	c.writeRequest = req
	if c.enforceCAS {
		if req.GetExpectedVersion() != c.response.GetVersion() {
			return nil, serviceerror.NewFailedPrecondition("conflict")
		}
		c.response.Version++
		copiedData := common.CloneProto(req)
		c.response.PerType, c.response.Clock = copiedData.PerType, copiedData.Clock
	}
	return &tquserdatapb.UpsertTaskQueueUserDataResponse{}, c.writeErr
}

func TestUserDataCHASMDelayedWriterCannotOverwriteNewerData(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name  string
		clock *clockspb.HybridLogicalClock
	}{
		{name: "missing clock"},
		{name: "equal clocks", clock: &clockspb.HybridLogicalClock{WallClock: 10}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			clock := testCase.clock
			newerData := &persistencespb.TaskQueueUserData{
				Clock: clock,
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY): {DeploymentData: &persistencespb.DeploymentData{}},
				},
			}
			client := &userDataChasmTestClient{
				response: &tquserdatapb.GetTaskQueueUserDataResponse{
					Version: 3, Clock: clock,
				},
				enforceCAS: true,
			}
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))
			matchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *matchingservice.UpdateTaskQueueUserDataRequest, ...grpc.CallOption) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
					// A newer DB writer finishes its CHASM write before this writer resumes.
					client.response = &tquserdatapb.GetTaskQueueUserDataResponse{Version: 4, Clock: newerData.Clock, PerType: newerData.PerType}
					return &matchingservice.UpdateTaskQueueUserDataResponse{}, nil
				},
			)
			manager := &userDataManagerImpl{
				matchingClient:  matchingClient,
				chasmClient:     client,
				metricsHandler:  metrics.NoopMetricsHandler,
				partition:       newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0).Partition(),
				config:          &taskQueueConfig{WriteUserDataToChasm: func() bool { return true }},
				logger:          log.NewTestLogger(),
				userDataChanged: make(chan struct{}),
				userData:        &persistencespb.VersionedTaskQueueUserData{Version: 874, Data: &persistencespb.TaskQueueUserData{Clock: clock}},
			}
			updated, _, err := manager.updateUserData(context.Background(), func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
				return userDataChasmTestData(clock), false, nil
			}, UserDataUpdateOptions{})
			require.NoError(t, err)
			require.Equal(t, int64(875), updated.Version)
			require.Equal(t, int64(3), client.writeRequest.GetExpectedVersion())
			require.Equal(t, int64(4), client.response.Version)
			protorequire.ProtoEqual(t, &tquserdatapb.GetTaskQueueUserDataResponse{Version: 4, Clock: newerData.Clock, PerType: newerData.PerType}, client.response)
		})
	}
}

func TestUserDataCHASMUpdateUsesCHASMVersion(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name         string
		chasmClock   *clockspb.HybridLogicalClock
		readErr      error
		writeErr     error
		expectWrite  bool
		expectCreate bool
	}{
		{name: "existing", chasmClock: &clockspb.HybridLogicalClock{WallClock: 10}, expectWrite: true},
		{name: "same clock", chasmClock: &clockspb.HybridLogicalClock{WallClock: 20}, expectWrite: true},
		{name: "missing", readErr: serviceerror.NewNotFound("missing"), expectWrite: true, expectCreate: true},
		{name: "read failed", readErr: serviceerror.NewUnavailable("unavailable")},
		{name: "CHASM newer", chasmClock: &clockspb.HybridLogicalClock{WallClock: 30}},
		{name: "CAS failed", chasmClock: &clockspb.HybridLogicalClock{WallClock: 10}, writeErr: serviceerror.NewFailedPrecondition("conflict"), expectWrite: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := &userDataChasmTestClient{
				readErr:  testCase.readErr,
				writeErr: testCase.writeErr,
			}
			if client.readErr == nil {
				client.response = &tquserdatapb.GetTaskQueueUserDataResponse{
					Version: 3,
					Clock:   testCase.chasmClock,
				}
			}
			partition := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0).Partition()
			manager := &userDataManagerImpl{
				chasmClient:    client,
				metricsHandler: metrics.NoopMetricsHandler,
				partition:      partition,
				config:         &taskQueueConfig{WriteUserDataToChasm: func() bool { return true }},
				logger:         log.NewTestLogger(),
			}
			data := &persistencespb.VersionedTaskQueueUserData{
				Version: 875,
				Data:    userDataChasmTestData(&clockspb.HybridLogicalClock{WallClock: 20}),
			}
			ctx := context.Background()
			manager.upsertUserDataToChasmLocked(ctx, manager.prepareUserDataChasmWriteLocked(ctx, data.Data), userDataChasmWriteReasonUpdate)
			if !testCase.expectWrite {
				require.Nil(t, client.writeRequest)
				return
			}
			req := client.writeRequest
			require.NotNil(t, req)
			require.Equal(t, defaultNamespaceId, req.NamespaceId)
			require.Equal(t, defaultRootTqID, req.TaskQueue)
			protorequire.ProtoEqual(t, &tquserdatapb.UpsertTaskQueueUserDataRequest{
				NamespaceId: defaultNamespaceId, TaskQueue: defaultRootTqID, PerType: data.Data.PerType, Clock: data.Data.Clock, ExpectedVersion: req.ExpectedVersion,
			}, req)
			require.NotSame(t, data.Data.Clock, req.Clock)
			if testCase.expectCreate {
				require.Zero(t, req.GetExpectedVersion())
			} else {
				require.Equal(t, int64(3), req.GetExpectedVersion())
			}
		})
	}
}

func TestUserDataCHASMSeedUsesVersionCAS(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name        string
		dbClock     *clockspb.HybridLogicalClock
		chasmClock  *clockspb.HybridLogicalClock
		missing     bool
		expectWrite bool
	}{
		{name: "DB newer", dbClock: &clockspb.HybridLogicalClock{WallClock: 20}, chasmClock: &clockspb.HybridLogicalClock{WallClock: 10}, expectWrite: true},
		{name: "CHASM clock missing", dbClock: &clockspb.HybridLogicalClock{WallClock: 20}, expectWrite: true},
		{name: "equal clocks", dbClock: &clockspb.HybridLogicalClock{WallClock: 20}, chasmClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "DB older", dbClock: &clockspb.HybridLogicalClock{WallClock: 10}, chasmClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "DB clock missing", chasmClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "CHASM missing", missing: true, expectWrite: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := &userDataChasmTestClient{
				response: &tquserdatapb.GetTaskQueueUserDataResponse{
					Version: 3,
					Clock:   testCase.chasmClock,
					PerType: userDataChasmTestData(testCase.chasmClock).PerType,
				},
			}
			if testCase.missing {
				client.response = nil
				client.readErr = serviceerror.NewNotFound("missing")
			}
			manager := &userDataManagerImpl{
				chasmClient:    client,
				metricsHandler: metrics.NoopMetricsHandler,
				partition:      newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0).Partition(),
				config:         &taskQueueConfig{WriteUserDataToChasm: func() bool { return true }},
				logger:         log.NewTestLogger(),
			}
			manager.seedUserDataToChasmLocked(context.Background(), &persistencespb.VersionedTaskQueueUserData{
				Version: 875,
				Data:    userDataChasmTestData(testCase.dbClock),
			})
			if !testCase.expectWrite {
				require.Nil(t, client.writeRequest)
				return
			}
			require.NotNil(t, client.writeRequest)
			if testCase.missing {
				require.Zero(t, client.writeRequest.GetExpectedVersion())
				return
			}
			require.Equal(t, int64(3), client.writeRequest.GetExpectedVersion())
		})
	}
}

func userDataChasmTestData(clock *clockspb.HybridLogicalClock) *persistencespb.TaskQueueUserData {
	return &persistencespb.TaskQueueUserData{
		Clock: clock,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {DeploymentData: &persistencespb.DeploymentData{}},
		},
	}
}

func TestUserDataCHASMLoadBootstrap(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name         string
		emptyDB      bool
		missingDB    bool
		disabled     bool
		missingChasm bool
		emptyChasm   bool
		chasmClock   *clockspb.HybridLogicalClock
		readErr      error
		writeErr     error
		wantRead     bool
		wantWrite    bool
		warning      string
	}{
		{name: "missing DB", missingDB: true},
		{name: "empty DB", emptyDB: true},
		{name: "disabled", disabled: true},
		{name: "missing CHASM", missingChasm: true, wantRead: true, wantWrite: true},
		{name: "empty CHASM", emptyChasm: true, chasmClock: &clockspb.HybridLogicalClock{WallClock: 30}, wantRead: true, wantWrite: true},
		{name: "empty CHASM with older clock", emptyChasm: true, chasmClock: &clockspb.HybridLogicalClock{WallClock: 10}, wantRead: true, wantWrite: true, warning: "task queue user data clock in db is newer than CHASM"},
		{name: "DB newer", chasmClock: &clockspb.HybridLogicalClock{WallClock: 10}, wantRead: true, wantWrite: true, warning: "task queue user data clock in db is newer than CHASM"},
		{name: "equal clocks", chasmClock: &clockspb.HybridLogicalClock{WallClock: 20}, wantRead: true},
		{name: "CHASM newer", chasmClock: &clockspb.HybridLogicalClock{WallClock: 30}, wantRead: true, warning: "task queue user data clock in db is older than CHASM"},
		{name: "CHASM clock missing", wantRead: true, wantWrite: true, warning: "task queue user data clock in db is newer than CHASM"},
		{name: "read fails", readErr: serviceerror.NewUnavailable("unavailable"), wantRead: true},
		{name: "write fails", missingChasm: true, writeErr: serviceerror.NewUnavailable("unavailable"), wantRead: true, wantWrite: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			db := &persistencespb.VersionedTaskQueueUserData{Version: 875, Data: userDataChasmTestData(&clockspb.HybridLogicalClock{WallClock: 20})}
			if testCase.emptyDB {
				db.Data = &persistencespb.TaskQueueUserData{}
			}
			store := persistence.NewMockTaskManager(gomock.NewController(t))
			if testCase.missingDB {
				db = nil
				store.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("missing"))
			} else {
				store.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).Return(&persistence.GetTaskQueueUserDataResponse{UserData: db}, nil)
			}
			client := &userDataChasmTestClient{
				response: &tquserdatapb.GetTaskQueueUserDataResponse{Version: 3, Clock: testCase.chasmClock, PerType: userDataChasmTestData(testCase.chasmClock).PerType},
				readErr:  testCase.readErr,
				writeErr: testCase.writeErr,
			}
			if testCase.emptyChasm {
				client.response.PerType = nil
			}
			if testCase.missingChasm {
				client.response = nil
				client.readErr = serviceerror.NewNotFound("missing")
			}
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			t.Cleanup(func() { metricsHandler.StopCapture(capture) })
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			logs := logger.StartCapture()
			manager := &userDataManagerImpl{
				store:          store,
				chasmClient:    client,
				metricsHandler: metricsHandler,
				partition:      newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0).Partition(),
				config:         &taskQueueConfig{WriteUserDataToChasm: func() bool { return !testCase.disabled }},
				logger:         logger, userDataChanged: make(chan struct{}),
			}
			require.NoError(t, manager.loadUserDataFromDB(context.Background()))
			protorequire.ProtoEqual(t, db, manager.userData)
			if testCase.wantRead {
				require.Equal(t, 1, client.readCalls)
			} else {
				require.Zero(t, client.readCalls)
			}
			if testCase.wantWrite {
				expectedVersion := int64(3)
				if testCase.missingChasm {
					expectedVersion = 0
				}
				protorequire.ProtoEqual(t, &tquserdatapb.UpsertTaskQueueUserDataRequest{
					NamespaceId: defaultNamespaceId, TaskQueue: defaultRootTqID, ExpectedVersion: expectedVersion, PerType: db.Data.PerType, Clock: db.Data.Clock,
				}, client.writeRequest)
			} else {
				require.Nil(t, client.writeRequest)
			}
			readings := capture.SnapshotMetric("task_queue_user_data_chasm_read_outcomes")
			writes := capture.SnapshotMetric("task_queue_user_data_chasm_write_outcomes")
			if testCase.wantRead {
				require.Len(t, readings, 1)
				outcome := "success"
				if testCase.missingChasm {
					outcome = "not_found"
				} else if testCase.readErr != nil {
					outcome = "error"
				}
				require.Equal(t, outcome, readings[0].Tags["outcome"])
			} else {
				require.Empty(t, readings)
			}
			if testCase.wantWrite {
				require.Len(t, writes, 1)
				reason := "db_clock_newer"
				if testCase.missingChasm || testCase.emptyChasm {
					reason = "missing_user_data"
				}
				outcome := "success"
				if testCase.writeErr != nil {
					outcome = "error"
				}
				require.Equal(t, reason, writes[0].Tags["reason"])
				require.Equal(t, outcome, writes[0].Tags["outcome"])
			} else {
				require.Empty(t, writes)
			}
			if testCase.warning != "" {
				logs.RequireContains(t, testlogger.CapturedLogPattern{
					Level: testlogger.Warn, Message: testCase.warning,
					Tags: map[string]any{"db-task-queue-user-data": testlogger.AnyTagValue, "chasm-task-queue-user-data": testlogger.AnyTagValue},
				})
			}
		})
	}
}
