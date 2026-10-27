package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *PartitionManagerTestSuite) TestSingleSessionPollRouting() {
	writeErr := serviceerror.NewUnavailable("assignment failed")
	for _, tc := range []struct {
		name          string
		assigned      bool
		activity      bool
		ordinary      bool
		empty         bool
		forwardErr    error
		assignmentErr error
	}{
		{name: "acquire workflow"},
		{name: "empty poll", empty: true},
		{name: "forwarding failed", forwardErr: context.DeadlineExceeded},
		{name: "assignment failed", assignmentErr: writeErr},
		{name: "assigned polls locally", assigned: true},
		{name: "activity polls locally", activity: true},
		{name: "ordinary sticky poll", ordinary: true},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			tv := testvars.New(t)
			pm := s.newEagerDispatchPartitionManager(0, nil)
			taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
			if tc.activity {
				taskType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
			}
			pm.partition = newTestTaskQueue(namespaceID, tv.TaskQueue().GetName(), taskType).StickyPartition(tv.StickyTaskQueue().GetName())
			pm.matchingClient = s.matchingClient
			queue := pm.defaultQueue().(*MockphysicalTaskQueueManager)
			metadata := &pollMetadata{}
			if !tc.ordinary {
				metadata.sessionOptions = &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
					SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
				}}
			}
			local := tc.assigned || tc.activity || tc.ordinary
			if !tc.activity && !tc.ordinary {
				queue.EXPECT().SingleSessionAssigned().Return(tc.assigned)
			}
			response := &matchingservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("token")}
			var expectedErr error
			if local {
				queue.EXPECT().PollTask(gomock.Any(), metadata).Return(nil, errNoTasks)
				expectedErr = errNoTasks
			} else {
				if tc.empty {
					response = &matchingservice.PollWorkflowTaskQueueResponse{}
				}
				forwarded := s.matchingClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *matchingservice.PollWorkflowTaskQueueRequest, _ ...any) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
						require.Equal(t, pm.partition.RpcName(), request.GetForwardedSource())
						require.Equal(t, pm.partition.TaskQueue().Name(), request.GetPollRequest().GetTaskQueue().GetName())
						require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, request.GetPollRequest().GetTaskQueue().GetKind())
						require.Same(t, metadata.sessionOptions, request.GetPollRequest().GetSessionOptions())
						require.Equal(t, tv.WorkerIdentity(), request.GetPollRequest().GetIdentity())
						return response, tc.forwardErr
					})
				expectedErr = tc.forwardErr
				if tc.empty {
					expectedErr = errNoTasks
				}
				if expectedErr == nil {
					queue.EXPECT().AssignSingleSession().After(forwarded).Return(tc.assignmentErr)
					expectedErr = tc.assignmentErr
				}
			}
			if expectedErr == nil {
				queue.EXPECT().MakePollerScalingDecision(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}
			ctx := context.WithValue(t.Context(), identityKey, tv.WorkerIdentity())
			queue.EXPECT().UpdatePollerInfo(pollerIdentity(tv.WorkerIdentity()), metadata).Times(2)
			task, _, err := pm.PollTask(ctx, metadata)
			if expectedErr != nil {
				require.ErrorIs(t, err, expectedErr)
				require.Nil(t, task)
			} else {
				require.NoError(t, err)
				require.Same(t, response, task.pollWorkflowTaskQueueResponse())
				require.Equal(t, pm.ns.Name(), task.namespace)
			}
		})
	}
}

func TestSingleSessionAssignmentPersists(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t)
	logger := log.NewTestLogger()
	store := newTestTaskManager(logger)
	partition := newTestTaskQueue(tv.NamespaceID().String(), tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition(tv.StickyTaskQueue().GetName())
	queue := UnversionedQueueKey(partition)
	cfg := newTaskQueueConfig(partition.TaskQueue(), NewConfig(dynamicconfig.NewNoopCollection()), tv.NamespaceName())
	db := newTaskQueueDB(cfg, store, queue, logger, metrics.NoopMetricsHandler, false)
	_, err := db.RenewLease(t.Context())
	require.NoError(t, err)
	require.False(t, db.SingleSessionAssigned())
	staleRange := db.rangeID

	changed, err := db.assignSingleSession(t.Context())
	require.NoError(t, err)
	require.True(t, changed)
	persisted, err := store.GetTaskQueue(t.Context(), &persistence.GetTaskQueueRequest{
		NamespaceID: queue.NamespaceId(), TaskQueue: queue.PersistenceName(), TaskType: queue.TaskType(),
	})
	require.NoError(t, err)
	require.True(t, persisted.TaskQueueInfo.GetSingleSessionAssigned())
	require.Nil(t, persisted.TaskQueueInfo.GetExpiryTime())
	require.Equal(t, staleRange+1, persisted.RangeID, "assignment fences snapshots taken before retention")

	changed, err = db.assignSingleSession(t.Context())
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, persisted.RangeID, db.rangeID)

	reloaded := newTaskQueueDB(cfg, store, queue, logger, metrics.NoopMetricsHandler, false)
	_, err = reloaded.RenewLease(t.Context())
	require.NoError(t, err)
	require.True(t, reloaded.SingleSessionAssigned())
	require.NoError(t, reloaded.SyncState(t.Context()))
}

func (s *PriMatcherSuite) TestSingleSessionStickyPollSkipsPriorityForwarder() {
	ctx := s.T().Context()
	tm := s.newTestMatcher(ctx, testMatcherOpts{})
	tm.partition = testMatcherTaskQueue.StickyPartition("session")
	defer tm.Stop()
	forwarder := newPollForwarderTask(1, priorityBacklogPollForwarder)
	tm.data.tasks.Add(forwarder)
	metadata := &pollMetadata{
		sessionOptions: &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: "session"},
		}},
		conditions: &matchingservice.PollConditions{NoWait: true},
	}
	task, err := tm.Poll(ctx, metadata)
	s.Require().ErrorIs(err, errNoTasks)
	s.Require().Nil(task)

	local := s.newBacklogTask(1, make(chan taskResponse, 1))
	tm.data.tasks.Add(local)
	task, err = tm.Poll(ctx, metadata)
	s.Require().NoError(err)
	s.Require().Same(local, task)
}

func (s *MatcherDataSuite) TestSingleSessionAcquisitionSkipsQuery() {
	query := s.newQueryTask("query")
	s.md.tasks.Add(query)
	metadata := &pollMetadata{
		sessionOptions: &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: "session"},
		}},
		forwardedFrom: "session",
	}
	s.Require().Nil(s.pollImmediately(metadata))
	workflow := s.newBacklogTask(1, 0, nil)
	s.md.tasks.Add(workflow)
	result := s.pollImmediately(metadata)
	s.Require().NotNil(result)
	s.Require().Same(workflow, result.task)
	metadata.forwardedFrom = ""
	result = s.pollImmediately(metadata)
	s.Require().NotNil(result)
	s.Require().Same(query, result.task)
}

func (s *PartitionManagerTestSuite) TestSingleSessionConcurrentAcquisition() {
	for _, tc := range []struct {
		name           string
		persistBlocked bool
		empty          bool
		forwardErr     error
	}{
		{name: "forwarding"},
		{name: "persisting assignment", persistBlocked: true},
		{name: "empty acquisition permits retry", empty: true},
		{name: "uncertain acquisition stays closed", forwardErr: context.DeadlineExceeded},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tv := testvars.New(t)
			pm := s.newEagerDispatchPartitionManager(0, nil)
			pm.partition = newTestTaskQueue(namespaceID, tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition(tv.StickyTaskQueue().GetName())
			pm.matchingClient = s.matchingClient
			queue := pm.defaultQueue().(*MockphysicalTaskQueueManager)
			var assigned atomic.Bool
			queue.EXPECT().SingleSessionAssigned().DoAndReturn(assigned.Load).AnyTimes()
			metadata := &pollMetadata{sessionOptions: &sessionpb.WorkerSessionOptions{
				Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
					SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
				},
			}}
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			block := func() error {
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			response := &matchingservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("token")}
			s.matchingClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *matchingservice.PollWorkflowTaskQueueRequest, ...any) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
					if !tc.persistBlocked {
						if err := block(); err != nil {
							return nil, err
						}
					}
					if tc.empty {
						return &matchingservice.PollWorkflowTaskQueueResponse{}, nil
					}
					return response, tc.forwardErr
				})
			if !tc.empty && tc.forwardErr == nil {
				queue.EXPECT().AssignSingleSession().DoAndReturn(func() error {
					assigned.Store(true)
					if tc.persistBlocked {
						return block()
					}
					return nil
				})
				queue.EXPECT().MakePollerScalingDecision(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}
			type result struct {
				task *internalTask
				err  error
			}
			completed := make(chan result, 1)
			go func() {
				task, _, err := pm.PollTask(ctx, metadata)
				completed <- result{task: task, err: err}
			}()
			await.Rcv(t, entered)

			for _, noWait := range []bool{false, true} {
				competing := &pollMetadata{sessionOptions: metadata.sessionOptions,
					conditions: &matchingservice.PollConditions{NoWait: noWait},
				}
				task, _, err := pm.PollTask(ctx, competing)
				require.ErrorIs(t, err, errNoTasks)
				require.Nil(t, task)
			}
			queue.EXPECT().PollTask(gomock.Any(), gomock.Any()).Return(nil, errNoTasks)
			_, _, err := pm.PollTask(ctx, &pollMetadata{})
			require.ErrorIs(t, err, errNoTasks, "ordinary polls do not enter the acquisition guard")

			other := s.newEagerDispatchPartitionManager(0, nil)
			other.partition = newTestTaskQueue(namespaceID, tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition(tv.StickyTaskQueue().GetName() + "-2")
			other.matchingClient = s.matchingClient
			otherQueue := other.defaultQueue().(*MockphysicalTaskQueueManager)
			otherQueue.EXPECT().SingleSessionAssigned().Return(false)
			otherQueue.EXPECT().AssignSingleSession().Return(nil)
			otherQueue.EXPECT().MakePollerScalingDecision(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			s.matchingClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).Return(response, nil)
			task, _, err := other.PollTask(ctx, &pollMetadata{sessionOptions: &sessionpb.WorkerSessionOptions{
				Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
					SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: other.partition.RpcName()},
				},
			}})
			require.NoError(t, err)
			require.NotNil(t, task, "a different session acquires independently")

			close(release)
			first := await.Rcv(t, completed)
			switch {
			case tc.empty:
				require.ErrorIs(t, first.err, errNoTasks)
				require.Nil(t, first.task)
				s.matchingClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).Return(response, nil)
				queue.EXPECT().AssignSingleSession().Return(nil)
				queue.EXPECT().MakePollerScalingDecision(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				task, _, err = pm.PollTask(ctx, metadata)
				require.NoError(t, err)
				require.NotNil(t, task, "confirmed empty acquisition releases the guard")
			case tc.forwardErr != nil:
				require.ErrorIs(t, first.err, tc.forwardErr)
				require.Nil(t, first.task)
				task, _, err = pm.PollTask(ctx, metadata)
				require.ErrorIs(t, err, errNoTasks)
				require.Nil(t, task, "uncertain acquisition must not forward again")
			default:
				require.NoError(t, first.err)
				require.Same(t, response, first.task.pollWorkflowTaskQueueResponse())
				queue.EXPECT().PollTask(gomock.Any(), metadata).Return(nil, errNoTasks)
				task, _, err = pm.PollTask(ctx, metadata)
				require.ErrorIs(t, err, errNoTasks)
				require.Nil(t, task, "assigned polls stay local")
			}
		})
	}
}

func (s *matchingEngineSuite) TestSingleSessionOfflineWorkflowAdmission() {
	for _, tc := range []struct {
		name                      string
		assigned, loaded, missing bool
	}{
		{name: "assigned unloaded", assigned: true},
		{name: "assigned loaded without pollers", assigned: true, loaded: true},
		{name: "ordinary unloaded"},
		{name: "ordinary loaded without pollers", loaded: true},
		{name: "metadata missing", missing: true},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			ctx := t.Context()
			tv := testvars.New(t).WithNamespaceID(s.ns.ID())
			partition := newTestTaskQueue(tv.NamespaceID().String(), tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition(tv.StickyTaskQueue().GetName())
			queue := UnversionedQueueKey(partition)
			if !tc.missing {
				expiry := timestamppb.New(time.Now().Add(time.Hour))
				if tc.assigned {
					expiry = nil
				}
				_, err := s.v1TaskManager.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
					RangeID: 1,
					TaskQueueInfo: &persistencespb.TaskQueueInfo{
						NamespaceId: queue.NamespaceId(), Name: queue.PersistenceName(), TaskType: queue.TaskType(),
						Kind: enumspb.TASK_QUEUE_KIND_STICKY, SingleSessionAssigned: tc.assigned,
						LastUpdateTime: timestamppb.Now(), ExpiryTime: expiry,
					},
				})
				require.NoError(t, err)
			}
			pm, _, err := s.matchingEngine.getTaskQueuePartitionManager(ctx, partition, tc.loaded, loadCauseUnspecified)
			require.NoError(t, err)
			require.False(t, stickyWorkerAvailable(pm))

			_, syncMatch, err := s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
				NamespaceId: tv.NamespaceID().String(), TaskQueue: tv.StickyTaskQueue(), Execution: tv.WorkflowExecution(),
				ScheduledEventId: 7, ScheduleToStartTimeout: durationpb.New(time.Minute),
			})
			require.False(t, syncMatch)
			require.Zero(t, s.v1TaskManager.getTaskCount(newUnversionedRootQueueKey(tv.NamespaceID().String(), tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)))
			if !tc.assigned {
				require.ErrorAs(t, err, new(*serviceerrors.StickyWorkerUnavailable))
				require.Zero(t, s.v1TaskManager.getTaskCount(queue))
				pm, _, err = s.matchingEngine.getTaskQueuePartitionManager(ctx, partition, false, loadCauseUnspecified)
				require.NoError(t, err)
				require.Equal(t, tc.loaded, pm != nil)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, s.v1TaskManager.getTaskCount(queue))
			pm, _, err = s.matchingEngine.getTaskQueuePartitionManager(ctx, partition, false, loadCauseUnspecified)
			require.NoError(t, err)
			require.NotNil(t, pm)
			require.False(t, stickyWorkerAvailable(pm))
			s.matchingEngine.unloadTaskQueuePartition(pm, unloadCauseUnspecified)
			pm, _, err = s.matchingEngine.getTaskQueuePartitionManager(ctx, partition, true, loadCauseUnspecified)
			require.NoError(t, err)
			task, _, err := pm.PollTask(ctx, &pollMetadata{sessionOptions: &sessionpb.WorkerSessionOptions{
				Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
					SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
				},
			}})
			require.NoError(t, err)
			require.NotNil(t, task)
			require.Equal(t, tv.WorkflowID(), task.event.GetData().GetWorkflowId())
			require.Equal(t, int64(7), task.event.GetData().GetScheduledEventId())
			task.finish(taskFinishResult{consumedToken: true})
		})
	}
}

func TestSingleSessionOfflineAdmissionReadFailure(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t)
	store := persistence.NewMockTaskManager(gomock.NewController(t))
	readErr := serviceerror.NewUnavailable("metadata read failed")
	store.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: tv.NamespaceID().String(), TaskQueue: tv.StickyTaskQueue().GetName(), TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}).Return(nil, readErr)
	engine := &matchingEngineImpl{taskManager: store, logger: log.NewTestLogger()}
	_, syncMatch, err := engine.AddWorkflowTask(t.Context(), &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: tv.NamespaceID().String(), TaskQueue: tv.StickyTaskQueue(), Execution: tv.WorkflowExecution(),
	})
	require.ErrorIs(t, err, readErr)
	require.False(t, syncMatch)
	require.Empty(t, engine.partitions)
}
