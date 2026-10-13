package matching

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestWorkerShutdownMarkerLifetime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		requests int
		initErr  error
	}{
		{name: "successful initialization and fan-out", requests: 1},
		{name: "initialization failure", requests: 1, initErr: serviceerror.NewUnavailable("initialization failed")},
		{name: "overlapping shutdowns", requests: 2},
		{name: "overlapping initialization failures", requests: 2, initErr: serviceerror.NewUnavailable("initialization failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			tv := testvars.New(t)
			timeSource := clock.NewEventTimeSource()
			config := defaultTestConfig()
			config.EnableMatchingFanOutForPollCancellation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			registry := namespace.NewMockRegistry(ctrl)
			registry.EXPECT().GetNamespaceName(tv.NamespaceID()).Return(tv.NamespaceName(), nil).Times(tc.requests)
			client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			hostInfo := membership.NewMockHostInfoProvider(ctrl)
			root := tqid.UnsafeTaskQueueFamily(tv.NamespaceID().String(), tv.TaskQueue().GetName()).
				TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
			rootPM := NewMocktaskQueuePartitionManager(ctrl)
			pollPartition := root.TaskQueue().NormalPartition(1)
			pollPM := NewMocktaskQueuePartitionManager(ctrl)
			pollPM.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil).AnyTimes()
			pollPM.EXPECT().LongPollExpirationInterval().Return(time.Minute).AnyTimes()
			availableTask := &internalTask{}
			pollPM.EXPECT().PollTask(gomock.Any(), gomock.Any()).Return(availableTask, false, nil).AnyTimes()
			engine := &matchingEngineImpl{
				config:                config,
				namespaceRegistry:     registry,
				logger:                log.NewNoopLogger(),
				timeSource:            timeSource,
				hostInfoProvider:      hostInfo,
				workerInstancePollers: workerPollerTracker{pollers: make(map[string]map[string]context.CancelFunc)},
				shutdownWorkers: cache.New(shutdownWorkersCacheMaxSize, &cache.Options{
					TTL: shutdownWorkersCacheTTL, TimeSource: timeSource,
				}),
				partitions: map[tqid.PartitionKey]taskQueuePartitionManager{
					root.Key(): rootPM, pollPartition.Key(): pollPM,
				},
				matchingRawClient: &routingMatchingClient{
					MockMatchingServiceClient: client,
					routeFn:                   func(tqid.Partition) (string, error) { return "remote", nil },
				},
			}
			initEntered := make(chan struct{}, tc.requests)
			initRelease := make(chan struct{})
			rootPM.EXPECT().WaitUntilInitialized(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
				initEntered <- struct{}{}
				select {
				case <-initRelease:
					return tc.initErr
				case <-ctx.Done():
					return ctx.Err()
				}
			}).Times(tc.requests)
			fanOutEntered := make(chan struct{}, tc.requests)
			fanOutRelease := make(chan struct{})
			if tc.initErr != nil {
				rootPM.EXPECT().Partition().Return(root).Times(tc.requests)
				rootPM.EXPECT().Stop(unloadCauseInitError)
			} else {
				udm := NewMockuserDataManager(ctrl)
				udm.EXPECT().PartitionScale().Return(&taskqueuespb.PartitionScaleInfo{Read: 2}).Times(tc.requests)
				rootPM.EXPECT().GetUserDataManager().Return(udm).Times(tc.requests)
				hostInfo.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("self")).Times(tc.requests)
				client.EXPECT().CancelOutstandingWorkerPollsPartition(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, _ *matchingservice.CancelOutstandingWorkerPollsPartitionRequest, _ ...grpc.CallOption) (*matchingservice.CancelOutstandingWorkerPollsPartitionResponse, error) {
						fanOutEntered <- struct{}{}
						select {
						case <-fanOutRelease:
							return &matchingservice.CancelOutstandingWorkerPollsPartitionResponse{}, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}).Times(tc.requests)
			}

			checkPolls := func() {
				t.Helper()
				task, _, err := engine.pollTask(t.Context(), pollPartition, &pollMetadata{workerInstanceKey: "stopped-worker"})
				require.ErrorIs(t, err, errNoTasks)
				require.Nil(t, task)
				task, _, err = engine.pollTask(t.Context(), pollPartition, &pollMetadata{workerInstanceKey: "live-worker"})
				require.NoError(t, err)
				require.Same(t, availableTask, task)
			}
			var wg sync.WaitGroup
			t.Cleanup(wg.Wait)
			completed := make(chan error, tc.requests)
			for range tc.requests {
				wg.Go(func() {
					_, err := engine.CancelOutstandingWorkerPolls(t.Context(), &matchingservice.CancelOutstandingWorkerPollsRequest{
						NamespaceId:       tv.NamespaceID().String(),
						TaskQueue:         tv.TaskQueue(),
						TaskQueueType:     enumspb.TASK_QUEUE_TYPE_WORKFLOW,
						WorkerInstanceKey: "stopped-worker",
					})
					completed <- err
				})
				await.Rcv(t, initEntered)
			}

			for range tc.requests {
				// Initialization and fan-out can each outlast the shutdown cache TTL.
				timeSource.Advance(shutdownWorkersCacheTTL + time.Second)
				checkPolls()
				await.Snd(t, initRelease, struct{}{})
				if tc.initErr == nil {
					await.Rcv(t, fanOutEntered)
					timeSource.Advance(shutdownWorkersCacheTTL + time.Second)
					checkPolls()
					await.Snd(t, fanOutRelease, struct{}{})
				}
				require.ErrorIs(t, await.Rcv(t, completed), tc.initErr)
				checkPolls()
			}

			// The last completed request starts a fresh TTL, even after an initialization error.
			timeSource.Advance(shutdownWorkersCacheTTL - time.Second)
			checkPolls()
			timeSource.Advance(2 * time.Second)
			task, _, err := engine.pollTask(t.Context(), pollPartition, &pollMetadata{workerInstanceKey: "stopped-worker"})
			require.NoError(t, err)
			require.Same(t, availableTask, task)
		})
	}
}

func TestWorkerShutdownBeforeRootLoads(t *testing.T) {
	t.Parallel()
	for _, taskType := range []enumspb.TaskQueueType{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		enumspb.TASK_QUEUE_TYPE_NEXUS,
	} {
		for _, fairness := range []bool{false, true} {
			for _, tc := range []struct {
				name       string
				scaleState *persistencespb.PartitionScaleState
				readCount  int
			}{
				{name: "static partitions", readCount: 2},
				{
					name: "persisted dynamic partitions including draining backlog",
					scaleState: &persistencespb.PartitionScaleState{
						Target:        3,
						MaxTarget:     5,
						TargetVersion: 1,
						BacklogState:  []uint64{0b11111},
					},
					readCount: 5,
				},
			} {
				t.Run(fmt.Sprintf("%s/fairness=%t/%s", taskType, fairness, tc.name), func(t *testing.T) {
					t.Parallel()
					ctrl := gomock.NewController(t)
					tv := testvars.New(t)
					logger := log.NewNoopLogger()
					ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
						Id:   tv.NamespaceID().String(),
						Name: tv.NamespaceName().String(),
					}, nil, "")
					registry := namespace.NewMockRegistry(ctrl)
					registry.EXPECT().GetNamespaceName(tv.NamespaceID()).Return(tv.NamespaceName(), nil).AnyTimes()
					registry.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(ns, nil).AnyTimes()

					config := defaultTestConfig()
					config.EnableMatchingFanOutForPollCancellation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
					config.NumTaskqueueReadPartitions = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(2)
					if fairness {
						useFairness(config)
					}
					client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
					client.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
						Return(&matchingservice.GetTaskQueueUserDataResponse{}, nil).AnyTimes()
					client.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any()).
						Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{}, nil).AnyTimes()
					root := createTestMatchingEngine(logger, ctrl, config, &routingMatchingClient{
						MockMatchingServiceClient: client,
						routeFn: func(p tqid.Partition) (string, error) {
							if p.IsRoot() {
								return "self", nil
							}
							return "remote", nil
						},
					}, registry)
					root.workerInstancePollers = workerPollerTracker{pollers: make(map[string]map[string]context.CancelFunc)}
					root.shutdownWorkers = cache.New(shutdownWorkersCacheMaxSize, &cache.Options{TTL: shutdownWorkersCacheTTL})
					root.Start()
					t.Cleanup(root.Stop)

					scaler := NewMockPartitionScaler(ctrl)
					scaler.EXPECT().OnTasks(gomock.Any()).Return(PartitionScalerDecision{NoChange: true}).AnyTimes()
					scaler.EXPECT().Stop().AnyTimes()
					factory := NewMockPartitionScalerFactory(ctrl)
					factory.EXPECT().New(gomock.Any(), gomock.Any(), taskType, gomock.Any(), gomock.Any()).Return(scaler)
					root.partitionScalerFactory = factory

					tq := tqid.UnsafeTaskQueueFamily(tv.NamespaceID().String(), tv.TaskQueue().GetName()).TaskQueue(taskType)
					if tc.scaleState != nil {
						store := root.taskManager
						if fairness {
							store = root.fairTaskManager
						}
						_, err := store.CreateTaskQueue(t.Context(), &persistence.CreateTaskQueueRequest{
							RangeID: 1,
							TaskQueueInfo: &persistencespb.TaskQueueInfo{
								NamespaceId:         tv.NamespaceID().String(),
								Name:                UnversionedQueueKey(tq.RootPartition()).PersistenceName(),
								TaskType:            taskType,
								Kind:                enumspb.TASK_QUEUE_KIND_NORMAL,
								PartitionScaleState: tc.scaleState,
							},
						})
						require.NoError(t, err)
					}

					remote := &matchingEngineImpl{
						logger:                logger,
						timeSource:            clock.NewRealTimeSource(),
						workerInstancePollers: workerPollerTracker{pollers: make(map[string]map[string]context.CancelFunc)},
						shutdownWorkers:       cache.New(shutdownWorkersCacheMaxSize, &cache.Options{TTL: shutdownWorkersCacheTTL}),
						partitions:            make(map[tqid.PartitionKey]taskQueuePartitionManager),
					}
					requests := make(chan *matchingservice.CancelOutstandingWorkerPollsPartitionRequest, 1)
					client.EXPECT().CancelOutstandingWorkerPollsPartition(gomock.Any(), gomock.Any()).
						DoAndReturn(func(ctx context.Context, req *matchingservice.CancelOutstandingWorkerPollsPartitionRequest, _ ...grpc.CallOption) (*matchingservice.CancelOutstandingWorkerPollsPartitionResponse, error) {
							requests <- req
							return remote.CancelOutstandingWorkerPollsPartition(ctx, req)
						})

					// Shutdown arrives before any partition is loaded on either host.
					resp, err := root.CancelOutstandingWorkerPolls(t.Context(), &matchingservice.CancelOutstandingWorkerPollsRequest{
						NamespaceId:       tv.NamespaceID().String(),
						TaskQueue:         tv.TaskQueue(),
						TaskQueueType:     taskType,
						WorkerInstanceKey: "stopped-worker",
						WorkerIdentity:    tv.WorkerIdentity(),
					})
					require.NoError(t, err)
					protorequire.ProtoEqual(t, &matchingservice.CancelOutstandingWorkerPollsResponse{}, resp)
					require.NotNil(t, remote.shutdownWorkers.Get("stopped-worker"), "remote host must learn the shutdown before late polls arrive")
					req := await.Rcv(t, requests)
					var partitionIDs []int
					for _, p := range req.GetPartitions() {
						partitionIDs = append(partitionIDs, int(p.GetNormalPartitionId()))
					}
					expectedIDs := make([]int, tc.readCount-1)
					for i := range expectedIDs {
						expectedIDs[i] = i + 1
					}
					require.ElementsMatch(t, expectedIDs, partitionIDs)

					for _, id := range expectedIDs {
						partition := tq.NormalPartition(id)
						pm := NewMocktaskQueuePartitionManager(ctrl)
						pm.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil).Times(2)
						pm.EXPECT().LongPollExpirationInterval().Return(time.Minute).Times(2)
						availableTask := &internalTask{}
						pm.EXPECT().PollTask(gomock.Any(), gomock.Any()).Return(availableTask, false, nil)
						remote.partitions[partition.Key()] = pm

						task, _, err := remote.pollTask(t.Context(), partition, &pollMetadata{workerInstanceKey: "stopped-worker"})
						require.ErrorIs(t, err, errNoTasks)
						require.Nil(t, task)
						task, _, err = remote.pollTask(t.Context(), partition, &pollMetadata{workerInstanceKey: "live-worker"})
						require.NoError(t, err)
						require.Same(t, availableTask, task)
					}
				})
			}
		}
	}
}
