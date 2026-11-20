package matching

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

// Partitions loaded by these tests use their own namespace ID, so that the engine's loaded counts for it
// start at zero even when another partition manager shares the engine.
const lifecycleNamespaceID = "lifecycle-ns-id"

// loadedCounts returns the engine's non-zero loaded partition and physical queue counts for nsID. Once
// everything it loaded has stopped, it's empty: a partition or queue that started and never stopped
// leaves a positive count, and one stopped twice a negative one.
func loadedCounts(e *matchingEngineImpl, nsID string) map[string]int {
	e.gaugeMetrics.lock.Lock()
	defer e.gaugeMetrics.lock.Unlock()
	counts := make(map[string]int)
	for k, v := range e.gaugeMetrics.loadedTaskQueuePartitionCount {
		if k.namespaceID == nsID && v != 0 {
			counts[fmt.Sprintf("partitions %+v", k)] = v
		}
	}
	for k, v := range e.gaugeMetrics.loadedPhysicalTaskQueueCount {
		if k.namespaceID == nsID && v != 0 {
			counts[fmt.Sprintf("physical queues %+v", k)] = v
		}
	}
	return counts
}

// A partition is stopped by whoever removes it from the engine's map, so an unload that comes after
// engine Stop (or races with it) doesn't stop it again.
func TestEngineStop_ConcurrentUnloadStopsOnce(t *testing.T) {
	for range 20 {
		ctrl := gomock.NewController(t)
		e := newFailoverTestEngine(t, ctrl, namespace.NewMockRegistry(ctrl))
		var pms []*MocktaskQueuePartitionManager
		for i := range 100 {
			partition := newRootPartition(namespaceID, fmt.Sprintf("tq-%d", i), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			pm := NewMocktaskQueuePartitionManager(ctrl)
			pm.EXPECT().Partition().Return(partition).AnyTimes()
			pm.EXPECT().Stop(gomock.Any()) // exactly once
			e.updateTaskQueue(partition, pm)
			pms = append(pms, pm)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, pm := range pms {
			wg.Go(func() {
				<-start
				e.unloadTaskQueuePartition(pm, unloadCauseIdle)
			})
		}
		wg.Go(func() {
			<-start
			e.Stop()
		})
		close(start)
		wg.Wait()

		// Unloads that arrive after Stop find nothing to stop.
		for _, pm := range pms {
			e.unloadTaskQueuePartition(pm, unloadCauseIdle)
		}
		require.Empty(t, e.getTaskQueuePartitions(1000))
	}
}

// Engine Stop waits for an unload that removed a partition before Stop took the map.
func TestEngineStop_WaitsForUnloadInProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		e := newFailoverTestEngine(t, ctrl, namespace.NewMockRegistry(ctrl))

		partition := newRootPartition(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		release := make(chan struct{})
		pm := NewMocktaskQueuePartitionManager(ctrl)
		pm.EXPECT().Partition().Return(partition).AnyTimes()
		pm.EXPECT().Stop(unloadCauseIdle).Do(func(unloadCause) { <-release })
		e.updateTaskQueue(partition, pm)
		go e.unloadTaskQueuePartition(pm, unloadCauseIdle)
		synctest.Wait()

		var engineStopped atomic.Bool
		go func() {
			e.Stop()
			engineStopped.Store(true)
		}()
		synctest.Wait()
		require.False(t, engineStopped.Load())

		close(release)
		synctest.Wait()
		require.True(t, engineStopped.Load())
	})
}

func TestGetTaskQueuePartitionManager_AfterEngineStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, registry := createMockNamespaceCache(ctrl, namespaceName)
	e := newFailoverTestEngine(t, ctrl, registry)
	e.Stop()

	partition := newRootPartition(lifecycleNamespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	pm, _, err := e.getTaskQueuePartitionManager(t.Context(), partition, true, loadCausePoll)
	require.ErrorIs(t, err, errTaskQueueClosed)
	require.Nil(t, pm)
	require.Empty(t, e.getTaskQueuePartitions(10))
	require.Empty(t, loadedCounts(e, lifecycleNamespaceID))
}

// Loads and unloads real partitions while the engine shuts down. Every partition that started is
// stopped exactly once, and none is left in the map.
func TestEngineStop_ConcurrentLoadAndUnload(t *testing.T) {
	for range 5 {
		ctrl := gomock.NewController(t)
		_, registry := createMockNamespaceCache(ctrl, namespaceName)
		e := newFailoverTestEngine(t, ctrl, registry)

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Go(func() {
				<-start
				partition := newRootPartition(lifecycleNamespaceID, fmt.Sprintf("tq-%d", i), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
				pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCausePoll)
				if err == nil && i%2 == 0 {
					e.unloadTaskQueuePartition(pm, unloadCauseIdle)
				}
			})
		}
		wg.Go(func() {
			<-start
			e.Stop()
		})
		close(start)
		wg.Wait()
		cancel()

		require.Empty(t, e.getTaskQueuePartitions(1000))
		require.Empty(t, loadedCounts(e, lifecycleNamespaceID))
	}
}

// setDefaultQueueForTest makes q pm's default queue without running initialize, as if pm had started
// and initialized with it, so that Stop tears q down.
func setDefaultQueueForTest(pm *taskQueuePartitionManagerImpl, q physicalTaskQueueManager) {
	pm.status.Store(common.DaemonStatusStarted)
	close(pm.initDone)
	pm.defaultQueueFuture.Set(q, nil)
}

// newLifecyclePartitionManager builds a root partition manager on the suite's engine, in its own
// namespace ID (see loadedCounts). It isn't started.
func (s *PartitionManagerTestSuite) newLifecyclePartitionManager(tqName string) *taskQueuePartitionManagerImpl {
	e := s.partitionMgr.engine
	f, err := tqid.NewTaskQueueFamily(lifecycleNamespaceID, tqName)
	s.Require().NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
	tqConfig := newTaskQueueConfig(partition.TaskQueue(), e.config, s.ns.Name())
	logger := s.partitionMgr.logger
	pm, err := newTaskQueuePartitionManager(e, s.ns, partition, tqConfig, logger, logger, metrics.NoopMetricsHandler, &mockUserDataManager{})
	s.Require().NoError(err)
	return pm
}

func (s *PartitionManagerTestSuite) startLifecyclePartitionManager(tqName string) *taskQueuePartitionManagerImpl {
	pm := s.newLifecyclePartitionManager(tqName)
	pm.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Require().NoError(pm.WaitUntilInitialized(ctx))
	return pm
}

func (s *PartitionManagerTestSuite) TestStop_Twice() {
	pm := s.startLifecyclePartitionManager(taskQueueName)
	_, err := pm.getVersionedQueue(context.Background(), "", "build-a", nil, true)
	s.Require().NoError(err)
	s.NotEmpty(loadedCounts(pm.engine, lifecycleNamespaceID))

	pm.Stop(unloadCauseIdle)
	pm.Stop(unloadCauseShuttingDown)
	s.Empty(loadedCounts(pm.engine, lifecycleNamespaceID))
}

// The engine can unload a partition after adding it to its map but before starting it. Start must
// then do nothing, and callers waiting for it to initialize get an error instead of waiting forever.
func (s *PartitionManagerTestSuite) TestStop_BeforeStart() {
	pm := s.newLifecyclePartitionManager(taskQueueName)
	pm.Stop(unloadCauseIdle)
	pm.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Require().ErrorIs(pm.WaitUntilInitialized(ctx), errTaskQueueClosed)
	s.Empty(loadedCounts(pm.engine, lifecycleNamespaceID))
}

// Stop right after Start, while initialize may still be creating and starting the default queue.
func (s *PartitionManagerTestSuite) TestStop_DuringInitialize() {
	for i := range 20 {
		pm := s.newLifecyclePartitionManager(fmt.Sprintf("tq-%d", i))
		pm.Start()
		pm.Stop(unloadCauseIdle)
	}
	s.Empty(loadedCounts(s.partitionMgr.engine, lifecycleNamespaceID))
}

func (s *PartitionManagerTestSuite) TestVersionedQueue_NotCreatedAfterStop() {
	pm := s.startLifecyclePartitionManager(taskQueueName)
	pm.Stop(unloadCauseIdle)

	vq, err := pm.getVersionedQueueNoWait("", "build-a", nil, true)
	s.Require().ErrorIs(err, errTaskQueueClosed)
	s.Nil(vq)
	s.Empty(loadedCounts(pm.engine, lifecycleNamespaceID))
}

// Creates versioned queues while the partition stops: each one is either refused or stopped with it.
func (s *PartitionManagerTestSuite) TestVersionedQueue_ConcurrentCreateAndStop() {
	for i := range 5 {
		pm := s.startLifecyclePartitionManager(fmt.Sprintf("tq-%d", i))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for b := range 50 {
			wg.Go(func() {
				<-start
				_, _ = pm.getVersionedQueueNoWait("", fmt.Sprintf("build-%d", b), nil, true)
			})
		}
		wg.Go(func() {
			<-start
			pm.Stop(unloadCauseIdle)
		})
		close(start)
		wg.Wait()
		s.Empty(loadedCounts(pm.engine, lifecycleNamespaceID))
	}
}
