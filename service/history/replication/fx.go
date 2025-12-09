package replication

import (
	"context"
	"math/rand"
	"strconv"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/fx"
)

type (
	ClusterChannelKey struct {
		ClusterName string
	}
)

var Module = fx.Provide(
	NewTaskFetcherFactory,
	func(m persistence.ExecutionManager) ExecutionManager {
		return m
	},
	NewExecutionManagerDLQWriter,
	ClientSchedulerRateLimiterProvider,
	ServerSchedulerRateLimiterProvider,
	PersistenceRateLimiterProvider,
	replicationTaskConverterFactoryProvider,
	replicationTaskExecutorProvider,
	fx.Annotated{
		Name:   "HighPriorityTaskScheduler",
		Target: replicationStreamHighPrioritySchedulerProvider,
	},
	fx.Annotated{
		Name:   "LowPriorityTaskScheduler",
		Target: replicationStreamLowPrioritySchedulerProvider,
	},
	executableTaskConverterProvider,
	streamReceiverMonitorProvider,
	eagerNamespaceRefresherProvider,
	sequentialTaskQueueFactoryProvider,
	dlqWriterAdapterProvider,
	newDLQWriterToggle,
	historyPaginatedFetcherProvider,
	resendHandlerProvider,
	eventImporterProvider,
	historyEventsHandlerProvider,
)

func eagerNamespaceRefresherProvider(
	metadataManager persistence.MetadataManager,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	clientBean client.Bean,
	clusterMetadata cluster.Metadata,
	metricsHandler metrics.Handler,
) EagerNamespaceRefresher {
	return NewEagerNamespaceRefresher(
		metadataManager,
		namespaceRegistry,
		logger,
		clientBean,
		nsreplication.NewTaskExecutor(
			clusterMetadata.GetCurrentClusterName(),
			metadataManager,
			logger,
		),
		clusterMetadata.GetCurrentClusterName(),
		metricsHandler,
	)
}

func replicationTaskConverterFactoryProvider(
	config *configs.Config,
) SourceTaskConverterProvider {
	return func(
		historyEngine historyi.Engine,
		shardContext historyi.ShardContext,
		clientClusterName string,
		serializer serialization.Serializer,
	) SourceTaskConverter {
		return NewSourceTaskConverter(
			historyEngine,
			shardContext.GetNamespaceRegistry(),
			serializer,
			config)
	}
}

func replicationTaskExecutorProvider() TaskExecutorProvider {
	return func(params TaskExecutorParams) TaskExecutor {
		return NewTaskExecutor(
			params.RemoteCluster,
			params.Shard,
			params.RemoteHistoryFetcher,
			params.DeleteManager,
			params.WorkflowCache,
		)
	}
}

func replicationStreamHighPrioritySchedulerProvider(
	config *configs.Config,
	logger log.Logger,
	queueFactory ctasks.SequentialTaskQueueFactory[TrackableExecutableTask],
	lc fx.Lifecycle,
) ctasks.Scheduler[TrackableExecutableTask] {
	// SequentialScheduler has panic wrapper when executing task,
	// if changing the executor, please make sure other executor has panic wrapper
	scheduler := ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationProcessorSchedulerWorkerCount,
		},
		WorkflowKeyHashFn,
		queueFactory,
		logger,
	)
	taskChannelKeyFn := func(e TrackableExecutableTask) ClusterChannelKey {
		return ClusterChannelKey{
			ClusterName: e.SourceClusterName(),
		}
	}
	channelWeightFn := func(key ClusterChannelKey) int {
		return 1
	}
	// This creates a per cluster channel.
	// They share the same weight so it just does a round-robin on all clusters' tasks.
	rrScheduler := ctasks.NewInterleavedWeightedRoundRobinScheduler(
		ctasks.InterleavedWeightedRoundRobinSchedulerOptions[TrackableExecutableTask, ClusterChannelKey]{
			TaskChannelKeyFn: taskChannelKeyFn,
			ChannelWeightFn:  channelWeightFn,
		},
		scheduler,
		logger,
	)
	lc.Append(fx.StartStopHook(rrScheduler.Start, rrScheduler.Stop))
	return rrScheduler
}

func replicationStreamLowPrioritySchedulerProvider(
	rateLimiter ClientSchedulerRateLimiter,
	timeSource clock.TimeSource,
	config *configs.Config,
	nsRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	lc fx.Lifecycle,
) ctasks.Scheduler[TrackableExecutableTask] {
	queueFactory := func(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
		item := task.QueueID()
		workflowKey, ok := item.(definition.WorkflowKey)
		if !ok {
			return NewSequentialTaskQueueWithID(item)
		}
		return NewSequentialTaskQueueWithID(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID)
	}
	taskQueueHashFunc := func(item interface{}) uint32 {
		workflowKey, ok := item.(definition.WorkflowKey)
		if !ok {
			return 0
		}

		idBytes := []byte(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID + "_" + strconv.Itoa(rand.Intn(config.ReplicationLowPriorityTaskParallelism())))
		return farm.Fingerprint32(idBytes)
	}
	// SequentialScheduler has panic wrapper when executing task,
	// if changing the executor, please make sure other executor has panic wrapper
	scheduler := ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationLowPriorityProcessorSchedulerWorkerCount,
		},
		taskQueueHashFunc,
		queueFactory,
		logger,
	)
	taskChannelKeyFn := func(e TrackableExecutableTask) ClusterChannelKey {
		return ClusterChannelKey{
			ClusterName: e.SourceClusterName(),
		}
	}
	channelWeightFn := func(key ClusterChannelKey) int {
		return 1
	}
	taskQuotaRequestFn := func(t TrackableExecutableTask) quotas.Request {
		var taskType string
		var nsName namespace.Name
		replicationTask := t.ReplicationTask()
		if replicationTask != nil {
			taskType = replicationTask.TaskType.String()

			rawTaskInfo := replicationTask.GetRawTaskInfo()
			if rawTaskInfo != nil {
				var err error
				nsName, err = nsRegistry.GetNamespaceName(namespace.ID(replicationTask.GetRawTaskInfo().NamespaceId))
				if err != nil {
					nsName = namespace.EmptyName
				}
			}
		}
		return quotas.NewRequest(
			taskType,
			taskSchedulerToken,
			nsName.String(),
			headers.CallerTypePreemptable,
			0,
			"")
	}
	taskMetricsTagsFn := func(t TrackableExecutableTask) []metrics.Tag {
		replicationTask := t.ReplicationTask()
		var taskType string
		namespaceTag := metrics.NamespaceUnknownTag()
		if replicationTask != nil {
			taskType = replicationTask.TaskType.String()
			rawTaskInfo := replicationTask.GetRawTaskInfo()
			if rawTaskInfo != nil {
				nsName, err := nsRegistry.GetNamespaceName(namespace.ID(replicationTask.GetRawTaskInfo().NamespaceId))
				if err != nil {
					namespaceTag = metrics.NamespaceTag(nsName.String())
				}
			}
		}
		return []metrics.Tag{
			namespaceTag,
			metrics.TaskTypeTag(taskType),
			metrics.OperationTag(taskType), // for backward compatibility
			metrics.TaskPriorityTag(ctasks.PriorityPreemptable.String()),
		}
	}
	// This creates a per cluster channel.
	// They share the same weight so it just does a round-robin on all clusters' tasks.
	rrScheduler := ctasks.NewInterleavedWeightedRoundRobinScheduler(
		ctasks.InterleavedWeightedRoundRobinSchedulerOptions[TrackableExecutableTask, ClusterChannelKey]{
			TaskChannelKeyFn: taskChannelKeyFn,
			ChannelWeightFn:  channelWeightFn,
		},
		scheduler,
		logger,
	)
	ts := ctasks.NewRateLimitedScheduler[TrackableExecutableTask](
		rrScheduler,
		rateLimiter,
		timeSource,
		taskQuotaRequestFn,
		taskMetricsTagsFn,
		ctasks.RateLimitedSchedulerOptions{
			EnableShadowMode: config.ReplicationEnableRateLimit(),
		},
		logger,
		metricsHandler,
	)
	lc.Append(fx.StartStopHook(ts.Start, ts.Stop))
	return ts
}

func sequentialTaskQueueFactoryProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
) ctasks.SequentialTaskQueueFactory[TrackableExecutableTask] {
	return func(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
		if config.EnableReplicationTaskBatching() {
			return NewSequentialBatchableTaskQueue(task, nil, logger, metricsHandler)
		}
		item := task.QueueID()
		workflowKey, ok := item.(definition.WorkflowKey)
		if !ok {
			return NewSequentialTaskQueueWithID(item)
		}
		return NewSequentialTaskQueueWithID(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID)
	}
}

func executableTaskConverterProvider(
	processToolBox ProcessToolBox,
) ExecutableTaskConverter {
	return NewExecutableTaskConverter(processToolBox)
}

func streamReceiverMonitorProvider(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
) StreamReceiverMonitor {
	return NewStreamReceiverMonitor(
		processToolBox,
		taskConverter,
		processToolBox.Config.EnableReplicationStream(),
	)
}

func resendHandlerProvider(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
	shardController shard.Controller,
	config *configs.Config,
	remoteHistoryFetcher eventhandler.HistoryPaginatedFetcher,
	logger log.Logger,
	importer eventhandler.EventImporter,
) eventhandler.ResendHandler {
	return eventhandler.NewResendHandler(
		namespaceRegistry,
		clientBean,
		serializer,
		clusterMetadata,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (historyi.Engine, error) {
			shardContext, err := shardController.GetShardByNamespaceWorkflow(
				namespaceId,
				workflowId,
			)
			if err != nil {
				return nil, err
			}
			return shardContext.GetEngine(ctx)
		},
		remoteHistoryFetcher,
		importer,
		logger,
		config,
	)
}

func eventImporterProvider(
	historyFetcher eventhandler.HistoryPaginatedFetcher,
	shardController shard.Controller,
	serializer serialization.Serializer,
	logger log.Logger,
) eventhandler.EventImporter {
	return eventhandler.NewEventImporter(
		historyFetcher,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (historyi.Engine, error) {
			shardContext, err := shardController.GetShardByNamespaceWorkflow(
				namespaceId,
				workflowId,
			)
			if err != nil {
				return nil, err
			}
			return shardContext.GetEngine(ctx)
		},
		serializer,
		logger,
	)
}

func dlqWriterAdapterProvider(
	dlqWriter *queues.DLQWriter,
	taskSerializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
) *DLQWriterAdapter {
	return NewDLQWriterAdapter(dlqWriter, taskSerializer, clusterMetadata.GetCurrentClusterName())
}

func historyEventsHandlerProvider(
	clusterMetadata cluster.Metadata,
	importer eventhandler.EventImporter,
	shardController shard.Controller,
	logger log.Logger,
) eventhandler.HistoryEventsHandler {
	return eventhandler.NewHistoryEventsHandler(
		clusterMetadata,
		importer,
		shardController,
		logger,
	)
}

func historyPaginatedFetcherProvider(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	logger log.Logger,
) eventhandler.HistoryPaginatedFetcher {
	return eventhandler.NewHistoryPaginatedFetcher(
		namespaceRegistry,
		clientBean,
		serializer,
		logger,
	)
}
