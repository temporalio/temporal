package replicator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
)

const (
	fetchTaskRequestTimeout                   = 10 * time.Second
	pollTimerJitterCoefficient                = 0.2
	pollIntervalSecs                          = 1
	taskProcessorErrorRetryWait               = time.Second
	taskProcessorErrorRetryBackoffCoefficient = 1
	taskProcessorErrorRetryMaxAttampts        = 5
)

func newReplicationMessageProcessor(
	currentCluster string,
	sourceCluster string,
	logger log.Logger,
	remotePeer adminservice.AdminServiceClient,
	metricsHandler metrics.Handler,
	namespaceTaskExecutor nsreplication.TaskExecutor,
	hostInfo membership.HostInfo,
	serviceResolver membership.ServiceResolver,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	matchingClient matchingservice.MatchingServiceClient,
	namespaceRegistry namespace.Registry,
) *replicationMessageProcessor {
	retryPolicy := backoff.NewExponentialRetryPolicy(taskProcessorErrorRetryWait).
		WithBackoffCoefficient(taskProcessorErrorRetryBackoffCoefficient).
		WithMaximumAttempts(taskProcessorErrorRetryMaxAttampts)

	return &replicationMessageProcessor{
		hostInfo:                  hostInfo,
		serviceResolver:           serviceResolver,
		status:                    common.DaemonStatusInitialized,
		currentCluster:            currentCluster,
		sourceCluster:             sourceCluster,
		logger:                    logger,
		remotePeer:                remotePeer,
		namespaceTaskExecutor:     namespaceTaskExecutor,
		metricsHandler:            metricsHandler.WithTags(metrics.OperationTag(metrics.NamespaceReplicationTaskScope)),
		retryPolicy:               retryPolicy,
		lastProcessedMessageID:    -1,
		lastRetrievedMessageID:    -1,
		done:                      make(chan struct{}),
		namespaceReplicationQueue: namespaceReplicationQueue,
		matchingClient:            matchingClient,
		namespaceRegistry:         namespaceRegistry,
	}
}

type (
	replicationMessageProcessor struct {
		hostInfo                  membership.HostInfo
		serviceResolver           membership.ServiceResolver
		status                    int32
		currentCluster            string
		sourceCluster             string
		logger                    log.Logger
		remotePeer                adminservice.AdminServiceClient
		namespaceTaskExecutor     nsreplication.TaskExecutor
		metricsHandler            metrics.Handler
		retryPolicy               backoff.RetryPolicy
		lastProcessedMessageID    int64
		lastRetrievedMessageID    int64
		done                      chan struct{}
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		matchingClient            matchingservice.MatchingServiceClient
		namespaceRegistry         namespace.Registry
	}
)

func (p *replicationMessageProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
}

func (p *replicationMessageProcessor) processorLoop() {
	timer := time.NewTimer(getWaitDuration())

	for {
		select {
		case <-timer.C:
			p.handleReplicationTasks()
			timer.Reset(getWaitDuration())
		case <-p.done:
			timer.Stop()
			return
		}
	}
}

func (p *replicationMessageProcessor) handleReplicationTasks() {
	// The following is a best effort to make sure only one worker is processing tasks for a
	// particular source cluster. When the ring is under reconfiguration, it is possible that
	// for a small period of time two or more workers think they are the owner and try to execute
	// the processing logic. This will not result in correctness issue as namespace replication task
	// processing will be protected by version check.
	info, err := p.serviceResolver.Lookup(p.sourceCluster)
	if err != nil {
		p.logger.Info("Failed to lookup host info. Skip current run")
		return
	}

	if info.Identity() != p.hostInfo.Identity() {
		p.logger.Debug("Worker not responsible for source cluster", tag.ClusterName(p.sourceCluster))
		return
	}

	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(fetchTaskRequestTimeout)
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)
	request := &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            p.currentCluster,
		LastRetrievedMessageId: p.lastRetrievedMessageID,
		LastProcessedMessageId: p.lastProcessedMessageID,
	}
	response, err := p.remotePeer.GetNamespaceReplicationMessages(ctx, request)
	defer cancel()

	if err != nil {
		p.logger.Error("Failed to get replication tasks", tag.Error(err))
		return
	}

	p.logger.Debug("Successfully fetched namespace replication tasks", tag.Counter(len(response.Messages.ReplicationTasks)))

	// TODO: specify a timeout for processing namespace replication tasks
	taskCtx := headers.SetCallerInfo(context.TODO(), headers.SystemPreemptableCallerInfo)
	for taskIndex := range response.Messages.ReplicationTasks {
		task := response.Messages.ReplicationTasks[taskIndex]
		err := backoff.ThrottleRetry(func() error {
			return p.handleReplicationTask(taskCtx, task)
		}, p.retryPolicy, isTransientRetryableError)

		if err != nil {
			metrics.ReplicatorFailures.With(p.metricsHandler).Record(1)
			p.logger.Error("Failed to apply replication tasks", tag.Error(err))

			dlqErr := backoff.ThrottleRetry(func() error {

				return p.putNamespaceReplicationTaskToDLQ(taskCtx, task)
			}, p.retryPolicy, isTransientRetryableError)
			if dlqErr != nil {
				p.logger.Error("Failed to put replication tasks to DLQ", tag.Error(dlqErr))
				metrics.ReplicatorDLQFailures.With(p.metricsHandler).Record(1)
				return
			}
		}
	}

	p.lastProcessedMessageID = response.Messages.GetLastRetrievedMessageId()
	p.lastRetrievedMessageID = response.Messages.GetLastRetrievedMessageId()
}

func (p *replicationMessageProcessor) putNamespaceReplicationTaskToDLQ(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
) error {
	switch task.TaskType {
	case enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK:
		metrics.NamespaceReplicationEnqueueDLQCount.With(p.metricsHandler).
			Record(1,
				metrics.ReplicationTaskTypeTag(task.TaskType),
				metrics.NamespaceTag(task.GetNamespaceTaskAttributes().GetInfo().GetName()),
			)
	case enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA:
		ns, err := p.namespaceRegistry.GetNamespaceByID(namespace.ID(task.GetTaskQueueUserDataAttributes().GetNamespaceId()))
		if err != nil {
			return err
		}
		metrics.NamespaceReplicationEnqueueDLQCount.With(p.metricsHandler).
			Record(1,
				metrics.ReplicationTaskTypeTag(task.TaskType),
				metrics.NamespaceTag(ns.Name().String()),
			)
	default:
		return serviceerror.NewUnavailable(
			fmt.Sprintf("Namespace replication task type not supported: %v", task.TaskType),
		)
	}
	return p.namespaceReplicationQueue.PublishToDLQ(ctx, task)
}

func (p *replicationMessageProcessor) handleReplicationTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
) error {
	metricsTag := metrics.ReplicationTaskTypeTag(task.TaskType)
	metrics.ReplicatorMessages.With(p.metricsHandler).Record(1, metricsTag)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ReplicatorLatency.With(p.metricsHandler).Record(time.Since(startTime), metricsTag)
	}()

	switch task.TaskType {
	case enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK:
		attr := task.GetNamespaceTaskAttributes()
		err := p.namespaceTaskExecutor.Execute(ctx, attr)
		if err != nil {
			p.logger.Error("unable to process namespace replication task",
				tag.WorkflowNamespaceID(attr.Id),
				tag.Error(err))
		}
		return err
	case enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA:
		attr := task.GetTaskQueueUserDataAttributes()
		err := p.handleTaskQueueUserDataReplicationTask(ctx, attr)
		if err != nil {
			p.logger.Error(fmt.Sprintf("unable to process task queue metadata replication task, %v", attr.TaskQueueName),
				tag.WorkflowNamespaceID(attr.NamespaceId),
				tag.Error(err))
		}
		return err
	default:
		return fmt.Errorf("cannot handle replication task of type %v", task.TaskType)
	}
}

func (p *replicationMessageProcessor) handleTaskQueueUserDataReplicationTask(
	ctx context.Context,
	attrs *replicationspb.TaskQueueUserDataAttributes,
) error {
	_, err := p.namespaceRegistry.GetNamespaceByID(namespace.ID(attrs.GetNamespaceId()))
	switch err.(type) {
	case nil:
	case *serviceerror.NamespaceNotFound:
		// The namespace in the request isn't registered on this cluster, drop the replication task.
		// This is okay and enables using the cluster-global replication queue to replicate different namespaces to
		// different sets of clusters.
		// When this cluster is added to the list of replicated clusters for this namespace on the origin cluster, the
		// force replication workflow should be triggered to seed the namespace replication queue with all task queue
		// user data entries for the namespace.
		return nil
	default:
		// return the original err
		return err
	}

	_, err = p.matchingClient.ApplyTaskQueueUserDataReplicationEvent(ctx, &matchingservice.ApplyTaskQueueUserDataReplicationEventRequest{
		NamespaceId: attrs.GetNamespaceId(),
		TaskQueue:   attrs.GetTaskQueueName(),
		UserData:    attrs.GetUserData(),
	})
	return err
}

func (p *replicationMessageProcessor) Stop() {
	close(p.done)
}

func getWaitDuration() time.Duration {
	return backoff.Jitter(time.Duration(pollIntervalSecs)*time.Second, pollTimerJitterCoefficient)
}

func isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		return false
	default:
		return true
	}
}
