// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service matching

package matching

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
)

var _ matchingservice.MatchingServiceClient = (*clientImpl)(nil)

const (
	// DefaultTimeout is the max timeout for regular calls
	DefaultTimeout = time.Minute * debug.TimeoutMultiplier
	// DefaultLongPollTimeout is the max timeout for long poll calls
	DefaultLongPollTimeout = time.Minute * 5 * debug.TimeoutMultiplier
	// evictionCheckInterval is how often departed hosts are reaped from the cache.
	evictionCheckInterval = 30 * time.Second
)

type clientImpl struct {
	timeout              time.Duration
	longPollTimeout      time.Duration
	clients              common.ClientCache
	resolver             membership.ServiceResolver
	connectionCloseDelay dynamicconfig.DurationPropertyFn
	metricsHandler       metrics.Handler
	logger               log.Logger
	loadBalancer         LoadBalancer
	spreadRouting        dynamicconfig.TypedPropertyFn[dynamicconfig.GradualChange[int]]
	partitionCache       *partitionCache
	evictionWatcher      *goro.Handle
}

// NewClient creates a new matching service gRPC client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
	metricsHandler metrics.Handler,
	logger log.Logger,
	lb LoadBalancer,
	spreadRouting dynamicconfig.TypedPropertyFn[dynamicconfig.GradualChange[int]],
	resolver membership.ServiceResolver,
	connectionCloseDelay dynamicconfig.DurationPropertyFn,
) matchingservice.MatchingServiceClient {
	c := &clientImpl{
		timeout:              timeout,
		longPollTimeout:      longPollTimeout,
		clients:              clients,
		resolver:             resolver,
		connectionCloseDelay: connectionCloseDelay,
		metricsHandler:       metricsHandler,
		logger:               logger,
		loadBalancer:         lb,
		spreadRouting:        spreadRouting,
		partitionCache:       newPartitionCache(metricsHandler),
	}

	// Start goroutine to prune partition count cache. Stopped by Stop().
	c.partitionCache.Start()

	// Evict cached clients whose host leaves the membership ring. Stopped by Stop().
	c.evictionWatcher = goro.NewHandle(context.Background()).Go(c.watchMembership)

	return c
}

// Stop deterministically releases the resources started by NewClient: it stops
// the eviction watcher and partition-cache rotation goroutines and closes every
// cached gRPC connection. It is safe to call more than once.
func (c *clientImpl) Stop() {
	c.evictionWatcher.Cancel()
	<-c.evictionWatcher.Done()
	c.partitionCache.Stop()
	c.clients.EvictAll()
}

// watchMembership evicts cached clients whose host leaves the membership ring.
// It runs until ctx is cancelled (by Stop).
func (c *clientImpl) watchMembership(ctx context.Context) error {
	listenerName := fmt.Sprintf("matchingClientCache-%s", uuid.New().String())
	ch := make(chan *membership.ChangedEvent, 1)
	if err := c.resolver.AddListener(listenerName, ch); err != nil {
		c.logger.Error("Failed to subscribe matching cache to membership", tag.Error(err))
		return err
	}
	defer func() { _ = c.resolver.RemoveListener(listenerName) }()

	// Reap departed hosts via a per-address deadline checked by a single ticker;
	// a re-add resets it to the latest removal.
	evictAt := make(map[string]time.Time)
	ticker := time.NewTicker(evictionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-ch:
			for _, h := range event.HostsRemoved {
				evictAt[h.GetAddress()] = time.Now().Add(c.connectionCloseDelay())
			}
			for _, h := range event.HostsAdded {
				delete(evictAt, h.GetAddress())
			}
		case <-ticker.C:
			reapEvictableClients(c.resolver, c.clients, evictAt)
		}
	}
}

func reapEvictableClients(
	resolver membership.ServiceResolver,
	clients common.ClientCache,
	evictAt map[string]time.Time,
) {
	if len(evictAt) == 0 {
		return
	}
	members := make(map[string]struct{})
	for _, m := range resolver.Members() {
		members[m.GetAddress()] = struct{}{}
	}
	now := time.Now()
	for addr, deadline := range evictAt {
		if _, ok := members[addr]; ok {
			delete(evictAt, addr) // back in the ring; cancel the eviction
			continue
		}
		if now.Before(deadline) {
			continue
		}
		clients.Evict(addr)
		delete(evictAt, addr)
	}
}

func (c *clientImpl) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardInfo().GetSourcePartition(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.addActivityTask)
}

func (c *clientImpl) addActivityTask(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.AddActivityTaskRequest,
	opts []grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	request = common.CloneProto(request)
	client, err := c.pickClientForWrite(request.GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()

	return client.AddActivityTask(ctx, request, opts...)
}

func (c *clientImpl) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardInfo().GetSourcePartition(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.addWorkflowTask)
}

func (c *clientImpl) addWorkflowTask(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.AddWorkflowTaskRequest,
	opts []grpc.CallOption,
) (*matchingservice.AddWorkflowTaskResponse, error) {
	request = common.CloneProto(request)
	client, err := c.pickClientForWrite(request.GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddWorkflowTask(ctx, request, opts...)
}

func (c *clientImpl) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetPollRequest().GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.pollActivityTaskQueue)
}

func (c *clientImpl) pollActivityTaskQueue(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts []grpc.CallOption,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	request = common.CloneProto(request)
	client, release, err := c.pickClientForRead(request.GetPollRequest().GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetPollRequest().GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.pollWorkflowTaskQueue)
}

func (c *clientImpl) pollWorkflowTaskQueue(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts []grpc.CallOption,
) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	request = common.CloneProto(request)
	client, release, err := c.pickClientForRead(request.GetPollRequest().GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*matchingservice.QueryWorkflowResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardInfo().GetSourcePartition(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.queryWorkflow)
}

func (c *clientImpl) queryWorkflow(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.QueryWorkflowRequest,
	opts []grpc.CallOption,
) (*matchingservice.QueryWorkflowResponse, error) {
	// use shallow copy since QueryRequest may contain a large payload
	request = &matchingservice.QueryWorkflowRequest{
		NamespaceId:      request.NamespaceId,
		TaskQueue:        common.CloneProto(request.TaskQueue),
		QueryRequest:     request.QueryRequest,
		VersionDirective: request.VersionDirective,
		ForwardInfo:      request.ForwardInfo,
		Priority:         request.Priority,
	}
	client, err := c.pickClientForWrite(request.GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) DispatchNexusTask(
	ctx context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_NEXUS,
		request.GetForwardInfo().GetSourcePartition(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.dispatchNexusTask)
}

func (c *clientImpl) dispatchNexusTask(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.DispatchNexusTaskRequest,
	opts []grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	// use shallow copy since Request may contain a large payload
	request = &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: request.NamespaceId,
		TaskQueue:   common.CloneProto(request.TaskQueue),
		Request:     request.Request,
		ForwardInfo: request.ForwardInfo,
	}
	client, err := c.pickClientForWrite(request.GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DispatchNexusTask(ctx, request, opts...)
}

func (c *clientImpl) PollNexusTaskQueue(
	ctx context.Context,
	request *matchingservice.PollNexusTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	p, loadBalance := c.resolvePartition(
		request.GetRequest().GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_NEXUS,
		request.GetForwardedSource(),
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.pollNexusTaskQueue)
}

func (c *clientImpl) pollNexusTaskQueue(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request *matchingservice.PollNexusTaskQueueRequest,
	opts []grpc.CallOption,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	request = common.CloneProto(request)
	client, release, err := c.pickClientForRead(request.GetRequest().GetTaskQueue(), p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollNexusTaskQueue(ctx, request, opts...)
}

// resolvePartition parses the input task queue partition once and decides how it should be routed.
// It returns the parsed partition and whether the load balancer should choose the final partition
// among the task queue's partitions (true only for a non-forwarded root normal partition; otherwise
// the returned partition is routed to directly). The result is threaded down to
// pickClientFor{Write,Read} so the proto is only parsed once per RPC.
func (c *clientImpl) resolvePartition(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (tqid.Partition, bool) {
	partition, err := tqid.PartitionFromProto(proto, nsid, taskType)
	if err != nil {
		// We preserve the old logic (not returning error in case of invalid proto info) until it's verified that
		// clients are not sending invalid names.
		c.logger.Info("invalid tq partition", tag.Error(err), tag.Stringer("proto", proto))
		metrics.MatchingClientInvalidTaskQueuePartition.With(c.metricsHandler).Record(1)
		return tqid.UnsafeTaskQueueFamily(nsid, proto.GetName()).TaskQueue(taskType).RootPartition(), false
	}

	np, ok := partition.(*tqid.NormalPartition)
	return partition, ok && np.IsRoot() && forwardedFrom == ""
}

// pickClientForWrite mutates the given proto. Callers should copy the proto before if necessary.
// When loadBalance is true the load balancer picks the final partition among p.TaskQueue()'s
// partitions; otherwise p is used directly.
func (c *clientImpl) pickClientForWrite(
	proto *taskqueuepb.TaskQueue,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
) (matchingservice.MatchingServiceClient, error) {
	if loadBalance {
		p = c.loadBalancer.PickWritePartition(p.TaskQueue(), pc)
	}
	proto.Name = p.RpcName()
	return c.getClientForTaskQueuePartition(p)
}

// pickClientForRead mutates the given proto. Callers should copy the proto before if necessary.
// When loadBalance is true the load balancer picks the final partition among p.TaskQueue()'s
// partitions; otherwise p is used directly.
func (c *clientImpl) pickClientForRead(
	proto *taskqueuepb.TaskQueue,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
) (client matchingservice.MatchingServiceClient, release func(), err error) {
	if loadBalance {
		token := c.loadBalancer.PickReadPartition(p.TaskQueue(), pc)
		p = token.TQPartition
		release = token.Release
	}

	proto.Name = p.RpcName()
	client, err = c.getClientForTaskQueuePartition(p)
	return client, release, err
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) Route(p tqid.Partition) (string, error) {
	spreadChange := c.spreadRouting()
	spread := spreadChange.Value(p.GradualChangeKey(), time.Now())
	return c.clients.Lookup(p.RoutingKey(spread))
}

func (c *clientImpl) getClientForTaskQueuePartition(
	partition tqid.Partition,
) (matchingservice.MatchingServiceClient, error) {
	addr, err := c.Route(partition)
	if err != nil {
		return nil, err
	}
	client, err := c.clients.GetClientForClientKey(addr)
	if err != nil {
		return nil, err
	}
	return client.(matchingservice.MatchingServiceClient), nil
}
