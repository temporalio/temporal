// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service matching

package matching

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
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
)

type clientImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	clients         common.ClientCache
	metricsHandler  metrics.Handler
	logger          log.Logger
	loadBalancer    LoadBalancer
}

// NewClient creates a new history service gRPC client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
	metricsHandler metrics.Handler,
	logger log.Logger,
	lb LoadBalancer,
) matchingservice.MatchingServiceClient {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
		metricsHandler:  metricsHandler,
		logger:          logger,
		loadBalancer:    lb,
	}
}

func (c *clientImpl) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddActivityTaskResponse, error) {
	request = common.CloneProto(request)
	client, err := c.pickClientForWrite(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardInfo().GetSourcePartition())
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
	request = common.CloneProto(request)
	client, err := c.pickClientForWrite(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardInfo().GetSourcePartition())
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
	opts ...grpc.CallOption) (*matchingservice.PollActivityTaskQueueResponse, error) {
	request = common.CloneProto(request)
	client, release, err := c.pickClientForRead(
		request.GetPollRequest().GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource())
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
	opts ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	request = common.CloneProto(request)
	client, release, err := c.pickClientForRead(
		request.GetPollRequest().GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource())
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

func (c *clientImpl) QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest, opts ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {
	// use shallow copy since QueryRequest may contain a large payload
	request = &matchingservice.QueryWorkflowRequest{
		NamespaceId:      request.NamespaceId,
		TaskQueue:        common.CloneProto(request.TaskQueue),
		QueryRequest:     request.QueryRequest,
		VersionDirective: request.VersionDirective,
		ForwardInfo:      request.ForwardInfo,
		Priority:         request.Priority,
	}
	client, err := c.pickClientForWrite(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, request.GetForwardInfo().GetSourcePartition())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

// processInputPartition returns a partition in certain cases that load balancer involvement is not necessary,
// otherwise, returns a task queue to pass down to the load balancer.
func (c *clientImpl) processInputPartition(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (tqid.Partition, *tqid.TaskQueue) {
	partition, err := tqid.PartitionFromProto(proto, nsid, taskType)
	if err != nil {
		// We preserve the old logic (not returning error in case of invalid proto info) until it's verified that
		// clients are not sending invalid names.
		c.logger.Info("invalid tq partition", tag.Error(err), tag.NewStringerTag("proto", proto))
		metrics.MatchingClientInvalidTaskQueuePartition.With(c.metricsHandler).Record(1)
		return tqid.UnsafeTaskQueueFamily(nsid, proto.GetName()).TaskQueue(taskType).RootPartition(), nil
	}

	if forwardedFrom != "" || !partition.IsRoot() {
		return partition, nil
	}

	switch p := partition.(type) {
	case *tqid.NormalPartition:
		return nil, p.TaskQueue()
	default:
		return partition, nil
	}
}

// pickClientForWrite mutates the given proto. Callers should copy the proto before if necessary.
func (c *clientImpl) pickClientForWrite(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (matchingservice.MatchingServiceClient, error) {
	p, tq := c.processInputPartition(proto, nsid, taskType, forwardedFrom)
	if tq != nil {
		p = c.loadBalancer.PickWritePartition(tq)
	}
	proto.Name = p.RpcName()
	return c.getClientForTaskQueuePartition(p)
}

// pickClientForRead mutates the given proto. Callers should copy the proto before if necessary.
func (c *clientImpl) pickClientForRead(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (client matchingservice.MatchingServiceClient, release func(), err error) {
	p, tq := c.processInputPartition(proto, nsid, taskType, forwardedFrom)
	if tq != nil {
		token := c.loadBalancer.PickReadPartition(tq)
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

func (c *clientImpl) getClientForTaskQueuePartition(
	partition tqid.Partition,
) (matchingservice.MatchingServiceClient, error) {
	client, err := c.clients.GetClientForKey(partition.RoutingKey())
	if err != nil {
		return nil, err
	}
	return client.(matchingservice.MatchingServiceClient), nil
}
