// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/rpcwrappers -service matching

package matching

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
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
	partition, err := c.pickPartitionForWrite(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource())
	if err != nil {
		return nil, err
	}
	request.TaskQueue.Name = partition.RpcName()
	client, err := c.getClientForTaskQueuePartition(partition)
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
	partition, err := c.pickPartitionForWrite(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource())
	if err != nil {
		return nil, err
	}
	request.TaskQueue.Name = partition.RpcName()
	client, err := c.getClientForTaskQueuePartition(partition)
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
	partition, release, err := c.pickPartitionForRead(
		request.PollRequest.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource())
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	request.PollRequest.TaskQueue.Name = partition.RpcName()
	client, err := c.getClientForTaskQueuePartition(partition)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	partition, release, err := c.pickPartitionForRead(
		request.PollRequest.GetTaskQueue(),
		request.GetNamespaceId(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource())
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	request.PollRequest.TaskQueue.Name = partition.RpcName()
	client, err := c.getClientForTaskQueuePartition(partition)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest, opts ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {
	partition, err := c.pickPartitionForWrite(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, request.GetForwardedSource())
	if err != nil {
		return nil, err
	}
	request.TaskQueue.Name = partition.RpcName()
	client, err := c.getClientForTaskQueuePartition(partition)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) pickPartitionForWrite(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (tqid.Partition, error) {
	partition, err := tqid.PartitionFromProto(proto, nsid, taskType)
	if err != nil {
		// We preserve the old logic (not returning error in case of invalid proto info) until it's verified that
		// clients are not sending invalid names.
		c.logger.Info("invalid tq partition", tag.Error(err), tag.NewStringsTag("proto", []string{proto.String()}))
		metrics.MatchingClientInvalidTaskQueuePartition.With(c.metricsHandler).Record(1)
		return tqid.UnsafeTaskQueueFamily(nsid, proto.GetName()).TaskQueue(taskType).RootPartition(), nil
	}

	if forwardedFrom != "" || !partition.IsRoot() {
		return partition, nil
	}

	switch p := partition.(type) {
	case *tqid.NormalPartition:
		return c.loadBalancer.PickWritePartition(p.TaskQueue()), nil
	default:
		return partition, nil
	}
}

func (c *clientImpl) pickPartitionForRead(proto *taskqueuepb.TaskQueue, nsid string, taskType enumspb.TaskQueueType, forwardedFrom string) (prtn tqid.Partition, release func(), err error) {
	partition, err := tqid.PartitionFromProto(proto, nsid, taskType)
	if err != nil {
		// We preserve the old logic (not returning error in case of invalid proto info) until it's verified that
		// clients are not sending invalid names.
		c.logger.Info("invalid tq partition", tag.Error(err), tag.NewStringsTag("proto", []string{proto.String()}))
		metrics.MatchingClientInvalidTaskQueuePartition.With(c.metricsHandler).Record(1)
		return tqid.UnsafeTaskQueueFamily(nsid, proto.GetName()).TaskQueue(taskType).RootPartition(), nil, nil
	}

	if forwardedFrom != "" || !partition.IsRoot() {
		return partition, nil, nil
	}

	switch p := partition.(type) {
	case *tqid.NormalPartition:
		token := c.loadBalancer.PickReadPartition(p.TaskQueue())
		return token.TQPartition, token.Release, nil
	default:
		return partition, nil, nil
	}
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) getClientForTaskQueuePartition(tqPrtn tqid.Partition) (matchingservice.MatchingServiceClient, error) {
	key := fmt.Sprintf("%s:%s:%d", tqPrtn.NamespaceID(), tqPrtn.RpcName(), int(tqPrtn.TaskType()))
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.(matchingservice.MatchingServiceClient), nil
}
