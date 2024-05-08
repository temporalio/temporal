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
//go:generate go run ../../cmd/tools/rpcwrappers -service history

package history

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
)

var (
	_ historyservice.HistoryServiceClient = (*clientImpl)(nil)
)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Second * 30 * debug.TimeoutMultiplier
)

type clientImpl struct {
	connections     connectionPool
	logger          log.Logger
	numberOfShards  int32
	redirector      redirector
	timeout         time.Duration
	tokenSerializer common.TaskTokenSerializer
	// shardIndex is incremented every time a shard-agnostic API is invoked. It is used to load balance requests
	// across hosts by picking an essentially random host. We use an index here so that we don't need to inject any
	// random number generator in order to make tests deterministic. We use a uint instead of an int because we
	// don't want this to become negative if we ever overflow.
	shardIndex atomic.Uint32
}

// NewClient creates a new history service gRPC client
func NewClient(
	dc *dynamicconfig.Collection,
	historyServiceResolver membership.ServiceResolver,
	logger log.Logger,
	numberOfShards int32,
	rpcFactory RPCFactory,
	timeout time.Duration,
) historyservice.HistoryServiceClient {
	connections := newConnectionPool(historyServiceResolver, rpcFactory)

	var redirector redirector
	if dynamicconfig.HistoryClientOwnershipCachingEnabled.Get(dc)() {
		logger.Info("historyClient: ownership caching enabled")
		redirector = newCachingRedirector(connections, historyServiceResolver, logger)
	} else {
		logger.Info("historyClient: ownership caching disabled")
		redirector = newBasicRedirector(connections, historyServiceResolver)
	}

	return &clientImpl{
		connections:     connections,
		logger:          logger,
		numberOfShards:  numberOfShards,
		redirector:      redirector,
		timeout:         timeout,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
	}
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {

	var shardID int32
	if request.GetShardId() != 0 {
		shardID = request.GetShardId()
	} else if request.GetWorkflowExecution() != nil {
		shardID = c.shardIDFromWorkflowID(request.GetNamespaceId(), request.GetWorkflowExecution().GetWorkflowId())
	} else {
		clientConn := c.connections.getOrCreateClientConn(rpcAddress(request.GetHostAddress()))
		return clientConn.historyClient.DescribeHistoryHost(ctx, request, opts...)
	}

	var response *historyservice.DescribeHistoryHostResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	if err := c.executeWithRedirect(ctx, shardID, op); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetReplicationMessages(
	ctx context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationMessagesResponse, error) {
	requestsByClient := make(map[historyservice.HistoryServiceClient]*historyservice.GetReplicationMessagesRequest)

	for _, token := range request.Tokens {
		client, err := c.redirector.clientForShardID(token.GetShardId())
		if err != nil {
			return nil, err
		}

		if _, ok := requestsByClient[client]; !ok {
			requestsByClient[client] = &historyservice.GetReplicationMessagesRequest{
				ClusterName: request.ClusterName,
			}
		}

		req := requestsByClient[client]
		req.Tokens = append(req.Tokens, token)
	}

	var wg sync.WaitGroup
	wg.Add(len(requestsByClient))
	respChan := make(chan *historyservice.GetReplicationMessagesResponse, len(requestsByClient))
	errChan := make(chan error, 1)
	for client, req := range requestsByClient {
		go func(client historyservice.HistoryServiceClient, request *historyservice.GetReplicationMessagesRequest) {
			defer wg.Done()

			ctx, cancel := c.createContext(ctx)
			defer cancel()
			resp, err := client.GetReplicationMessages(ctx, request, opts...)
			if err != nil {
				c.logger.Warn("Failed to get replication tasks from client", tag.Error(err))
				// Returns service busy error to notify replication
				if _, ok := err.(*serviceerror.ResourceExhausted); ok {
					select {
					case errChan <- err:
					default:
					}
				}
				return
			}
			respChan <- resp
		}(client, req)
	}

	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationMessagesResponse{ShardMessages: make(map[int32]*replicationspb.ReplicationMessages)}
	for resp := range respChan {
		for shardID, tasks := range resp.ShardMessages {
			response.ShardMessages[shardID] = tasks
		}
	}
	var err error
	if len(errChan) > 0 {
		err = <-errChan
	}
	return response, err
}

func (c *clientImpl) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationStatusResponse, error) {
	clientConns := c.connections.getAllClientConns()
	respChan := make(chan *historyservice.GetReplicationStatusResponse, len(clientConns))
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(len(clientConns))
	for _, client := range clientConns {
		historyClient := client.historyClient
		go func(client historyservice.HistoryServiceClient) {
			defer wg.Done()
			resp, err := historyClient.GetReplicationStatus(ctx, request, opts...)
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			} else {
				respChan <- resp
			}
		}(historyClient)
	}
	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationStatusResponse{}
	for resp := range respChan {
		response.Shards = append(response.Shards, resp.Shards...)
	}

	if len(errChan) > 0 {
		err := <-errChan
		return response, err
	}

	return response, nil
}

func (c *clientImpl) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (historyservice.HistoryService_StreamWorkflowReplicationMessagesClient, error) {
	ctxMetadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	_, targetClusterShardID, err := DecodeClusterShardMD(ctxMetadata)
	if err != nil {
		return nil, err
	}
	client, err := c.redirector.clientForShardID(targetClusterShardID.ShardID)
	if err != nil {
		return nil, err
	}
	return client.StreamWorkflowReplicationMessages(
		metadata.NewOutgoingContext(ctx, ctxMetadata),
		opts...,
	)
}

// GetDLQTasks doesn't need redirects or routing because DLQ tasks are not sharded, so it just picks any available host
// in the connection pool (or creates one) and forwards the request to it.
func (c *clientImpl) GetDLQTasks(
	ctx context.Context,
	in *historyservice.GetDLQTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQTasksResponse, error) {
	historyClient, err := c.getAnyClient("GetDLQTasks")
	if err != nil {
		return nil, err
	}
	return historyClient.GetDLQTasks(ctx, in, opts...)
}

func (c *clientImpl) DeleteDLQTasks(
	ctx context.Context,
	in *historyservice.DeleteDLQTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.DeleteDLQTasksResponse, error) {
	historyClient, err := c.getAnyClient("DeleteDLQTasks")
	if err != nil {
		return nil, err
	}
	return historyClient.DeleteDLQTasks(ctx, in, opts...)
}

func (c *clientImpl) ListQueues(
	ctx context.Context,
	in *historyservice.ListQueuesRequest,
	opts ...grpc.CallOption,
) (*historyservice.ListQueuesResponse, error) {
	historyClient, err := c.getAnyClient("ListQueues")
	if err != nil {
		return nil, err
	}
	return historyClient.ListQueues(ctx, in, opts...)
}

func (c *clientImpl) ListTasks(
	ctx context.Context,
	in *historyservice.ListTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.ListTasksResponse, error) {
	// Depth of the shardId field is 2 which is not supported by the rpcwrapper generator.
	// Simply changing the maxDepth for ShardId field in the rpcwrapper generator will
	// cause the generation logic for other methods to find more than one routing fields.

	shardID := in.Request.GetShardId()
	var response *historyservice.ListTasksResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ListTasks(ctx, in, opts...)
		return err
	}
	if err := c.executeWithRedirect(ctx, shardID, op); err != nil {
		return nil, err
	}
	return response, nil
}

// getAnyClient returns an arbitrary client by looking up a client by a sequentially increasing shard ID. This is useful
// for history APIs that are shard-agnostic (e.g. namespace or DLQ v2 APIs).
func (c *clientImpl) getAnyClient(apiName string) (historyservice.HistoryServiceClient, error) {
	// Subtract 1 so that the first index is 0 because Add returns the new value.
	shardIndex := c.shardIndex.Add(1) - 1
	// Add 1 at the end because shard IDs are 1-indexed.
	shardID := shardIndex%uint32(c.numberOfShards) + 1
	client, err := c.redirector.clientForShardID(int32(shardID))
	if err != nil {
		msg := fmt.Sprintf("can't find history host to serve API: %q, err: %v", apiName, err)
		return nil, serviceerror.NewUnavailable(msg)
	}
	return client, nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) shardIDFromWorkflowID(namespaceID, workflowID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID, workflowID, c.numberOfShards)
}

func checkShardID(shardID int32) error {
	if shardID <= 0 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid ShardID: %d", shardID))
	}
	return nil
}

func (c *clientImpl) executeWithRedirect(
	ctx context.Context,
	shardID int32,
	op clientOperation,
) error {
	return c.redirector.execute(ctx, shardID, op)
}
