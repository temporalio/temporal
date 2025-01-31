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
//go:generate go run ../../cmd/tools/genrpcwrappers -service history

package history

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
}

func (c *clientImpl) StartWorkflowExecution(ctx context.Context, in *historyservice.StartWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) PollMutableState(ctx context.Context, in *historyservice.PollMutableStateRequest, opts ...grpc.CallOption) (*historyservice.PollMutableStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ResetStickyTaskQueue(ctx context.Context, in *historyservice.ResetStickyTaskQueueRequest, opts ...grpc.CallOption) (*historyservice.ResetStickyTaskQueueResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RecordWorkflowTaskStarted(ctx context.Context, in *historyservice.RecordWorkflowTaskStartedRequest, opts ...grpc.CallOption) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RecordActivityTaskStarted(ctx context.Context, in *historyservice.RecordActivityTaskStartedRequest, opts ...grpc.CallOption) (*historyservice.RecordActivityTaskStartedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RespondWorkflowTaskCompleted(ctx context.Context, in *historyservice.RespondWorkflowTaskCompletedRequest, opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RespondWorkflowTaskFailed(ctx context.Context, in *historyservice.RespondWorkflowTaskFailedRequest, opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskFailedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RecordActivityTaskHeartbeat(ctx context.Context, in *historyservice.RecordActivityTaskHeartbeatRequest, opts ...grpc.CallOption) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RespondActivityTaskCompleted(ctx context.Context, in *historyservice.RespondActivityTaskCompletedRequest, opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RespondActivityTaskFailed(ctx context.Context, in *historyservice.RespondActivityTaskFailedRequest, opts ...grpc.CallOption) (*historyservice.RespondActivityTaskFailedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RespondActivityTaskCanceled(ctx context.Context, in *historyservice.RespondActivityTaskCanceledRequest, opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) SignalWorkflowExecution(ctx context.Context, in *historyservice.SignalWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) SignalWithStartWorkflowExecution(ctx context.Context, in *historyservice.SignalWithStartWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RemoveSignalMutableState(ctx context.Context, in *historyservice.RemoveSignalMutableStateRequest, opts ...grpc.CallOption) (*historyservice.RemoveSignalMutableStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) TerminateWorkflowExecution(ctx context.Context, in *historyservice.TerminateWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ResetWorkflowExecution(ctx context.Context, in *historyservice.ResetWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.ResetWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) UpdateWorkflowExecutionOptions(ctx context.Context, in *historyservice.UpdateWorkflowExecutionOptionsRequest, opts ...grpc.CallOption) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RequestCancelWorkflowExecution(ctx context.Context, in *historyservice.RequestCancelWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ScheduleWorkflowTask(ctx context.Context, in *historyservice.ScheduleWorkflowTaskRequest, opts ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) VerifyFirstWorkflowTaskScheduled(ctx context.Context, in *historyservice.VerifyFirstWorkflowTaskScheduledRequest, opts ...grpc.CallOption) (*historyservice.VerifyFirstWorkflowTaskScheduledResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RecordChildExecutionCompleted(ctx context.Context, in *historyservice.RecordChildExecutionCompletedRequest, opts ...grpc.CallOption) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) VerifyChildExecutionCompletionRecorded(ctx context.Context, in *historyservice.VerifyChildExecutionCompletionRecordedRequest, opts ...grpc.CallOption) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ReplicateEventsV2(ctx context.Context, in *historyservice.ReplicateEventsV2Request, opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ReplicateWorkflowState(ctx context.Context, in *historyservice.ReplicateWorkflowStateRequest, opts ...grpc.CallOption) (*historyservice.ReplicateWorkflowStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) SyncShardStatus(ctx context.Context, in *historyservice.SyncShardStatusRequest, opts ...grpc.CallOption) (*historyservice.SyncShardStatusResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) SyncActivity(ctx context.Context, in *historyservice.SyncActivityRequest, opts ...grpc.CallOption) (*historyservice.SyncActivityResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RemoveTask(ctx context.Context, in *historyservice.RemoveTaskRequest, opts ...grpc.CallOption) (*historyservice.RemoveTaskResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, in *historyservice.QueryWorkflowRequest, opts ...grpc.CallOption) (*historyservice.QueryWorkflowResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ReapplyEvents(ctx context.Context, in *historyservice.ReapplyEventsRequest, opts ...grpc.CallOption) (*historyservice.ReapplyEventsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) PurgeDLQMessages(ctx context.Context, in *historyservice.PurgeDLQMessagesRequest, opts ...grpc.CallOption) (*historyservice.PurgeDLQMessagesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RefreshWorkflowTasks(ctx context.Context, in *historyservice.RefreshWorkflowTasksRequest, opts ...grpc.CallOption) (*historyservice.RefreshWorkflowTasksResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) RebuildMutableState(ctx context.Context, in *historyservice.RebuildMutableStateRequest, opts ...grpc.CallOption) (*historyservice.RebuildMutableStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) UpdateWorkflowExecution(ctx context.Context, in *historyservice.UpdateWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) PollWorkflowExecutionUpdate(ctx context.Context, in *historyservice.PollWorkflowExecutionUpdateRequest, opts ...grpc.CallOption) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) SyncWorkflowState(ctx context.Context, in *historyservice.SyncWorkflowStateRequest, opts ...grpc.CallOption) (*historyservice.SyncWorkflowStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) UpdateActivityOptions(ctx context.Context, in *historyservice.UpdateActivityOptionsRequest, opts ...grpc.CallOption) (*historyservice.UpdateActivityOptionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) PauseActivity(ctx context.Context, in *historyservice.PauseActivityRequest, opts ...grpc.CallOption) (*historyservice.PauseActivityResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) UnpauseActivity(ctx context.Context, in *historyservice.UnpauseActivityRequest, opts ...grpc.CallOption) (*historyservice.UnpauseActivityResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *clientImpl) ResetActivity(ctx context.Context, in *historyservice.ResetActivityRequest, opts ...grpc.CallOption) (*historyservice.ResetActivityResponse, error) {
	//TODO implement me
	panic("implement me")
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

func (c *clientImpl) DeepHealthCheck(ctx context.Context, request *historyservice.DeepHealthCheckRequest, opts ...grpc.CallOption) (*historyservice.DeepHealthCheckResponse, error) {
	return c.connections.getOrCreateClientConn(rpcAddress(request.GetHostAddress())).historyClient.DeepHealthCheck(ctx, request, opts...)
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
	_, targetClusterShardID, err := DecodeClusterShardMD(headers.NewGRPCHeaderGetter(ctx))
	if err != nil {
		return nil, err
	}

	var streamClient historyservice.HistoryService_StreamWorkflowReplicationMessagesClient
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		streamClient, err = client.StreamWorkflowReplicationMessages(
			metadata.NewOutgoingContext(ctx, ctxMetadata),
			opts...)
		return err
	}
	if err := c.executeWithRedirect(ctx, targetClusterShardID.ShardID, op); err != nil {
		return nil, err
	}
	return streamClient, nil
}

// getRandomShard returns a random shard ID for history APIs that are shard-agnostic (e.g. namespace or DLQ v2 APIs).
func (c *clientImpl) getRandomShard() int32 {
	// Add 1 at the end because shard IDs are 1-indexed.
	return int32(rand.Intn(int(c.numberOfShards)) + 1)
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
