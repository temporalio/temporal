// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service history

package history

import (
	"context"
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
	"go.temporal.io/server/common/tasktoken"
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
	connections     connectionPool[historyservice.HistoryServiceClient]
	logger          log.Logger
	numberOfShards  int32
	redirector      Redirector[historyservice.HistoryServiceClient]
	timeout         time.Duration
	tokenSerializer *tasktoken.Serializer
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
	connections := NewConnectionPool(historyServiceResolver, rpcFactory, historyservice.NewHistoryServiceClient)

	var redirector Redirector[historyservice.HistoryServiceClient]
	if dynamicconfig.HistoryClientOwnershipCachingEnabled.Get(dc)() {
		logger.Info("historyClient: ownership caching enabled")
		redirector = NewCachingRedirector(
			connections,
			historyServiceResolver,
			logger,
			dynamicconfig.HistoryClientOwnershipCachingStaleTTL.Get(dc),
		)
	} else {
		logger.Info("historyClient: ownership caching disabled")
		redirector = NewBasicRedirector(connections, historyServiceResolver)
	}

	return &clientImpl{
		connections:     connections,
		logger:          logger,
		numberOfShards:  numberOfShards,
		redirector:      redirector,
		timeout:         timeout,
		tokenSerializer: tasktoken.NewSerializer(),
	}
}

func (c *clientImpl) DeepHealthCheck(ctx context.Context, request *historyservice.DeepHealthCheckRequest, opts ...grpc.CallOption) (*historyservice.DeepHealthCheckResponse, error) {
	return c.connections.getOrCreateClientConn(rpcAddress(request.GetHostAddress())).grpcClient.DeepHealthCheck(ctx, request, opts...)
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
		return clientConn.grpcClient.DescribeHistoryHost(ctx, request, opts...)
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
		historyClient := client.grpcClient
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

func (c *clientImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	var shardID int32

	// For Chasm components we need to route the shard based on business ID. Note that shardIDFromWorkflowID simply
	// calculates the hash from the ID so it works for both workflowID and businessID.
	if len(request.GetComponentRef()) == 0 {
		shardID = c.shardIDFromWorkflowID(request.GetNamespaceId(), request.GetWorkflowExecution().GetWorkflowId())
	} else {
		componentRef, err := c.tokenSerializer.DeserializeChasmComponentRef(request.GetComponentRef())
		if err != nil {
			return nil, err
		}

		shardID = c.shardIDFromWorkflowID(componentRef.GetNamespaceId(), componentRef.GetBusinessId())
	}

	var response *historyservice.RecordActivityTaskStartedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskStarted(ctx, request, opts...)
		return err
	}
	if err := c.executeWithRedirect(ctx, shardID, op); err != nil {
		return nil, err
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
		return serviceerror.NewInvalidArgumentf("Invalid ShardID: %d", shardID)
	}
	return nil
}

func (c *clientImpl) executeWithRedirect(
	ctx context.Context,
	shardID int32,
	op ClientOperation[historyservice.HistoryServiceClient],
) error {
	return c.redirector.Execute(ctx, shardID, op)
}
