package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	testNamespaceID   = "namespace-id"
	testTaskQueueName = "task-queue"
)

type testClientCache struct {
	client      matchingservice.MatchingServiceClient
	lookupKey   string
	lookupIndex int
	lookupCalls int
}

func (c *testClientCache) Lookup(key string, index int) (string, error) {
	c.lookupKey = key
	c.lookupIndex = index
	c.lookupCalls++
	return "matching-address", nil
}

func (c *testClientCache) GetClientForKey(key string, index int) (any, error) {
	_, err := c.Lookup(key, index)
	return c.client, err
}

func (c *testClientCache) GetClientForClientKey(string) (any, error) {
	return c.client, nil
}

func (c *testClientCache) GetAllClients() ([]any, error) {
	return []any{c.client}, nil
}

func (*testClientCache) Evict(string) {}

func (*testClientCache) EvictAll() {}

type testLoadBalancer struct {
	writePartition *tqid.NormalPartition
	writeEstimate  int
	writeCalls     int
	readToken      *pollToken
	readCalls      int
}

func (l *testLoadBalancer) PickWritePartition(*tqid.TaskQueue, PartitionCounts) (*tqid.NormalPartition, int) {
	l.writeCalls++
	return l.writePartition, l.writeEstimate
}

func (l *testLoadBalancer) PickReadPartition(*tqid.TaskQueue, PartitionCounts) *pollToken {
	l.readCalls++
	return l.readToken
}

func newTestClient(cache *testClientCache, loadBalancer LoadBalancer) *clientImpl {
	return &clientImpl{
		timeout:         time.Second,
		longPollTimeout: time.Second,
		clients:         cache,
		logger:          log.NewNoopLogger(),
		loadBalancer:    loadBalancer,
		spreadRouting: func() dynamicconfig.GradualChange[int] {
			return dynamicconfig.StaticGradualChange(0)
		},
	}
}

func testTaskQueue(t *testing.T, taskType enumspb.TaskQueueType) *tqid.TaskQueue {
	t.Helper()
	family, err := tqid.NewTaskQueueFamily(testNamespaceID, testTaskQueueName)
	require.NoError(t, err)
	return family.TaskQueue(taskType)
}

func requireRoutedTo(t *testing.T, cache *testClientCache, partition *tqid.NormalPartition) {
	t.Helper()
	wantKey, wantIndex := partition.RoutingKey(0)
	require.Equal(t, wantKey, cache.lookupKey)
	require.Equal(t, wantIndex, cache.lookupIndex)
	require.Equal(t, 1, cache.lookupCalls)
}

func TestDoGrantEagerDispatch_LoadBalancedRoot(t *testing.T) {
	controller := gomock.NewController(t)
	serviceClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	taskQueue := testTaskQueue(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	selectedPartition := taskQueue.NormalPartition(3)
	cache := &testClientCache{client: serviceClient}
	loadBalancer := &testLoadBalancer{writePartition: selectedPartition}
	client := newTestClient(cache, loadBalancer)
	request := &matchingservice.GrantEagerDispatchRequest{
		NamespaceId: testNamespaceID,
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     testTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		},
		Items: []*matchingservice.GrantEagerDispatchRequest_Item{{Count: 1}},
	}
	response := &matchingservice.GrantEagerDispatchResponse{}

	serviceClient.EXPECT().GrantEagerDispatch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, outgoing *matchingservice.GrantEagerDispatchRequest, _ ...grpc.CallOption) (*matchingservice.GrantEagerDispatchResponse, error) {
			require.Equal(t, int32(3), outgoing.GetTaskQueuePartition().GetNormalPartitionId())
			require.Same(t, request.GetItems()[0], outgoing.GetItems()[0])
			return response, nil
		},
	)

	actual, err := client.doGrantEagerDispatch(
		context.Background(),
		taskQueue.RootPartition(),
		true,
		PartitionCounts{Read: 4, Write: 4},
		request,
		nil,
	)
	require.NoError(t, err)
	require.Same(t, response, actual)
	require.Nil(t, request.GetTaskQueuePartition().GetPartitionId(), "load balancing must not mutate the caller's request")
	require.Equal(t, 1, loadBalancer.writeCalls)
	requireRoutedTo(t, cache, selectedPartition)
}

func TestDoGrantEagerDispatch_DirectPartition(t *testing.T) {
	controller := gomock.NewController(t)
	serviceClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	taskQueue := testTaskQueue(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	directPartition := taskQueue.NormalPartition(2)
	cache := &testClientCache{client: serviceClient}
	loadBalancer := &testLoadBalancer{}
	client := newTestClient(cache, loadBalancer)
	request := &matchingservice.GrantEagerDispatchRequest{
		NamespaceId: testNamespaceID,
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     testTaskQueueName,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			PartitionId: &taskqueuespb.TaskQueuePartition_NormalPartitionId{
				NormalPartitionId: 2,
			},
		},
	}
	response := &matchingservice.GrantEagerDispatchResponse{}

	serviceClient.EXPECT().GrantEagerDispatch(gomock.Any(), request).Return(response, nil)

	actual, err := client.doGrantEagerDispatch(
		context.Background(),
		directPartition,
		false,
		PartitionCounts{Read: 4, Write: 4},
		request,
		nil,
	)
	require.NoError(t, err)
	require.Same(t, response, actual)
	require.Zero(t, loadBalancer.writeCalls)
	requireRoutedTo(t, cache, directPartition)
}

func TestDoAddWorkflowTask_LoadBalancedRoot(t *testing.T) {
	controller := gomock.NewController(t)
	serviceClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	taskQueue := testTaskQueue(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	selectedPartition := taskQueue.NormalPartition(3)
	cache := &testClientCache{client: serviceClient}
	loadBalancer := &testLoadBalancer{writePartition: selectedPartition, writeEstimate: 17}
	client := newTestClient(cache, loadBalancer)
	request := &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: testNamespaceID,
		TaskQueue:   &taskqueuepb.TaskQueue{Name: testTaskQueueName},
	}
	response := &matchingservice.AddWorkflowTaskResponse{}

	serviceClient.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, outgoing *matchingservice.AddWorkflowTaskRequest, _ ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {
			require.Equal(t, selectedPartition.RpcName(), outgoing.GetTaskQueue().GetName())
			outgoingMetadata, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{"17"}, outgoingMetadata.Get(estimatedTasksAllPartitionsHeaderName))
			return response, nil
		},
	)

	actual, err := client.doAddWorkflowTask(
		context.Background(),
		taskQueue.RootPartition(),
		true,
		PartitionCounts{Read: 4, Write: 4},
		request,
		nil,
	)
	require.NoError(t, err)
	require.Same(t, response, actual)
	require.Equal(t, testTaskQueueName, request.GetTaskQueue().GetName(), "load balancing must not mutate the caller's request")
	require.Equal(t, 1, loadBalancer.writeCalls)
	requireRoutedTo(t, cache, selectedPartition)
}

func TestDoPollWorkflowTaskQueue_LoadBalancedRoot(t *testing.T) {
	controller := gomock.NewController(t)
	serviceClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	taskQueue := testTaskQueue(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	selectedPartition := taskQueue.NormalPartition(2)
	partitionBalancer := &tqLoadBalancer{
		taskQueue:    taskQueue,
		pollerCounts: []int{0, 0, 1, 0},
	}
	cache := &testClientCache{client: serviceClient}
	loadBalancer := &testLoadBalancer{readToken: &pollToken{
		TQPartition: selectedPartition,
		balancer:    partitionBalancer,
	}}
	client := newTestClient(cache, loadBalancer)
	request := &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: testNamespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: testTaskQueueName},
			Identity:  "worker",
		},
	}
	response := &matchingservice.PollWorkflowTaskQueueResponse{}

	serviceClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, outgoing *matchingservice.PollWorkflowTaskQueueRequest, _ ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
			require.Equal(t, selectedPartition.RpcName(), outgoing.GetPollRequest().GetTaskQueue().GetName())
			require.Equal(t, "worker", outgoing.GetPollRequest().GetIdentity())
			return response, nil
		},
	)

	actual, err := client.doPollWorkflowTaskQueue(
		context.Background(),
		taskQueue.RootPartition(),
		true,
		PartitionCounts{Read: 4, Write: 4},
		request,
		nil,
	)
	require.NoError(t, err)
	require.Same(t, response, actual)
	require.Equal(t, testTaskQueueName, request.GetPollRequest().GetTaskQueue().GetName(), "load balancing must not mutate the caller's request")
	require.Equal(t, 1, loadBalancer.readCalls)
	require.Zero(t, partitionBalancer.pollerCounts[2], "the poll token must be released after the RPC")
	requireRoutedTo(t, cache, selectedPartition)
}

func TestGrantEagerDispatch_RejectsUnsupportedPartition(t *testing.T) {
	tests := []struct {
		name      string
		partition func() *taskqueuespb.TaskQueuePartition
	}{
		{
			name: "sticky",
			partition: func() *taskqueuespb.TaskQueuePartition {
				return &taskqueuespb.TaskQueuePartition{
					TaskQueue:     testTaskQueueName,
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
					PartitionId:   &taskqueuespb.TaskQueuePartition_StickyName{StickyName: "sticky"},
				}
			},
		},
		{
			name: "worker commands",
			partition: func() *taskqueuespb.TaskQueuePartition {
				return &taskqueuespb.TaskQueuePartition{
					TaskQueue:     testTaskQueueName,
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
					PartitionId: &taskqueuespb.TaskQueuePartition_WorkerCommands{
						WorkerCommands: &taskqueuespb.WorkerCommandsPartitionId{},
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &clientImpl{}
			_, err := client.GrantEagerDispatch(context.Background(), &matchingservice.GrantEagerDispatchRequest{
				NamespaceId:        testNamespaceID,
				TaskQueuePartition: test.partition(),
			})

			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
		})
	}
}
