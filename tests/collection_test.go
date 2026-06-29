package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/collection"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const collectionTestTimeout = 10 * time.Second * debug.TimeoutMultiplier

type CollectionTestSuite struct {
	testcore.FunctionalTestBase

	chasmContext context.Context
	handler      collectionpb.CollectionServiceServer
}

func TestCollectionTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CollectionTestSuite))
}

func (s *CollectionTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                            true,
			dynamicconfig.DeleteNamespaceUseChasmDeleteExecution.Key(): true,
			// Needed because completion flows through the standalone-activity frontend handler.
			activity.Enabled.Key(): true,
		}),
	)

	chasmEngine, err := s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.handler = collection.NewHandler(s.Logger)
}

func (s *CollectionTestSuite) TestCollectionLifecycle() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, collectionTestTimeout)
	defer cancel()

	collectionID := tv.Any().String()

	startResp, err := s.handler.StartCollectionExecution(ctx, &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	})
	s.NoError(err)
	s.NotEmpty(startResp.GetRunId())

	descResp, err := s.handler.DescribeCollectionExecution(ctx, &collectionpb.DescribeCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)
	s.False(descResp.GetState().GetClosed())

	_, err = s.handler.CloseCollectionExecution(ctx, &collectionpb.CloseCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)

	descResp, err = s.handler.DescribeCollectionExecution(ctx, &collectionpb.DescribeCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)
	s.True(descResp.GetState().GetClosed())
}

func (s *CollectionTestSuite) TestCollectionActivityArm() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, collectionTestTimeout)
	defer cancel()

	collectionID := tv.Any().String()
	itemID := "item-0"
	taskQueue := tv.TaskQueue().GetName()
	input := payload.EncodeString("hello")

	_, err := s.handler.StartCollectionExecution(ctx, &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	})
	s.NoError(err)

	_, err = s.handler.AddCollectionItems(ctx, &collectionpb.AddCollectionItemsRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		Items: []*collectionpb.CollectionItem{{
			ItemId: itemID,
			Activity: &collectionpb.ActivityOperation{
				ActivityType:        tv.ActivityType(),
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				Input:               &commonpb.Payloads{Payloads: []*commonpb.Payload{input}},
				StartToCloseTimeout: durationpb.New(time.Minute),
			},
		}},
	})
	s.NoError(err)

	// Act as the worker: poll the dispatched activity task and complete it.
	pollResp, err := s.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotNil(pollResp.GetTaskToken())
	s.Equal(payload.ToString(input), payload.ToString(pollResp.GetInput().GetPayloads()[0]))

	_, err = s.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  tv.WorkerIdentity(),
		TaskToken: pollResp.GetTaskToken(),
		Result:    &commonpb.Payloads{Payloads: []*commonpb.Payload{payload.EncodeString("world")}},
	})
	s.NoError(err)

	completed := activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED.String()
	s.Eventually(func() bool {
		descCtx, descCancel := context.WithTimeout(s.chasmContext, 5*time.Second)
		defer descCancel()
		descResp, derr := s.handler.DescribeCollectionExecution(descCtx, &collectionpb.DescribeCollectionExecutionRequest{
			NamespaceId:  s.NamespaceID().String(),
			CollectionId: collectionID,
		})
		s.NoError(derr)
		items := descResp.GetItems()
		return len(items) == 1 && items[0].GetStatus() == completed
	}, collectionTestTimeout, 100*time.Millisecond)
}

func (s *CollectionTestSuite) TestCollectionStart_AlreadyRunning() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, collectionTestTimeout)
	defer cancel()

	collectionID := tv.Any().String()
	req := &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	}

	_, err := s.handler.StartCollectionExecution(ctx, req)
	s.NoError(err)

	_, err = s.handler.StartCollectionExecution(ctx, &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	})
	s.Error(err)
}
