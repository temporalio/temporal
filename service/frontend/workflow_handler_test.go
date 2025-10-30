package frontend

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	numHistoryShards = 10
	esIndexName      = ""

	testWorkflowID            = "test-workflow-id"
	testRunID                 = "test-run-id"
	testHistoryArchivalURI    = "testScheme://history/URI"
	testVisibilityArchivalURI = "testScheme://visibility/URI"
)

type (
	WorkflowHandlerSuite struct {
		suite.Suite
		protorequire.ProtoAssertions
		*require.Assertions

		controller                         *gomock.Controller
		mockResource                       *resourcetest.Test
		mockNamespaceCache                 *namespace.MockRegistry
		mockHistoryClient                  *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata                *cluster.MockMetadata
		mockSearchAttributesProvider       *searchattribute.MockProvider
		mockSearchAttributesMapperProvider *searchattribute.MockMapperProvider
		mockMatchingClient                 *matchingservicemock.MockMatchingServiceClient

		mockProducer           *persistence.MockNamespaceReplicationQueue
		mockMetadataMgr        *persistence.MockMetadataManager
		mockExecutionManager   *persistence.MockExecutionManager
		mockVisibilityMgr      *manager.MockVisibilityManager
		mockArchivalMetadata   archiver.MetadataMock
		mockArchiverProvider   *provider.MockArchiverProvider
		mockHistoryArchiver    *archiver.MockHistoryArchiver
		mockVisibilityArchiver *archiver.MockVisibilityArchiver

		tokenSerializer *tasktoken.Serializer

		testNamespace   namespace.Name
		testNamespaceID namespace.ID
	}
)

var testNamespaceID = primitives.MustValidateUUID("deadbeef-c001-4567-890a-bcdef0123456")

func TestWorkflowHandlerSuite(t *testing.T) {
	s := new(WorkflowHandlerSuite)
	suite.Run(t, s)
}

func (s *WorkflowHandlerSuite) SetupSuite() {
}

func (s *WorkflowHandlerSuite) TearDownSuite() {
}

func (s *WorkflowHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.testNamespace = "test-namespace"
	s.testNamespaceID = "e4f90ec0-1313-45be-9877-8aa41f72a45a"

	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockSearchAttributesProvider = s.mockResource.SearchAttributesProvider
	s.mockSearchAttributesMapperProvider = s.mockResource.SearchAttributesMapperProvider
	s.mockMetadataMgr = s.mockResource.MetadataMgr
	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockVisibilityMgr = s.mockResource.VisibilityManager
	s.mockArchivalMetadata = s.mockResource.ArchivalMetadata
	s.mockArchiverProvider = s.mockResource.ArchiverProvider
	s.mockMatchingClient = s.mockResource.MatchingClient

	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockHistoryArchiver = archiver.NewMockHistoryArchiver(s.controller)
	s.mockVisibilityArchiver = archiver.NewMockVisibilityArchiver(s.controller)

	s.tokenSerializer = tasktoken.NewSerializer()

	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName}).AnyTimes()
	s.mockExecutionManager.EXPECT().GetName().Return("mock-execution-manager").AnyTimes()
}

func (s *WorkflowHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *WorkflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
	healthInterceptor := interceptor.NewHealthInterceptor()
	healthInterceptor.SetHealthy(true)
	return NewWorkflowHandler(
		config,
		s.mockProducer,
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetLogger(),
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager().GetName(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetMatchingClient(),
		nil,
		nil,
		s.mockResource.GetArchiverProvider(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesMapperProvider(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		health.NewServer(),
		clock.NewRealTimeSource(),
		s.mockResource.GetMembershipMonitor(),
		healthInterceptor,
		scheduler.NewSpecBuilder(),
		true,
	)
}

func (s *WorkflowHandlerSuite) TestDisableListVisibilityByFilter() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	config := s.newConfig()
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByNamespace(true)

	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetReadStoreName(testNamespace).Return("").AnyTimes()

	// test list open by wid
	listRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace: testNamespace.String(),
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: "wid",
		}},
	}
	_, err := wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	s.Error(err)
	s.Equal(errListNotAllowed, err)

	// test list open by workflow type
	listRequest.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	s.Error(err)
	s.Equal(errListNotAllowed, err)

	// test list close by wid
	listRequest2 := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace: testNamespace.String(),
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: "wid",
		}},
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errListNotAllowed, err)

	// test list close by workflow type
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errListNotAllowed, err)

	// test list close by workflow status
	failedStatus := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_StatusFilter{StatusFilter: &filterpb.StatusFilter{Status: failedStatus}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errListNotAllowed, err)
}

func (s *WorkflowHandlerSuite) TestPollForTask_Failed_ContextTimeoutTooShort() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	bgCtx := context.Background()
	_, err := wh.PollWorkflowTaskQueue(bgCtx, &workflowservice.PollWorkflowTaskQueueRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutNotSet, err)

	_, err = wh.PollActivityTaskQueue(bgCtx, &workflowservice.PollActivityTaskQueueRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutNotSet, err)

	shortCtx, cancel := context.WithTimeout(bgCtx, common.MinLongPollTimeout-time.Millisecond)
	defer cancel()

	_, err = wh.PollWorkflowTaskQueue(shortCtx, &workflowservice.PollWorkflowTaskQueueRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutTooShort, err)

	_, err = wh.PollActivityTaskQueue(shortCtx, &workflowservice.PollActivityTaskQueueRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutTooShort, err)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_StartRequestNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	_, err := wh.StartWorkflowExecution(context.Background(), nil)
	s.Error(err)
	s.Equal(errRequestNotSet, err)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_NamespaceNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(namespace.EmptyName).Return(namespace.EmptyID, serviceerror.NewNamespaceNotFound("missing-namespace")).AnyTimes()
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(namespace.EmptyName).Return(nil, nil)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		// Namespace: "forget to specify",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowIdNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowIDNotSet, err)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowTypeNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowTypeNotSet, err)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_TaskQueueNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "",
		},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(serviceerror.NewInvalidArgument("missing task queue name"), err)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidExecutionTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(time.Duration(-1) * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.ErrorContains(err, errInvalidWorkflowExecutionTimeoutSeconds.Error())
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidRunTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.ErrorContains(err, errInvalidWorkflowRunTimeoutSeconds.Error())
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_EnsureNonNilRetryPolicyInitialized() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(time.Duration(-1) * time.Second),
		RetryPolicy:              &commonpb.RetryPolicy{},
		RequestId:                uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(&commonpb.RetryPolicy{
		BackoffCoefficient: 2.0,
		InitialInterval:    durationpb.New(time.Second),
		MaximumInterval:    durationpb.New(100 * time.Second),
	}, startWorkflowExecutionRequest.RetryPolicy)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_EnsureNilRetryPolicyNotInitialized() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(time.Duration(-1) * time.Second),
		RequestId:                uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Nil(startWorkflowExecutionRequest.RetryPolicy)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidTaskTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId: uuid.NewString(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.ErrorContains(err, errInvalidWorkflowTaskTimeoutSeconds.Error())
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_CronAndStartDelaySet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId:          uuid.NewString(),
		CronSchedule:       "dummy-cron-schedule",
		WorkflowStartDelay: durationpb.New(10 * time.Second),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.ErrorIs(err, errCronAndStartDelaySet)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidStartDelay() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId:          uuid.NewString(),
		WorkflowStartDelay: durationpb.New(-10 * time.Second),
	}

	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.ErrorContains(err, errInvalidWorkflowStartDelaySeconds.Error())
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_InvalidWorkflowIdReusePolicy_TerminateIfRunning() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               testWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	resp, err := wh.StartWorkflowExecution(context.Background(), req)

	s.Nil(resp)
	s.Equal(err, serviceerror.NewInvalidArgument(
		"Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy"))
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_InvalidWorkflowIdReusePolicy_RejectDuplicate() {
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               testWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	}

	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	resp, err := wh.StartWorkflowExecution(context.Background(), req)
	s.Nil(resp)
	s.Equal(err, serviceerror.NewInvalidArgument(
		"Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE cannot be used together with WorkflowIdConflictPolicy WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING"))
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_DefaultWorkflowIdDuplicationPolicies() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).Return(nil, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespace.NewID(), nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), mock.MatchedBy(
		func(request *historyservice.StartWorkflowExecutionRequest) bool {
			return request.StartRequest.WorkflowIdReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE &&
				request.StartRequest.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
		},
	)).Return(&historyservice.StartWorkflowExecutionResponse{Started: true}, nil)

	wh := s.getWorkflowHandler(s.newConfig())
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:   testWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		// both policies are not specified
	}

	resp, err := wh.StartWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.True(resp.Started)
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		RequestId: uuid.NewString(),
	}

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "dont-care",
					WorkflowId: strings.Repeat("X", 4000),
					RunId:      uuid.NewString(),
				},
			},
		},
	}

	_, err := wh.StartWorkflowExecution(context.Background(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "link exceeds allowed size of 4000")

	req.Links = []*commonpb.Link{}
	for i := 0; i < 11; i++ {
		req.Links = append(req.Links, &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "dont-care",
					WorkflowId: "dont-care",
					RunId:      uuid.NewString(),
				},
			},
		})
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "cannot attach more than 10 links per request, got 11")

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{},
			},
		},
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "workflow event link must not have an empty namespace field")

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace: "present",
				},
			},
		},
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "workflow event link must not have an empty workflow ID field")

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "present",
					WorkflowId: "present",
				},
			},
		},
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "workflow event link must not have an empty run ID field")

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "present",
					WorkflowId: "present",
					RunId:      uuid.NewString(),
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId: 3,
						},
					},
				},
			},
		},
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "workflow event link ref cannot have an unspecified event type and a non-zero event ID")

	req.Links = []*commonpb.Link{
		{
			Variant: &commonpb.Link_BatchJob_{
				BatchJob: &commonpb.Link_BatchJob{},
			},
		},
	}

	_, err = wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "batch job link must not have an empty job ID")
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidCallbackLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		RequestId: uuid.NewString(),
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Internal_{
					Internal: &commonpb.Callback_Internal{},
				},
				Links: []*commonpb.Link{
					{
						Variant: &commonpb.Link_WorkflowEvent_{
							WorkflowEvent: &commonpb.Link_WorkflowEvent{},
						},
					},
				},
			},
		},
	}

	var invalidArgument *serviceerror.InvalidArgument
	_, err := wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "workflow event link must not have an empty namespace field")
}

func (s *WorkflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidAggregatedLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	config.CallbackEndpointConfigs = dc.GetTypedPropertyFnFilteredByNamespace(callbacks.AddressMatchRules{
		Rules: []callbacks.AddressMatchRule{
			{
				Regexp:        regexp.MustCompile(`.*`),
				AllowInsecure: true,
			},
		},
	})
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		RequestId: uuid.NewString(),
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://localhost/test",
					},
				},
				Links: []*commonpb.Link{
					{
						Variant: &commonpb.Link_WorkflowEvent_{
							WorkflowEvent: &commonpb.Link_WorkflowEvent{},
						},
					},
					{
						Variant: &commonpb.Link_WorkflowEvent_{
							WorkflowEvent: &commonpb.Link_WorkflowEvent{
								Namespace:  "dont-care",
								WorkflowId: "dont-care",
								RunId:      "run-id-0",
							},
						},
					},
				},
			},
		},
	}

	// add 10 links and one of them is duplicated in the callback
	req.Links = []*commonpb.Link{}
	for i := 0; i < 10; i++ {
		req.Links = append(req.Links, &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "dont-care",
					WorkflowId: "dont-care",
					RunId:      fmt.Sprintf("run-id-%d", i),
				},
			},
		})
	}

	var invalidArgument *serviceerror.InvalidArgument
	_, err := wh.StartWorkflowExecution(context.Background(), req)
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "cannot attach more than 10 links per request, got 11")
}

func (s *WorkflowHandlerSuite) TestSignalWithStartWorkflowExecution_InvalidWorkflowIdConflictPolicy() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:               testWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		SignalName:               "SIGNAL",
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	resp, err := wh.SignalWithStartWorkflowExecution(context.Background(), req)

	s.Nil(resp)
	s.Equal(err, serviceerror.NewInvalidArgument(
		"Invalid WorkflowIDConflictPolicy: WORKFLOW_ID_CONFLICT_POLICY_FAIL is not supported for this operation."))
}

func (s *WorkflowHandlerSuite) TestSignalWithStartWorkflowExecution_InvalidWorkflowIdReusePolicy_TerminateIfRunning() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:               testWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		SignalName:               "SIGNAL",
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	resp, err := wh.SignalWithStartWorkflowExecution(context.Background(), req)

	s.Nil(resp)
	s.Equal(err, serviceerror.NewInvalidArgument(
		"Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy"))
}

func (s *WorkflowHandlerSuite) TestSignalWithStartWorkflowExecution_DefaultWorkflowIdDuplicationPolicies() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).Return(nil, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespace.NewID(), nil)
	s.mockHistoryClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), mock.MatchedBy(
		func(request *historyservice.SignalWithStartWorkflowExecutionRequest) bool {
			return request.SignalWithStartRequest.WorkflowIdReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE &&
				request.SignalWithStartRequest.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
		},
	)).Return(&historyservice.SignalWithStartWorkflowExecutionResponse{Started: true}, nil)

	wh := s.getWorkflowHandler(s.newConfig())
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   testWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "WORKFLOW"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "TASK_QUEUE", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		SignalName:   "SIGNAL",
		// both policies are not specified
	}

	resp, err := wh.SignalWithStartWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.True(resp.Started)
}

func (s *WorkflowHandlerSuite) TestSignalWithStartWorkflowExecution_Failed_InvalidLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:  "test-namespace",
		WorkflowId: "workflow-id",
		WorkflowType: &commonpb.WorkflowType{
			Name: "workflow-type",
		},
		SignalName: "dont-care",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "task-queue",
		},
		RequestId: uuid.NewString(),
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: strings.Repeat("X", 4000),
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}

	_, err := wh.SignalWithStartWorkflowExecution(context.Background(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "link exceeds allowed size of 4000")
}

func (s *WorkflowHandlerSuite) TestSignalWorkflowExecution_Failed_InvalidLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
		},
		SignalName: "dont-care",
		Identity:   "test",
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: strings.Repeat("X", 4000),
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}

	_, err := wh.SignalWorkflowExecution(context.Background(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "link exceeds allowed size of 4000")
}

func (s *WorkflowHandlerSuite) TestTerminateWorkflowExecution_Failed_InvalidLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
		},
		Reason: "dont-care",
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: strings.Repeat("X", 4000),
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}

	_, err := wh.TerminateWorkflowExecution(context.Background(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "link exceeds allowed size of 4000")
}

func (s *WorkflowHandlerSuite) TestTerminateWorkflowExecution_Succeed_WithDefaultReasonAndIdentity() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(testNamespace).Return(namespaceID, nil)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.TerminateWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal("workflow-id", request.TerminateRequest.WorkflowExecution.GetWorkflowId())
			// Verify that default values are set when reason and identity are empty
			s.Equal(defaultUserTerminateReason, request.TerminateRequest.GetReason())
			s.Equal(defaultUserTerminateIdentity, request.TerminateRequest.GetIdentity())
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	)

	req := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
		},
	}

	_, err := wh.TerminateWorkflowExecution(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestTerminateWorkflowExecution_Succeed_WithCustomReasonAndIdentity() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(testNamespace).Return(namespaceID, nil)
	reason := "reason"
	identity := "identity"
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.TerminateWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal("workflow-id", request.TerminateRequest.WorkflowExecution.GetWorkflowId())
			// Verify that custom values are preserved and not overwritten by defaults
			s.Equal(reason, request.TerminateRequest.GetReason())
			s.Equal(identity, request.TerminateRequest.GetIdentity())
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	)

	req := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
		},
		Reason:   reason,
		Identity: identity,
	}

	_, err := wh.TerminateWorkflowExecution(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestRequestCancelWorkflowExecution_Failed_InvalidLinks() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).AnyTimes().Return(nil, nil)
	config := s.newConfig()
	config.MaxLinksPerRequest = dc.GetIntPropertyFnFilteredByNamespace(10)
	wh := s.getWorkflowHandler(config)

	req := &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
		},
		Reason: "dont-care",
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: strings.Repeat("X", 4000),
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}

	_, err := wh.RequestCancelWorkflowExecution(context.Background(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "link exceeds allowed size of 4000")
}

func (s *WorkflowHandlerSuite) TestRegisterNamespace_Failure_InvalidArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(errors.New("invalid URI"))
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(
		enumspb.ARCHIVAL_STATE_ENABLED,
		testHistoryArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
		testVisibilityArchivalURI,
	)
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestRegisterNamespace_Success_EnabledWithNoArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", testHistoryArchivalURI))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", testVisibilityArchivalURI))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_ENABLED, "", enumspb.ARCHIVAL_STATE_ENABLED, "")
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestRegisterNamespace_Success_EnabledWithArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "invalidURI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "invalidURI"))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(
		enumspb.ARCHIVAL_STATE_ENABLED,
		testHistoryArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
		testVisibilityArchivalURI,
	)
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestRegisterNamespace_Success_ClusterNotConfiguredForArchival() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(
		enumspb.ARCHIVAL_STATE_ENABLED,
		testVisibilityArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
		"invalidURI",
	)
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestRegisterNamespace_Success_NotEnabled() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestDeprecateNamespace_Success() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_DEPRECATED, request.Namespace.Info.State)
			return nil
		},
	)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")

	resp, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
	s.NotNil(resp)
	respDeprecate, errDeprecate := wh.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DEPRECATED,
		},
	})
	s.NoError(errDeprecate)
	s.NotNil(respDeprecate)
}

func (s *WorkflowHandlerSuite) TestDeprecateNamespace_Error() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_DEPRECATED, request.Namespace.Info.State)
			return serviceerror.NewInternal("db is down")
		},
	)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")

	resp, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
	s.NotNil(resp)
	respDeprecate, errDeprecate := wh.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DEPRECATED,
		},
	})
	s.Error(errDeprecate)
	s.Nil(respDeprecate)
}

func (s *WorkflowHandlerSuite) TestDeleteNamespace_Success() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_DELETED, request.Namespace.Info.State)
			return nil
		},
	)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")

	resp, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
	s.NotNil(resp)
	respDelete, errDelete := wh.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})
	s.NoError(errDelete)
	s.NotNil(respDelete)
}

func (s *WorkflowHandlerSuite) TestDeleteNamespace_Error() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI")).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_DELETED, request.Namespace.Info.State)
			return serviceerror.NewInternal("db is down")
		},
	)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")

	resp, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
	s.NotNil(resp)
	respDelete, errDelete := wh.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})
	s.Error(errDelete)
	s.Nil(respDelete)
}

func (s *WorkflowHandlerSuite) TestDescribeNamespace_Success_ArchivalDisabled() {
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := &workflowservice.DescribeNamespaceRequest{
		Namespace: "test-namespace",
	}
	result, err := wh.DescribeNamespace(context.Background(), req)

	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetHistoryArchivalState())
	s.Equal("", result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetVisibilityArchivalState())
	s.Equal("", result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestDescribeNamespace_Success_ArchivalEnabled() {
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := &workflowservice.DescribeNamespaceRequest{
		Namespace: "test-namespace",
	}
	result, err := wh.DescribeNamespace(context.Background(), req)

	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetHistoryArchivalState())
	s.Equal(testHistoryArchivalURI, result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetVisibilityArchivalState())
	s.Equal(testVisibilityArchivalURI, result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Failure_UpdateExistingArchivalURI() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"",
		enumspb.ARCHIVAL_STATE_UNSPECIFIED,
		"updated visibility URI",
		enumspb.ARCHIVAL_STATE_UNSPECIFIED,
	)
	_, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Failure_InvalidArchivalURI() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(errors.New("invalid URI"))
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"testScheme://invalid/updated/history/URI",
		enumspb.ARCHIVAL_STATE_ENABLED,
		"",
		enumspb.ARCHIVAL_STATE_UNSPECIFIED,
	)
	_, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToArchivalDisabledWithoutSettingURI() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"",
		enumspb.ARCHIVAL_STATE_DISABLED,
		"",
		enumspb.ARCHIVAL_STATE_DISABLED,
	)
	result, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetHistoryArchivalState())
	s.Equal(testHistoryArchivalURI, result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetVisibilityArchivalState())
	s.Equal(testVisibilityArchivalURI, result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Success_ClusterNotConfiguredForArchival() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random history URI"},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random visibility URI"},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest("", enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED)
	result, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetHistoryArchivalState())
	s.Equal("some random history URI", result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetVisibilityArchivalState())
	s.Equal("some random visibility URI", result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToArchivalDisabledWithSettingBucket() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enumspb.ARCHIVAL_STATE_DISABLED,
		testVisibilityArchivalURI,
		enumspb.ARCHIVAL_STATE_DISABLED,
	)
	result, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetHistoryArchivalState())
	s.Equal(testHistoryArchivalURI, result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, result.Config.GetVisibilityArchivalState())
	s.Equal(testVisibilityArchivalURI, result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToEnabled() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
		testVisibilityArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
	)
	result, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetHistoryArchivalState())
	s.Equal(testHistoryArchivalURI, result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetVisibilityArchivalState())
	s.Equal(testVisibilityArchivalURI, result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestUpdateNamespace_Success_ArchivalNeverEnabledToEnabled() {
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalConfigState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
		testVisibilityArchivalURI,
		enumspb.ARCHIVAL_STATE_ENABLED,
	)
	result, err := wh.UpdateNamespace(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Config)
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetHistoryArchivalState())
	s.Equal(testHistoryArchivalURI, result.Config.GetHistoryArchivalUri())
	s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, result.Config.GetVisibilityArchivalState())
	s.Equal(testVisibilityArchivalURI, result.Config.GetVisibilityArchivalUri())
}

func (s *WorkflowHandlerSuite) TestHistoryArchived() {
	wh := s.getWorkflowHandler(s.newConfig())

	getHistoryRequest := &workflowservice.GetWorkflowExecutionHistoryRequest{}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-namespace"))

	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-namespace"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, nil)
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-namespace"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("got archival indication error"))
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.True(wh.historyArchived(context.Background(), getHistoryRequest, "test-namespace"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, errors.New("got non-archival indication error"))
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-namespace"))
}

func (s *WorkflowHandlerSuite) TestGetArchivedHistory_Failure_NamespaceCacheEntryError() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, errors.New("error getting namespace"))

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestGetArchivedHistory_Failure_ArchivalURIEmpty() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:      "",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidURI() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      "uri without scheme",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      testHistoryArchivalURI,
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()

	nextPageToken := []byte{'1', '2', '3'}
	historyBatch1 := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{EventId: 1},
			{EventId: 2},
		},
	}
	historyBatch2 := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{EventId: 3},
			{EventId: 4},
			{EventId: 5},
		},
	}
	history := &historypb.History{}
	history.Events = append(history.Events, historyBatch1.Events...)
	history.Events = append(history.Events, historyBatch2.Events...)
	s.mockHistoryArchiver.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&archiver.GetHistoryResponse{
		NextPageToken:  nextPageToken,
		HistoryBatches: []*historypb.History{historyBatch1, historyBatch2},
	}, nil)
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any()).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	s.Equal(history, resp.History)
	s.Equal(nextPageToken, resp.NextPageToken)
	s.True(resp.GetArchived())
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidRequest() {
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), &workflowservice.ListArchivedWorkflowExecutionsRequest{})
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Failure_ClusterNotConfiguredForArchival() {
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Failure_NamespaceCacheEntryError() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(nil, errors.New("error getting namespace"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Failure_NamespaceNotConfiguredForArchival() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(namespace.NewLocalNamespaceForTest(
		nil,
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
		},
		"",
	), nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidURI() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "uri without scheme",
		},
		"",
	), nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestListArchivedVisibility_Success() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   testVisibilityArchivalURI,
		},
		"",
	), nil).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)
	s.mockVisibilityArchiver.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&archiver.QueryVisibilityResponse{}, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any()).Return(s.mockVisibilityArchiver, nil)
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes("", false)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.NotNil(resp)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestGetSearchAttributes() {
	wh := s.getWorkflowHandler(s.newConfig())

	ctx := context.Background()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	resp, err := wh.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *WorkflowHandlerSuite) TestDescribeWorkflowExecution_RunningStatus() {
	wh := s.getWorkflowHandler(s.newConfig())
	now := timestamppb.New(time.Now())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(
		s.testNamespaceID,
		nil,
	).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				StartTime:        now,
				CloseTime:        now,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				ExecutionTime:    now,
				Memo:             nil,
				SearchAttributes: nil,
			},
		},
		nil,
	)

	request := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.testNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
		},
	}
	_, err := wh.DescribeWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestDescribeWorkflowExecution_CompletedStatus() {
	wh := s.getWorkflowHandler(s.newConfig())
	now := timestamppb.New(time.Now())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(
		s.testNamespaceID,
		nil,
	).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				StartTime:        now,
				CloseTime:        now,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				ExecutionTime:    now,
				Memo:             nil,
				SearchAttributes: nil,
			},
		},
		nil,
	)

	request := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.testNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
		},
	}
	_, err := wh.DescribeWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestListWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.testNamespace).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetReadStoreName(s.testNamespace).Return(elasticsearch.PersistenceName).AnyTimes()

	query := "WorkflowId = 'wid'"
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
		PageSize:  int32(config.VisibilityMaxPageSize(s.testNamespace.String())),
		Query:     query,
	}
	ctx := context.Background()

	// page size <= 0 => max page size = 1000
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      config.VisibilityMaxPageSize(s.testNamespace.String()),
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	_, err := wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	// page size > 1000 => max page size = 1000
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      config.VisibilityMaxPageSize(s.testNamespace.String()),
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	listRequest.PageSize = int32(config.VisibilityMaxPageSize(s.testNamespace.String())) + 1
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	// page size between 0 and 1000
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      10,
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	listRequest.PageSize = 10
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())
}

func (s *WorkflowHandlerSuite) TestCountWorkflowExecutions() {
	wh := s.getWorkflowHandler(s.newConfig())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 5}, nil)

	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	countRequest.Query = query
	resp, err := wh.CountWorkflowExecutions(ctx, countRequest)
	s.NoError(err)
	s.Equal(int64(5), resp.Count)
}

func (s *WorkflowHandlerSuite) TestVerifyHistoryIsComplete() {
	logger := log.NewTestLogger()
	events := make([]*historyspb.StrippedHistoryEvent, 50)
	for i := 0; i < len(events); i++ {
		events[i] = &historyspb.StrippedHistoryEvent{EventId: int64(i + 1)}
	}
	var eventsWithHoles []*historyspb.StrippedHistoryEvent
	eventsWithHoles = append(eventsWithHoles, events[9:12]...)
	eventsWithHoles = append(eventsWithHoles, events[20:31]...)

	testCases := []struct {
		events       []*historyspb.StrippedHistoryEvent
		firstEventID int64
		lastEventID  int64
		isFirstPage  bool
		isLastPage   bool
		pageSize     int
		isResultErr  bool
	}{
		{events[:1], 1, 1, true, true, 1000, false},
		{events[:5], 1, 5, true, true, 1000, false},
		{events[9:31], 10, 31, true, true, 1000, false},
		{events[9:29], 10, 50, true, false, 20, false},
		{events[9:30], 10, 50, true, false, 20, false},

		{events[9:29], 1, 50, false, false, 20, false},
		{events[9:29], 1, 29, false, true, 20, false},

		{eventsWithHoles, 1, 50, false, false, 22, true},
		{eventsWithHoles, 10, 50, true, false, 22, true},
		{eventsWithHoles, 1, 31, false, true, 22, true},
		{eventsWithHoles, 10, 31, true, true, 1000, true},

		{events[9:31], 9, 31, true, true, 1000, true},
		{events[9:31], 9, 50, true, false, 22, true},
		{events[9:31], 11, 31, true, true, 1000, true},
		{events[9:31], 11, 50, true, false, 22, true},

		{events[9:31], 10, 30, true, true, 1000, true},
		{events[9:31], 1, 30, false, true, 22, true},
		{events[9:31], 10, 32, true, true, 1000, true},
		{events[9:31], 1, 32, false, true, 22, true},
	}

	for i, tc := range testCases {
		err := api.VerifyHistoryIsComplete(
			logger,
			tc.events[0],
			tc.events[len(tc.events)-1],
			len(tc.events),
			tc.firstEventID,
			tc.lastEventID,
			tc.isFirstPage,
			tc.isLastPage,
			tc.pageSize,
		)
		if tc.isResultErr {
			s.Error(err, "testcase %v failed", i)
		} else {
			s.NoError(err, "testcase %v failed", i)
		}
	}
}

func (s *WorkflowHandlerSuite) TestGetSystemInfo() {
	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.GetSystemInfo(context.Background(), &workflowservice.GetSystemInfoRequest{})
	s.NoError(err)
	s.Equal(headers.ServerVersion, resp.ServerVersion)
	s.True(resp.Capabilities.SignalAndQueryHeader)
	s.True(resp.Capabilities.InternalErrorDifferentiation)
	s.True(resp.Capabilities.ActivityFailureIncludeHeartbeat)
	s.True(resp.Capabilities.SupportsSchedules)
	s.True(resp.Capabilities.EncodedFailureAttributes)
	s.True(resp.Capabilities.UpsertMemo)
	s.True(resp.Capabilities.Nexus)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_Terminate() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	inputString := "unit test"
	jobId := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	params := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID.String(),
		BatchType:   enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:       testNamespace.String(),
			VisibilityQuery: inputString,
			JobId:           jobId,
			Reason:          inputString,
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{
					Identity: inputString,
				},
			},
		},
	}
	inputPayload, err := payloads.Encode(params)
	s.NoError(err)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeTerminate), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.ProtoEqual(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobId,
		Reason:    inputString,
		Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{
				Identity: inputString,
			},
		},
		VisibilityQuery: inputString,
	}

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_Cancellation() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	inputString := "unit test"
	jobId := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	params := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID.String(),
		BatchType:   enumspb.BATCH_OPERATION_TYPE_CANCEL,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:       testNamespace.String(),
			VisibilityQuery: inputString,
			JobId:           jobId,
			Reason:          inputString,
			Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
				CancellationOperation: &batchpb.BatchOperationCancellation{
					Identity: inputString,
				},
			},
		},
	}
	inputPayload, err := payloads.Encode(params)
	s.NoError(err)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeCancel), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.ProtoEqual(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobId,
		Reason:    inputString,
		Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
			CancellationOperation: &batchpb.BatchOperationCancellation{
				Identity: inputString,
			},
		},
		VisibilityQuery: inputString,
	}

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_Signal() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	inputString := "unit test"
	signalName := "signal name"
	jobId := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	signalPayloads := payloads.EncodeString(signalName)
	params := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID.String(),
		BatchType:   enumspb.BATCH_OPERATION_TYPE_SIGNAL,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:       testNamespace.String(),
			VisibilityQuery: inputString,
			JobId:           jobId,
			Reason:          inputString,
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{
					Signal:   signalName,
					Input:    signalPayloads,
					Identity: inputString,
				},
			},
		},
	}
	inputPayload, err := payloads.Encode(params)
	s.NoError(err)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeSignal), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.ProtoEqual(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobId,
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   signalName,
				Input:    signalPayloads,
				Identity: inputString,
			},
		},
		Reason:          inputString,
		VisibilityQuery: inputString,
	}

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Signal() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
	}
	reason := "reason"
	identity := "identity"
	signalName := "signal name"
	jobId := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	signalPayloads := payloads.EncodeString(signalName)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace:  testNamespace.String(),
		JobId:      jobId,
		Reason:     reason,
		Executions: executions,
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   signalName,
				Input:    signalPayloads,
				Identity: identity,
			},
		},
	}
	params := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID.String(),
		BatchType:   enumspb.BATCH_OPERATION_TYPE_SIGNAL,
		Request:     request,
	}
	inputPayload, err := payloads.Encode(params)
	s.NoError(err)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(identity, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeSignal), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(reason), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(identity), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.ProtoEqual(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Reset() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
	}
	reason := "reason"
	identity := "identity"
	jobId := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	params := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID.String(),
		BatchType:   enumspb.BATCH_OPERATION_TYPE_RESET,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:  testNamespace.String(),
			JobId:      jobId,
			Reason:     reason,
			Executions: executions,
			Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
				ResetOperation: &batchpb.BatchOperationReset{
					Identity:         identity,
					ResetType:        enumspb.RESET_TYPE_LAST_WORKFLOW_TASK,
					ResetReapplyType: enumspb.RESET_REAPPLY_TYPE_NONE,
				},
			},
		},
	}
	inputPayload, err := payloads.Encode(params)
	s.NoError(err)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(identity, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeReset), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(reason), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(identity), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.ProtoEqual(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobId,
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				ResetType:        enumspb.RESET_TYPE_LAST_WORKFLOW_TASK,
				ResetReapplyType: enumspb.RESET_REAPPLY_TYPE_NONE,
				Identity:         identity,
			},
		},
		Reason:     reason,
		Executions: executions,
	}

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Reset_WithPostResetOperations() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
	}
	reason := "reason"
	identity := "identity"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	// Create post-reset operations to test the serialization fix
	postResetOps := []*workflowpb.PostResetOperation{
		{
			Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
				UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
					WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
					UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
				},
			},
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(batcher.BatchWFTypeProtobufName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(identity, request.StartRequest.Identity)
			s.ProtoEqual(payload.EncodeString(batcher.BatchTypeReset), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.ProtoEqual(payload.EncodeString(reason), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.ProtoEqual(payload.EncodeString(identity), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])

			// Decode the input and verify PostResetOperations are correctly set
			var batchParams batchspb.BatchOperationInput
			err := payloads.Decode(request.StartRequest.Input, &batchParams)
			s.NoError(err)

			// Verify that PostResetOperations slice has the correct length and no nil values
			s.Len(batchParams.Request.Operation.(*workflowservice.StartBatchOperationRequest_ResetOperation).ResetOperation.PostResetOperations, len(postResetOps))

			for i, encoded := range batchParams.Request.Operation.(*workflowservice.StartBatchOperationRequest_ResetOperation).ResetOperation.PostResetOperations {
				s.ProtoEqual(postResetOps[i], encoded)
			}

			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)

	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.NewString(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Options: &commonpb.ResetOptions{
					Target: &commonpb.ResetOptions_WorkflowTaskId{
						WorkflowTaskId: 10,
					},
				},
				PostResetOperations: postResetOps,
				Identity:            identity,
			},
		},
		Reason:     reason,
		Executions: executions,
	}

	_, err := wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Reset_EmptyPostResetOperations() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
	}
	reason := "reason"
	identity := "identity"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.StartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.StartWorkflowExecutionResponse, error) {
			// Decode the input and verify PostResetOperations slice is properly initialized
			var batchParams batchspb.BatchOperationInput
			err := payloads.Decode(request.StartRequest.Input, &batchParams)
			s.NoError(err)
			s.Len(batchParams.Request.Operation.(*workflowservice.StartBatchOperationRequest_ResetOperation).ResetOperation.PostResetOperations, 0)

			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)

	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.NewString(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Options: &commonpb.ResetOptions{
					Target: &commonpb.ResetOptions_WorkflowTaskId{
						WorkflowTaskId: 10,
					},
				},
				PostResetOperations: []*workflowpb.PostResetOperation{}, // Empty slice
				Identity:            identity,
			},
		},
		Reason:     reason,
		Executions: executions,
	}

	_, err := wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_TooMany() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
	}
	reason := "reason"
	identity := "identity"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	// StartBatchOperation API uses CountWorkflowExecutions to know how many existing in-flight batch operations.
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 1}, nil)

	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.NewString(),
		Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
			CancellationOperation: &batchpb.BatchOperationCancellation{
				Identity: identity,
			},
		},
		Reason:     reason,
		Executions: executions,
	}

	_, err := wh.StartBatchOperation(context.Background(), request)
	s.EqualError(err, "Max concurrent batch operations is reached")
}

func (s *WorkflowHandlerSuite) TestStartBatchOperation_InvalidRequest() {
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.NewString(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   "signalName",
				Identity: "identity",
			},
		},
		Reason:          uuid.NewString(),
		VisibilityQuery: uuid.NewString(),
	}

	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.NewString()
	request.JobId = ""
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.JobId = uuid.NewString()
	request.Operation = nil
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Operation = &workflowservice.StartBatchOperationRequest_SignalOperation{
		SignalOperation: &batchpb.BatchOperationSignal{
			Signal:   "signalName",
			Identity: "identity",
		},
	}
	request.Reason = ""
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Reason = uuid.NewString()
	request.VisibilityQuery = ""
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *WorkflowHandlerSuite) TestStopBatchOperation() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	jobID := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.TerminateWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.Equal(namespaceID.String(), request.NamespaceId)
			s.Equal(jobID, request.TerminateRequest.WorkflowExecution.GetWorkflowId())
			s.Equal("", request.TerminateRequest.WorkflowExecution.GetRunId())
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	)
	request := &workflowservice.StopBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobID,
		Reason:    "reason",
	}

	_, err := wh.StopBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowHandlerSuite) TestStopBatchOperation_InvalidRequest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	request := &workflowservice.StopBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.NewString(),
		Reason:    "reason",
	}

	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.NewString()
	request.JobId = ""
	_, err = wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.JobId = uuid.NewString()
	request.Reason = ""
	_, err = wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *WorkflowHandlerSuite) TestDescribeBatchOperation_CompletedStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	jobID := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	now := timestamppb.New(time.Now())
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.Run("StatsNotInMemo", func() {
		s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *historyservice.DescribeWorkflowExecutionRequest,
				_ ...grpc.CallOption,
			) (*historyservice.DescribeWorkflowExecutionResponse, error) {
				return &historyservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: jobID,
						},
						StartTime:     now,
						CloseTime:     now,
						Status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						ExecutionTime: now,
						Memo: &commonpb.Memo{
							Fields: map[string]*commonpb.Payload{
								batcher.BatchOperationTypeMemo: payload.EncodeString(batcher.BatchTypeReset),
							},
						},
						SearchAttributes: nil,
					},
				}, nil
			},
		)
		request := &workflowservice.DescribeBatchOperationRequest{
			Namespace: testNamespace.String(),
			JobId:     jobID,
		}

		_, err := wh.DescribeBatchOperation(context.Background(), request)
		s.Require().Error(err)
		s.Contains(err.Error(), "batch operation stats are not present in the memo")
	})
	s.Run("StatsInMemo", func() {
		s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *historyservice.DescribeWorkflowExecutionRequest,
				_ ...grpc.CallOption,
			) (*historyservice.DescribeWorkflowExecutionResponse, error) {

				statsPayload, err := payload.Encode(batcher.BatchOperationStats{
					NumSuccess: 2,
					NumFailure: 1,
				})
				s.Require().NoError(err)
				return &historyservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: jobID,
						},
						StartTime:     now,
						CloseTime:     now,
						Status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						ExecutionTime: now,
						Memo: &commonpb.Memo{
							Fields: map[string]*commonpb.Payload{
								batcher.BatchOperationTypeMemo:  payload.EncodeString(batcher.BatchTypeTerminate),
								batcher.BatchOperationStatsMemo: statsPayload,
							},
						},
						SearchAttributes: nil,
					},
				}, nil
			},
		)
		request := &workflowservice.DescribeBatchOperationRequest{
			Namespace: testNamespace.String(),
			JobId:     jobID,
		}

		resp, err := wh.DescribeBatchOperation(context.Background(), request)
		s.Require().NoError(err)
		s.Equal(jobID, resp.GetJobId())
		s.Equal(now, resp.GetStartTime())
		s.Equal(now, resp.GetCloseTime())
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE, resp.GetOperationType())
		s.Equal(enumspb.BATCH_OPERATION_STATE_COMPLETED, resp.GetState())
		s.Equal(int64(3), resp.GetTotalOperationCount())
		s.Equal(int64(2), resp.GetCompleteOperationCount())
		s.Equal(int64(1), resp.GetFailureOperationCount())
	})
}

func (s *WorkflowHandlerSuite) TestDescribeBatchOperation_RunningStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	jobID := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	now := timestamppb.New(time.Now())
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.DescribeWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			hbdPayload, err := payloads.Encode(batcher.HeartBeatDetails{
				TotalEstimate: 5,
				SuccessCount:  3,
				ErrorCount:    1,
			})
			s.Require().NoError(err)
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: jobID,
					},
					StartTime:     now,
					CloseTime:     now,
					Status:        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					ExecutionTime: now,
					Memo: &commonpb.Memo{
						Fields: map[string]*commonpb.Payload{
							batcher.BatchOperationTypeMemo: payload.EncodeString(batcher.BatchTypeTerminate),
						},
					},
					SearchAttributes: nil,
				},
				PendingActivities: []*workflowpb.PendingActivityInfo{
					{
						HeartbeatDetails: hbdPayload,
					},
				},
			}, nil
		},
	)
	request := &workflowservice.DescribeBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobID,
	}

	resp, err := wh.DescribeBatchOperation(context.Background(), request)
	s.NoError(err)
	s.Equal(jobID, resp.GetJobId())
	s.Equal(now, resp.GetStartTime())
	s.Equal(now, resp.GetCloseTime())
	s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE, resp.GetOperationType())
	s.Equal(enumspb.BATCH_OPERATION_STATE_RUNNING, resp.GetState())
	s.Assert().Equal(int64(5), resp.TotalOperationCount)
	s.Assert().Equal(int64(3), resp.CompleteOperationCount)
	s.Assert().Equal(int64(1), resp.FailureOperationCount)
}

func (s *WorkflowHandlerSuite) TestDescribeBatchOperation_FailedStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	jobID := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	now := timestamppb.New(time.Now())
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.DescribeWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.DescribeWorkflowExecutionResponse, error) {

			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: jobID,
					},
					StartTime:     now,
					CloseTime:     now,
					Status:        enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
					ExecutionTime: now,
					Memo: &commonpb.Memo{
						Fields: map[string]*commonpb.Payload{
							batcher.BatchOperationTypeMemo: payload.EncodeString(batcher.BatchTypeTerminate),
						},
					},
					SearchAttributes: nil,
				},
			}, nil
		},
	)
	request := &workflowservice.DescribeBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     jobID,
	}

	resp, err := wh.DescribeBatchOperation(context.Background(), request)
	s.NoError(err)
	s.Equal(jobID, resp.GetJobId())
	s.Equal(now, resp.GetStartTime())
	s.Equal(now, resp.GetCloseTime())
	s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE, resp.GetOperationType())
	s.Equal(enumspb.BATCH_OPERATION_STATE_FAILED, resp.GetState())
}

func (s *WorkflowHandlerSuite) TestDescribeBatchOperation_InvalidRequest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	request := &workflowservice.DescribeBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.NewString(),
	}
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.DescribeBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.NewString()
	request.JobId = ""
	_, err = wh.DescribeBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *WorkflowHandlerSuite) TestListBatchOperations() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.NewString())
	jobID := uuid.NewString()
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	now := timestamppb.New(time.Now())
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetReadStoreName(testNamespace).Return("").AnyTimes()
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *manager.ListWorkflowExecutionsRequestV2,
		) (*manager.ListWorkflowExecutionsResponse, error) {
			s.True(strings.Contains(request.Query, searchattribute.TemporalNamespaceDivision))
			return &manager.ListWorkflowExecutionsResponse{
				Executions: []*workflowpb.WorkflowExecutionInfo{
					{Execution: &commonpb.WorkflowExecution{
						WorkflowId: jobID,
					},
						StartTime:     now,
						CloseTime:     now,
						Status:        enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
						ExecutionTime: now,
						Memo: &commonpb.Memo{
							Fields: map[string]*commonpb.Payload{
								batcher.BatchOperationTypeMemo: payload.EncodeString(batcher.BatchTypeTerminate),
							},
						},
					},
				},
			}, nil
		},
	)
	request := &workflowservice.ListBatchOperationsRequest{
		Namespace: testNamespace.String(),
	}

	resp, err := wh.ListBatchOperations(context.Background(), request)
	s.NoError(err)
	s.Equal(1, len(resp.OperationInfo))
	s.Equal(jobID, resp.OperationInfo[0].GetJobId())
	s.Equal(now, resp.OperationInfo[0].GetStartTime())
	s.Equal(now, resp.OperationInfo[0].GetCloseTime())
	s.Equal(enumspb.BATCH_OPERATION_STATE_FAILED, resp.OperationInfo[0].GetState())
}

func (s *WorkflowHandlerSuite) TestListBatchOperations_InvalidRerquest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	request := &workflowservice.ListBatchOperationsRequest{
		Namespace: "",
	}
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.ListBatchOperations(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

// This test is to make sure that GetWorkflowExecutionHistory returns the correct history when history service sends
// History events in the field response.History. This happens when history.sendRawHistoryBetweenInternalServices is enabled.
// This test verifies that HistoryEventFilterType is applied and EVENT_TYPE_WORKFLOW_EXECUTION_FAILED is converted to
// EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW for older SDKs.
func (s *WorkflowHandlerSuite) TestGetWorkflowExecutionHistory_InternalRawHistoryEnabled() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New().String()}
	newRunID := uuid.New().String()

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).Times(2)
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), gomock.Any()).Return(searchattribute.TestNameTypeMap, nil).Times(2)

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              tests.Namespace.String(),
		Execution:              &we,
		MaximumPageSize:        10,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		SkipArchival:           true,
	}
	s.mockHistoryClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request:     req,
	}).Return(&historyservice.GetWorkflowExecutionHistoryResponse{
		Response: &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{},
		},
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   int64(5),
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
					Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
						WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
							Failure: &failurepb.Failure{Message: "this workflow task failed"},
						},
					},
				},
				{
					EventId:   int64(5),
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
						WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
							Failure:                      &failurepb.Failure{Message: "this workflow failed"},
							RetryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
							WorkflowTaskCompletedEventId: 4,
							NewExecutionRunId:            newRunID,
						},
					},
				},
			},
		},
	}, nil).Times(2)

	oldGoSDKVersion := "1.9.1"
	newGoSDKVersion := "1.10.1"

	// new sdk: should see failed event
	ctx := headers.SetVersionsForTests(context.Background(), newGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := wh.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Archived)
	event := resp.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.EventType)
	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	s.Equal("this workflow failed", attrs.Failure.Message)
	s.Equal(newRunID, attrs.NewExecutionRunId)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, attrs.RetryState)

	// old sdk: should see continued-as-new event
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// See comment in workflowHandler.go:GetWorkflowExecutionHistory
	ctx = headers.SetVersionsForTests(context.Background(), oldGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, "")
	resp, err = wh.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Archived)
	event = resp.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, event.EventType)
	attrs2 := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(newRunID, attrs2.NewExecutionRunId)
	s.Equal("this workflow failed", attrs2.Failure.Message)
}

func (s *WorkflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewNoopCollection(), numHistoryShards)
}

func updateRequest(
	historyArchivalURI string,
	historyArchivalState enumspb.ArchivalState,
	visibilityArchivalURI string,
	visibilityArchivalState enumspb.ArchivalState,
) *workflowservice.UpdateNamespaceRequest {
	return &workflowservice.UpdateNamespaceRequest{
		Namespace: "test-name",
		Config: &namespacepb.NamespaceConfig{
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
		},
	}
}

func persistenceGetNamespaceResponse(historyArchivalState, visibilityArchivalState *namespace.ArchivalConfigState) *persistence.GetNamespaceResponse {
	return &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          testNamespaceID,
				Name:        "test-name",
				State:       0,
				Description: "test-description",
				Owner:       "test-owner-email",
				Data:        make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationFromDays(1),
				HistoryArchivalState:    historyArchivalState.State,
				HistoryArchivalUri:      historyArchivalState.URI,
				VisibilityArchivalState: visibilityArchivalState.State,
				VisibilityArchivalUri:   visibilityArchivalState.URI,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
				},
			},
			ConfigVersion:               0,
			FailoverVersion:             0,
			FailoverNotificationVersion: 0,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: 0,
	}
}

func registerNamespaceRequest(
	historyArchivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalState enumspb.ArchivalState,
	visibilityArchivalURI string,
) *workflowservice.RegisterNamespaceRequest {
	return &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "test-namespace",
		Description:                      "test-description",
		OwnerEmail:                       "test-owner-email",
		WorkflowExecutionRetentionPeriod: durationpb.New(10 * time.Hour * 24),
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{
				ClusterName: cluster.TestCurrentClusterName,
			},
		},
		ActiveClusterName:       cluster.TestCurrentClusterName,
		Data:                    make(map[string]string),
		HistoryArchivalState:    historyArchivalState,
		HistoryArchivalUri:      historyArchivalURI,
		VisibilityArchivalState: visibilityArchivalState,
		VisibilityArchivalUri:   visibilityArchivalURI,
		IsGlobalNamespace:       false,
	}
}

func getHistoryRequest(nextPageToken []byte) *workflowservice.GetWorkflowExecutionHistoryRequest {
	return &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		NextPageToken: nextPageToken,
	}
}

func listArchivedWorkflowExecutionsTestRequest() *workflowservice.ListArchivedWorkflowExecutionsRequest {
	return &workflowservice.ListArchivedWorkflowExecutionsRequest{
		Namespace: "some random namespace name",
		PageSize:  10,
		Query:     "some random query string",
	}
}

func TestContextNearDeadline(t *testing.T) {
	assert.False(t, contextNearDeadline(context.Background(), longPollTailRoom))

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()
	assert.True(t, contextNearDeadline(ctx, longPollTailRoom))
	assert.False(t, contextNearDeadline(ctx, time.Millisecond))
}

func TestValidateRequestId(t *testing.T) {
	req := workflowservice.StartWorkflowExecutionRequest{RequestId: ""}
	err := validateRequestId(&req.RequestId, 100)
	assert.Nil(t, err)
	assert.Len(t, req.RequestId, 36) // new UUID length
}

func TestDedupLinksFromCallbacks(t *testing.T) {
	links := []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "test-ns",
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   3,
							EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
						},
					},
				},
			},
		},
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "test-ns",
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   5,
							EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
						},
					},
				},
			},
		},
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "test-ns",
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: "test-request-id",
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
						},
					},
				},
			},
		},
	}
	callbacks := []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{},
			},
			Links: []*commonpb.Link{
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  "test-ns",
							WorkflowId: "test-workflow-id",
							RunId:      "test-run-id",
							Reference: &commonpb.Link_WorkflowEvent_EventRef{
								EventRef: &commonpb.Link_WorkflowEvent_EventReference{
									EventId:   3,
									EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
								},
							},
						},
					},
				},
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  "test-ns",
							WorkflowId: "test-workflow-id",
							RunId:      "test-run-id",
							Reference: &commonpb.Link_WorkflowEvent_EventRef{
								EventRef: &commonpb.Link_WorkflowEvent_EventReference{
									EventId:   5,
									EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
								},
							},
						},
					},
				},
			},
		},
		{
			Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			},
			Links: []*commonpb.Link{
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  "test-ns",
							WorkflowId: "test-workflow-id",
							RunId:      "test-run-id",
							Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
								RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
									RequestId: "test-request-id",
									EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
								},
							},
						},
					},
				},
			},
		},
	}

	dedupedLinks := dedupLinksFromCallbacks(links, callbacks)
	assert.Len(t, dedupedLinks, 1)
	protoassert.ProtoEqual(
		t,
		&commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "test-ns",
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: "test-request-id",
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
						},
					},
				},
			},
		},
		dedupedLinks[0],
	)
}

func (s *WorkflowHandlerSuite) Test_DeleteWorkflowExecution() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *workflowservice.DeleteWorkflowExecutionRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &workflowservice.DeleteWorkflowExecutionRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "Execution is not set on request."},
		},
		{
			Name: "empty namespace",
			Request: &workflowservice.DeleteWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: "test-workflow-id",
					RunId:      "wrong-run-id",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Invalid RunId."},
		},
	}
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := wh.DeleteWorkflowExecution(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// History call failed.
	s.mockResource.HistoryClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("random error"))
	s.mockResource.NamespaceCache.EXPECT().GetNamespaceID(namespace.Name("test-namespace")).Return(namespace.ID("test-namespace-id"), nil)
	resp, err := wh.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "test-workflow-id",
			RunId:      "d2595cb3-3b21-4026-a3e8-17bc32fb2a2b",
		},
	})
	s.Error(err)
	s.Equal("random error", err.Error())
	s.Nil(resp)

	// Success case.
	s.mockResource.HistoryClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)
	s.mockResource.NamespaceCache.EXPECT().GetNamespaceID(namespace.Name("test-namespace")).Return(namespace.ID("test-namespace-id"), nil)
	resp, err = wh.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: "test-namespace",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "test-workflow-id",
			// RunId is not required.
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *WorkflowHandlerSuite) TestExecuteMultiOperation() {
	ctx := context.Background()
	config := s.newConfig()
	config.EnableExecuteMultiOperation = func(string) bool { return true }
	wh := s.getWorkflowHandler(config)

	s.mockResource.NamespaceCache.EXPECT().
		GetNamespaceID(namespace.Name(s.testNamespace.String())).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockSearchAttributesMapperProvider.EXPECT().
		GetMapper(gomock.Any()).Return(nil, nil).AnyTimes()

	newStartOp := func(op *workflowservice.StartWorkflowExecutionRequest) *workflowservice.ExecuteMultiOperationRequest_Operation {
		return &workflowservice.ExecuteMultiOperationRequest_Operation{
			Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
				StartWorkflow: op,
			},
		}
	}
	validStartReq := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.testNamespace.String(),
			WorkflowId:   "WORKFLOW_ID",
			WorkflowType: &commonpb.WorkflowType{Name: "workflow-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
		}
	}
	newUpdateOp := func(op *workflowservice.UpdateWorkflowExecutionRequest) *workflowservice.ExecuteMultiOperationRequest_Operation {
		return &workflowservice.ExecuteMultiOperationRequest_Operation{
			Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
				UpdateWorkflow: op,
			},
		}
	}
	validUpdateReq := func() *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.testNamespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "WORKFLOW_ID"},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
	}

	// NOTE: functional tests are testing the happy case

	s.Run("operations list that is not [Start, Update] is invalid", func() {
		// empty list
		resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)

		// 1 item
		resp, err = wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace:  s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{newStartOp(nil)},
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)

		// 3 items
		resp, err = wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				newStartOp(nil), newStartOp(nil), newStartOp(nil),
			},
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)

		// 2 undefined operations
		resp, err = wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				{},
				{},
			},
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)

		// 2 Starts
		resp, err = wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				newStartOp(validStartReq()), newStartOp(validStartReq()),
			},
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)

		// 2 Updates
		resp, err = wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				newUpdateOp(validUpdateReq()), newUpdateOp(validUpdateReq()),
			},
		})

		s.Nil(resp)
		s.Equal(errMultiOpNotStartAndUpdate, err)
	})

	assertMultiOpsErr := func(expectedErrs []error, actual error) {
		s.Equal("Update-with-Start could not be executed.", actual.Error())
		s.EqualValues(expectedErrs, actual.(*serviceerror.MultiOperationExecution).OperationErrors())
	}

	s.Run("operation with different workflow ID as previous operation is invalid", func() {
		updateReq := validUpdateReq()
		updateReq.WorkflowExecution.WorkflowId = "foo"

		resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.testNamespace.String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				newStartOp(validStartReq()),
				newUpdateOp(updateReq),
			},
		})

		s.Nil(resp)
		assertMultiOpsErr([]error{errMultiOpAborted, errMultiOpWorkflowIdInconsistent}, err)
	})

	s.Run("Start operation is validated", func() {
		// expecting the same validation as for standalone Start operation; only testing one here:
		s.Run("requires workflow id", func() {
			startReq := validStartReq()
			startReq.WorkflowId = ""

			resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.testNamespace.String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					newStartOp(startReq),
					newUpdateOp(validUpdateReq()),
				},
			})

			s.Nil(resp)
			assertMultiOpsErr([]error{errWorkflowIDNotSet, errMultiOpAborted}, err)
		})

		// unique to MultiOperation:
		s.Run("`cron_schedule` is invalid", func() {
			startReq := validStartReq()
			startReq.CronSchedule = "0 */12 * * *"

			resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.testNamespace.String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					newStartOp(startReq),
					newUpdateOp(validUpdateReq()),
				},
			})

			s.Nil(resp)
			assertMultiOpsErr([]error{errMultiOpStartCronSchedule, errMultiOpAborted}, err)
		})

		// unique to MultiOperation:
		s.Run("`request_eager_execution` is invalid", func() {
			startReq := validStartReq()
			startReq.RequestEagerExecution = true

			resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.testNamespace.String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					newStartOp(startReq),
					newUpdateOp(validUpdateReq()),
				},
			})

			s.Nil(resp)
			assertMultiOpsErr([]error{errMultiOpEagerWorkflow, errMultiOpAborted}, err)
		})

		// unique to MultiOperation:
		s.Run("`workflow_start_delay` is invalid", func() {
			startReq := validStartReq()
			startReq.WorkflowStartDelay = durationpb.New(1 * time.Second)

			resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.testNamespace.String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					newStartOp(startReq),
					newUpdateOp(validUpdateReq()),
				},
			})

			s.Nil(resp)
			assertMultiOpsErr([]error{errMultiOpStartDelay, errMultiOpAborted}, err)
		})
	})

	s.Run("Update operation is validated", func() {
		// expecting the same validation as for standalone Update operation; only testing a few of the validations here
		s.Run("requires workflow id", func() {
			updateReq := validUpdateReq()
			updateReq.Request.Input = nil

			resp, err := wh.ExecuteMultiOperation(ctx, &workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.testNamespace.String(),
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					newStartOp(validStartReq()),
					newUpdateOp(updateReq),
				},
			})

			s.Nil(resp)
			assertMultiOpsErr([]error{errMultiOpAborted, errUpdateInputNotSet}, err)
		})
	})
}

func (s *WorkflowHandlerSuite) TestShutdownWorker() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	ctx := context.Background()

	stickyTaskQueue := "sticky-task-queue"

	expectedMatchingRequest := &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: s.testNamespaceID.String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     stickyTaskQueue,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Eq(s.testNamespace)).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().ForceUnloadTaskQueuePartition(gomock.Any(), gomock.Eq(expectedMatchingRequest)).Return(&matchingservice.ForceUnloadTaskQueuePartitionResponse{}, nil)

	_, err := wh.ShutdownWorker(ctx, &workflowservice.ShutdownWorkerRequest{
		Namespace:       s.testNamespace.String(),
		StickyTaskQueue: stickyTaskQueue,
		Identity:        "worker",
		Reason:          "graceful shutdown",
	})
	if err != nil {
		s.Fail("ShutdownWorker failed:", err)
	}
}

func (s *WorkflowHandlerSuite) TestPatchSchedule_TriggerImmediatelyScheduledTime() {
	config := s.newConfig()
	config.EnableSchedules = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)
	ctx := context.Background()

	scheduleID := "test-schedule-id"
	requestID := "test-request-id"

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Eq(s.testNamespace)).Return(s.testNamespaceID, nil).AnyTimes()

	testCases := []struct {
		name                    string
		setupTrigger            func() *schedulepb.TriggerImmediatelyRequest
		expectScheduledTimeSet  bool
		expectHistoryClientCall bool
	}{
		{
			name: "trigger with nil ScheduledTime should get timestamp set",
			setupTrigger: func() *schedulepb.TriggerImmediatelyRequest {
				return &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					ScheduledTime: nil,
				}
			},
			expectScheduledTimeSet:  true,
			expectHistoryClientCall: true,
		},
		{
			name: "trigger with existing ScheduledTime should not be modified",
			setupTrigger: func() *schedulepb.TriggerImmediatelyRequest {
				existingTime := timestamppb.New(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))
				return &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					ScheduledTime: existingTime,
				}
			},
			expectScheduledTimeSet:  false, // Should not modify existing time
			expectHistoryClientCall: true,
		},
		{
			name: "no trigger should not call history client",
			setupTrigger: func() *schedulepb.TriggerImmediatelyRequest {
				return nil
			},
			expectScheduledTimeSet:  false,
			expectHistoryClientCall: true, // Will still call but without trigger
		},
	}

	for _, tt := range testCases {
		s.Run(tt.name, func() {
			trigger := tt.setupTrigger()

			patch := &schedulepb.SchedulePatch{}
			if trigger != nil {
				patch.TriggerImmediately = trigger
			}

			request := &workflowservice.PatchScheduleRequest{
				Namespace:  s.testNamespace.String(),
				ScheduleId: scheduleID,
				RequestId:  requestID,
				Patch:      patch,
				Identity:   "test-identity",
			}

			// Capture the original ScheduledTime if it exists
			var originalScheduledTime *timestamppb.Timestamp
			if trigger != nil {
				originalScheduledTime = trigger.ScheduledTime
			}

			if tt.expectHistoryClientCall {
				s.mockHistoryClient.EXPECT().SignalWorkflowExecution(
					gomock.Any(),
					gomock.Any(),
				).Return(&historyservice.SignalWorkflowExecutionResponse{}, nil).Times(1)
			}

			beforeCall := time.Now()
			resp, err := wh.PatchSchedule(ctx, request)
			afterCall := time.Now()

			s.NoError(err)
			s.NotNil(resp)

			if trigger != nil {
				if tt.expectScheduledTimeSet {
					// Verify that ScheduledTime was set and is recent
					s.NotNil(trigger.ScheduledTime, "ScheduledTime should have been set")
					s.Nil(originalScheduledTime, "Original ScheduledTime should have been nil")

					scheduledTime := trigger.ScheduledTime.AsTime()
					s.True(scheduledTime.After(beforeCall) || scheduledTime.Equal(beforeCall),
						"ScheduledTime should be at or after the call start time")
					s.True(scheduledTime.Before(afterCall) || scheduledTime.Equal(afterCall),
						"ScheduledTime should be at or before the call end time")
				} else {
					// Verify that existing ScheduledTime was not modified
					if originalScheduledTime != nil {
						s.Equal(originalScheduledTime.AsTime(), trigger.ScheduledTime.AsTime(),
							"Existing ScheduledTime should not have been modified")
					}
				}
			}
		})
	}
}

func (s *WorkflowHandlerSuite) TestPatchSchedule_ValidationAndErrors() {
	config := s.newConfig()
	config.EnableSchedules = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)
	ctx := context.Background()

	s.Run("nil request should return error", func() {
		resp, err := wh.PatchSchedule(ctx, nil)
		s.Nil(resp)
		s.Equal(errRequestNotSet, err)
	})

	s.Run("schedules disabled should return error", func() {
		disabledConfig := s.newConfig()
		disabledConfig.EnableSchedules = dc.GetBoolPropertyFnFilteredByNamespace(false)
		disabledWh := s.getWorkflowHandler(disabledConfig)

		request := &workflowservice.PatchScheduleRequest{
			Namespace:  s.testNamespace.String(),
			ScheduleId: "test-schedule",
			Patch:      &schedulepb.SchedulePatch{},
		}

		resp, err := disabledWh.PatchSchedule(ctx, request)
		s.Nil(resp)
		s.Equal(errSchedulesNotAllowed, err)
	})

	s.Run("request ID too long should return error", func() {
		longRequestID := strings.Repeat("a", common.ScheduleNotesSizeLimit+1)
		request := &workflowservice.PatchScheduleRequest{
			Namespace:  s.testNamespace.String(),
			ScheduleId: "test-schedule",
			RequestId:  longRequestID,
			Patch:      &schedulepb.SchedulePatch{},
		}

		resp, err := wh.PatchSchedule(ctx, request)
		s.Nil(resp)
		s.Equal(errRequestIDTooLong, err)
	})

	s.Run("invalid namespace should return error", func() {
		s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Eq(namespace.Name("invalid-namespace"))).Return(namespace.ID(""), errors.New("namespace not found")).Times(1)

		request := &workflowservice.PatchScheduleRequest{
			Namespace:  "invalid-namespace",
			ScheduleId: "test-schedule",
			Patch:      &schedulepb.SchedulePatch{},
		}

		resp, err := wh.PatchSchedule(ctx, request)
		s.Nil(resp)
		s.Error(err)
	})
}
