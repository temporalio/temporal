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

package frontend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	e "go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/service/worker/scheduler"
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
	workflowHandlerSuite struct {
		suite.Suite
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

		tokenSerializer common.TaskTokenSerializer

		testNamespace   namespace.Name
		testNamespaceID namespace.ID
	}
)

var testNamespaceID = primitives.MustValidateUUID("deadbeef-c001-4567-890a-bcdef0123456")

func TestWorkflowHandlerSuite(t *testing.T) {
	s := new(workflowHandlerSuite)
	suite.Run(t, s)
}

func (s *workflowHandlerSuite) SetupSuite() {
}

func (s *workflowHandlerSuite) TearDownSuite() {
}

func (s *workflowHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

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

	s.tokenSerializer = common.NewProtoTaskTokenSerializer()

	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName}).AnyTimes()
	s.mockExecutionManager.EXPECT().GetName().Return("mock-execution-manager").AnyTimes()
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
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
	)
}

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
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

func (s *workflowHandlerSuite) TestPollForTask_Failed_ContextTimeoutTooShort() {
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

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_StartRequestNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	_, err := wh.StartWorkflowExecution(context.Background(), nil)
	s.Error(err)
	s.Equal(errRequestNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_NamespaceNotSet() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowIdNotSet() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowIDNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowTypeNotSet() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowTypeNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_TaskQueueNotSet() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errTaskQueueNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidExecutionTimeout() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errInvalidWorkflowExecutionTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidRunTimeout() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errInvalidWorkflowRunTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_EnsureNonNilRetryPolicyInitialized() {
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
		RequestId:                uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(&commonpb.RetryPolicy{
		BackoffCoefficient: 2.0,
		InitialInterval:    durationpb.New(time.Second),
		MaximumInterval:    durationpb.New(100 * time.Second),
	}, startWorkflowExecutionRequest.RetryPolicy)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_EnsureNilRetryPolicyNotInitialized() {
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
		RequestId:                uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Nil(startWorkflowExecutionRequest.RetryPolicy)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidTaskTimeout() {
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
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errInvalidWorkflowTaskTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_CronAndStartDelaySet() {
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
		RequestId:          uuid.New(),
		CronSchedule:       "dummy-cron-schedule",
		WorkflowStartDelay: durationpb.New(10 * time.Second),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.ErrorIs(err, errCronAndStartDelaySet)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidStartDelay() {
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
		RequestId:          uuid.New(),
		WorkflowStartDelay: durationpb.New(-10 * time.Second),
	}

	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)

	s.ErrorIs(err, errInvalidWorkflowStartDelaySeconds)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_InvalidWorkflowIdReusePolicy_TerminateIfRunning() {
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
		"Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy."))
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_DefaultWorkflowIdDuplicationPolicies() {
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

func (s *workflowHandlerSuite) TestSignalWithStartWorkflowExecution_InvalidWorkflowIdConflictPolicy() {
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

func (s *workflowHandlerSuite) TestSignalWithStartWorkflowExecution_InvalidWorkflowIdReusePolicy_TerminateIfRunning() {
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
		"Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy."))
}

func (s *workflowHandlerSuite) TestSignalWithStartWorkflowExecution_DefaultWorkflowIdDuplicationPolicies() {
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

func (s *workflowHandlerSuite) TestRegisterNamespace_Failure_InvalidArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	s.mockArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	s.mockHistoryArchiver.EXPECT().ValidateURI(gomock.Any()).Return(nil)
	s.mockVisibilityArchiver.EXPECT().ValidateURI(gomock.Any()).Return(errors.New("invalid URI"))
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestRegisterNamespace_Success_EnabledWithNoArchivalURI() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_ENABLED, "", enumspb.ARCHIVAL_STATE_ENABLED, "")
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestRegisterNamespace_Success_EnabledWithArchivalURI() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestRegisterNamespace_Success_ClusterNotConfiguredForArchival() {
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

func (s *workflowHandlerSuite) TestRegisterNamespace_Success_NotEnabled() {
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

func (s *workflowHandlerSuite) TestDeprecateNamespace_Success() {
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

func (s *workflowHandlerSuite) TestDeprecateNamespace_Error() {
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

func (s *workflowHandlerSuite) TestDeleteNamespace_Success() {
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

func (s *workflowHandlerSuite) TestDeleteNamespace_Error() {
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

func (s *workflowHandlerSuite) TestDescribeNamespace_Success_ArchivalDisabled() {
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

func (s *workflowHandlerSuite) TestDescribeNamespace_Success_ArchivalEnabled() {
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

func (s *workflowHandlerSuite) TestUpdateNamespace_Failure_UpdateExistingArchivalURI() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)

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

func (s *workflowHandlerSuite) TestUpdateNamespace_Failure_InvalidArchivalURI() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)

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

func (s *workflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToArchivalDisabledWithoutSettingURI() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestUpdateNamespace_Success_ClusterNotConfiguredForArchival() {
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

func (s *workflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToArchivalDisabledWithSettingBucket() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestUpdateNamespace_Success_ArchivalEnabledToEnabled() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestUpdateNamespace_Success_ArchivalNeverEnabledToEnabled() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

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

func (s *workflowHandlerSuite) TestHistoryArchived() {
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

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_NamespaceCacheEntryError() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, errors.New("error getting namespace"))

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_ArchivalURIEmpty() {
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

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidURI() {
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

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
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
	s.mockArchiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	s.Equal(history, resp.History)
	s.Equal(nextPageToken, resp.NextPageToken)
	s.True(resp.GetArchived())
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidRequest() {
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), &workflowservice.ListArchivedWorkflowExecutionsRequest{})
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_ClusterNotConfiguredForArchival() {
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_NamespaceCacheEntryError() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(nil, errors.New("error getting namespace"))
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_NamespaceNotConfiguredForArchival() {
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

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidURI() {
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

func (s *workflowHandlerSuite) TestListArchivedVisibility_Success() {
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
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes("", false)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.NotNil(resp)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestGetSearchAttributes() {
	wh := s.getWorkflowHandler(s.newConfig())

	ctx := context.Background()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	resp, err := wh.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *workflowHandlerSuite) TestDescribeWorkflowExecution_RunningStatus() {
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

func (s *workflowHandlerSuite) TestDescribeWorkflowExecution_CompletedStatus() {
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

func (s *workflowHandlerSuite) TestListWorkflowExecutions() {
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

func (s *workflowHandlerSuite) TestScanWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.testNamespace).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetReadStoreName(s.testNamespace).Return(elasticsearch.PersistenceName).AnyTimes()

	query := "WorkflowId = 'wid'"
	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
		PageSize:  int32(config.VisibilityMaxPageSize(s.testNamespace.String())),
		Query:     query,
	}
	ctx := context.Background()

	// page size <= 0 => max page size = 1000
	s.mockVisibilityMgr.EXPECT().ScanWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      config.VisibilityMaxPageSize(s.testNamespace.String()),
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	_, err := wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, scanRequest.GetQuery())

	// page size > 1000 => max page size = 1000
	s.mockVisibilityMgr.EXPECT().ScanWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      config.VisibilityMaxPageSize(s.testNamespace.String()),
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	scanRequest.PageSize = int32(config.VisibilityMaxPageSize(s.testNamespace.String())) + 1
	_, err = wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, scanRequest.GetQuery())

	// page size between 0 and 1000
	s.mockVisibilityMgr.EXPECT().ScanWorkflowExecutions(
		gomock.Any(),
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   s.testNamespaceID,
			Namespace:     s.testNamespace,
			PageSize:      10,
			NextPageToken: nil,
			Query:         query,
		},
	).Return(&manager.ListWorkflowExecutionsResponse{}, nil)
	scanRequest.PageSize = 10
	_, err = wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, scanRequest.GetQuery())
}

func (s *workflowHandlerSuite) TestCountWorkflowExecutions() {
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

func (s *workflowHandlerSuite) TestVerifyHistoryIsComplete() {
	events := make([]*historypb.HistoryEvent, 50)
	for i := 0; i < len(events); i++ {
		events[i] = &historypb.HistoryEvent{EventId: int64(i + 1)}
	}
	var eventsWithHoles []*historypb.HistoryEvent
	eventsWithHoles = append(eventsWithHoles, events[9:12]...)
	eventsWithHoles = append(eventsWithHoles, events[20:31]...)

	testCases := []struct {
		events       []*historypb.HistoryEvent
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
		err := e.VerifyHistoryIsComplete(tc.events, tc.firstEventID, tc.lastEventID, tc.isFirstPage, tc.isLastPage, tc.pageSize)
		if tc.isResultErr {
			s.Error(err, "testcase %v failed", i)
		} else {
			s.NoError(err, "testcase %v failed", i)
		}
	}
}

func (s *workflowHandlerSuite) TestGetSystemInfo() {
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
	// Nexus is enabled by a dynamic config feature flag which defaults to false.
	s.False(resp.Capabilities.Nexus)
}

func (s *workflowHandlerSuite) TestStartBatchOperation_Terminate() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	inputString := "unit test"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	params := &batcher.BatchParams{
		Namespace: testNamespace.String(),
		Reason:    inputString,
		BatchType: batcher.BatchTypeTerminate,
		Query:     inputString,
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
			s.Equal(batcher.BatchWFTypeName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.Equal(payload.EncodeString(batcher.BatchTypeTerminate), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.Equal(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
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

func (s *workflowHandlerSuite) TestStartBatchOperation_Cancellation() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	inputString := "unit test"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	params := &batcher.BatchParams{
		Namespace: testNamespace.String(),
		Reason:    inputString,
		BatchType: batcher.BatchTypeCancel,
		Query:     inputString,
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
			s.Equal(batcher.BatchWFTypeName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.Equal(payload.EncodeString(batcher.BatchTypeCancel), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.Equal(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
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

func (s *workflowHandlerSuite) TestStartBatchOperation_Signal() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	inputString := "unit test"
	signalName := "signal name"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	signalPayloads := payloads.EncodeString(signalName)
	params := &batcher.BatchParams{
		Namespace: testNamespace.String(),
		Query:     inputString,
		Reason:    inputString,
		BatchType: batcher.BatchTypeSignal,
		SignalParams: batcher.SignalParams{
			SignalName: signalName,
			Input:      signalPayloads,
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
			s.Equal(batcher.BatchWFTypeName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(inputString, request.StartRequest.Identity)
			s.Equal(payload.EncodeString(batcher.BatchTypeSignal), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.Equal(payload.EncodeString(inputString), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.Equal(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
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

func (s *workflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Signal() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		},
	}
	reason := "reason"
	identity := "identity"
	signalName := "signal name"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	signalPayloads := payloads.EncodeString(signalName)
	params := &batcher.BatchParams{
		Namespace:  testNamespace.String(),
		Executions: executions,
		Reason:     reason,
		BatchType:  batcher.BatchTypeSignal,
		SignalParams: batcher.SignalParams{
			SignalName: signalName,
			Input:      signalPayloads,
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
			s.Equal(batcher.BatchWFTypeName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(identity, request.StartRequest.Identity)
			s.Equal(payload.EncodeString(batcher.BatchTypeSignal), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.Equal(payload.EncodeString(reason), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.Equal(payload.EncodeString(identity), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.Equal(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   signalName,
				Input:    signalPayloads,
				Identity: identity,
			},
		},
		Reason:     reason,
		Executions: executions,
	}

	_, err = wh.StartBatchOperation(context.Background(), request)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_Reset() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		},
	}
	reason := "reason"
	identity := "identity"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	params := &batcher.BatchParams{
		Namespace:  testNamespace.String(),
		Executions: executions,
		Reason:     reason,
		BatchType:  batcher.BatchTypeReset,
		ResetParams: batcher.ResetParams{
			ResetType:        enumspb.RESET_TYPE_LAST_WORKFLOW_TASK,
			ResetReapplyType: enumspb.RESET_REAPPLY_TYPE_NONE,
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
			s.Equal(batcher.BatchWFTypeName, request.StartRequest.WorkflowType.Name)
			s.Equal(primitives.PerNSWorkerTaskQueue, request.StartRequest.TaskQueue.Name)
			s.Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, request.StartRequest.WorkflowIdReusePolicy)
			s.Equal(identity, request.StartRequest.Identity)
			s.Equal(payload.EncodeString(batcher.BatchTypeReset), request.StartRequest.Memo.Fields[batcher.BatchOperationTypeMemo])
			s.Equal(payload.EncodeString(reason), request.StartRequest.Memo.Fields[batcher.BatchReasonMemo])
			s.Equal(payload.EncodeString(identity), request.StartRequest.SearchAttributes.IndexedFields[searchattribute.BatcherUser])
			s.Equal(inputPayload, request.StartRequest.Input)
			return &historyservice.StartWorkflowExecutionResponse{}, nil
		},
	)
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil)
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
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
func (s *workflowHandlerSuite) TestStartBatchOperation_WorkflowExecutions_TooMany() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	executions := []*commonpb.WorkflowExecution{
		{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		},
	}
	reason := "reason"
	identity := "identity"
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()
	// Simulate std visibility, which does not support CountWorkflowExecutions
	// TODO: remove this once every visibility implementation supports CountWorkflowExecutions
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(nil, store.OperationNotSupportedErr)
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(
		func(
			_ context.Context,
			request *manager.ListWorkflowExecutionsRequestV2,
		) (*manager.ListWorkflowExecutionsResponse, error) {
			s.Equal(testNamespace, request.Namespace)
			s.True(strings.Contains(request.Query, searchattribute.WorkflowType))
			s.Equal(int(config.MaxConcurrentBatchOperation(testNamespace.String())), request.PageSize)
			s.Equal([]byte{}, request.NextPageToken)
			return &manager.ListWorkflowExecutionsResponse{
				Executions: []*workflowpb.WorkflowExecutionInfo{
					{
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: testWorkflowID,
							RunId:      testRunID,
						},
					},
				},
				NextPageToken: nil,
			}, nil
		},
	)

	request := &workflowservice.StartBatchOperationRequest{
		Namespace: testNamespace.String(),
		JobId:     uuid.New(),
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

func (s *workflowHandlerSuite) TestStartBatchOperation_InvalidRequest() {
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.New(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   "signalName",
				Identity: "identity",
			},
		},
		Reason:          uuid.New(),
		VisibilityQuery: uuid.New(),
	}

	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.New()
	request.JobId = ""
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.JobId = uuid.New()
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

	request.Reason = uuid.New()
	request.VisibilityQuery = ""
	_, err = wh.StartBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *workflowHandlerSuite) TestStopBatchOperation() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	jobID := uuid.New()
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

func (s *workflowHandlerSuite) TestStopBatchOperation_InvalidRequest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	request := &workflowservice.StopBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.New(),
		Reason:    "reason",
	}

	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.New()
	request.JobId = ""
	_, err = wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.JobId = uuid.New()
	request.Reason = ""
	_, err = wh.StopBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *workflowHandlerSuite) TestDescribeBatchOperation_CompletedStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	jobID := uuid.New()
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

func (s *workflowHandlerSuite) TestDescribeBatchOperation_RunningStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	jobID := uuid.New()
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

func (s *workflowHandlerSuite) TestDescribeBatchOperation_FailedStatus() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	jobID := uuid.New()
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

func (s *workflowHandlerSuite) TestDescribeBatchOperation_InvalidRequest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	request := &workflowservice.DescribeBatchOperationRequest{
		Namespace: "",
		JobId:     uuid.New(),
	}
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.DescribeBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)

	request.Namespace = uuid.New()
	request.JobId = ""
	_, err = wh.DescribeBatchOperation(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *workflowHandlerSuite) TestListBatchOperations() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	jobID := uuid.New()
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

func (s *workflowHandlerSuite) TestListBatchOperations_InvalidRerquest() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	request := &workflowservice.ListBatchOperationsRequest{
		Namespace: "",
	}
	var invalidArgumentErr *serviceerror.InvalidArgument
	_, err := wh.ListBatchOperations(context.Background(), request)
	s.ErrorAs(err, &invalidArgumentErr)
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNoopClient(), s.mockResource.GetLogger()), numHistoryShards)
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

	req.RequestId = "\x87\x01"
	err = validateRequestId(&req.RequestId, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid UTF-8 string")
}

func (s *workflowHandlerSuite) Test_DeleteWorkflowExecution() {
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

func (s *workflowHandlerSuite) Test_ValidateTaskQueue() {
	wh := s.getWorkflowHandler(s.newConfig())

	tq := taskqueuepb.TaskQueue{Name: "\x87\x01"}
	err := wh.validateTaskQueue(&tq)
	s.Error(err)
	s.Contains(err.Error(), "is not a valid UTF-8 string")

	tq = taskqueuepb.TaskQueue{Name: "valid-tq-name"}
	err = wh.validateTaskQueue(&tq)
	s.NoError(err)

	tq = taskqueuepb.TaskQueue{Name: "valid-tq-name", NormalName: "\x87\x01", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
	err = wh.validateTaskQueue(&tq)
	s.Error(err)
	s.Contains(err.Error(), "is not a valid UTF-8 string")
}

func (s *workflowHandlerSuite) TestExecuteMultiOperation() {
	ctx := context.Background()
	config := s.newConfig()
	config.EnableExecuteMultiOperation = func(string) bool { return true }
	config.EnableUpdateWorkflowExecution = func(string) bool { return true }
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
		s.Equal("MultiOperation could not be executed.", actual.Error())
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
