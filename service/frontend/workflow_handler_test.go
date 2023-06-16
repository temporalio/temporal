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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
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
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker/batcher"
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

	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
	return NewWorkflowHandler(
		config,
		s.mockProducer,
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetLogger(),
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
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
			EarliestTime: timestamp.TimePtr(time.Time{}),
			LatestTime:   timestamp.TimePtr(time.Now().UTC()),
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
			EarliestTime: timestamp.TimePtr(time.Time{}),
			LatestTime:   timestamp.TimePtr(time.Now().UTC()),
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

func (s *workflowHandlerSuite) TestTransientTaskInjection() {
	cfg := s.newConfig()
	baseEvents := []*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	}

	// Needed to execute test but not relevant
	s.mockSearchAttributesProvider.EXPECT().
		GetSearchAttributes(esIndexName, false).
		Return(searchattribute.NameTypeMap{}, nil).
		AnyTimes()

	// Install a test namespace into mock namespace registry
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   s.testNamespaceID.String(),
			Name: s.testNamespace.String(),
		},
		&persistencespb.NamespaceConfig{},
		"target-cluster:not-relevant-to-this-test",
	)
	s.mockNamespaceCache.EXPECT().GetNamespace(s.testNamespace).Return(ns, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(ns, nil).AnyTimes()

	// History read will return a base set of non-transient events from baseEvents above
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).
		Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: baseEvents}, nil).
		AnyTimes()

	pollRequest := workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.testNamespace.String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "taskqueue:" + s.T().Name()},
	}

	for _, tc := range []struct {
		name           string
		taskInfo       historyspb.TransientWorkflowTaskInfo
		transientCount int
	}{
		{
			name: "HistorySuffix",
			taskInfo: historyspb.TransientWorkflowTaskInfo{
				HistorySuffix: []*historypb.HistoryEvent{
					{EventId: 3},
					{EventId: 4},
					{EventId: 5},
					{EventId: 6},
				},
			},
			transientCount: 4,
		},
	} {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
			defer cancel()
			s.mockMatchingClient.EXPECT().PollWorkflowTaskQueue(ctx, gomock.Any()).Return(
				&matchingservice.PollWorkflowTaskQueueResponse{
					NextEventId: int64(len(baseEvents) + 1),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: "wfid:" + s.T().Name(),
						RunId:      "1",
					},
					TransientWorkflowTask: &tc.taskInfo,
				},
				nil,
			)

			wh := s.getWorkflowHandler(cfg)
			pollResp, err := wh.PollWorkflowTaskQueue(ctx, &pollRequest)

			s.NoError(err)
			events := pollResp.GetHistory().GetEvents()
			s.Len(events, len(baseEvents)+tc.transientCount)
		})
	}
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

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_RequestIdNotSet() {
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
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
			MaximumAttempts:    1,
		},
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errRequestIDNotSet, err)
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(time.Duration(-1) * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(time.Duration(-1) * time.Second),
		RetryPolicy:              &commonpb.RetryPolicy{},
		RequestId:                uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(&commonpb.RetryPolicy{
		BackoffCoefficient: 2.0,
		InitialInterval:    timestamp.DurationPtr(time.Second),
		MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(time.Duration(-1) * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId:          uuid.New(),
		CronSchedule:       "dummy-cron-schedule",
		WorkflowStartDelay: timestamp.DurationPtr(10 * time.Second),
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
		WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
		WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout:      timestamp.DurationPtr(time.Duration(-1) * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    timestamp.DurationPtr(2 * time.Second),
			MaximumAttempts:    1,
		},
		RequestId:          uuid.New(),
		WorkflowStartDelay: timestamp.DurationPtr(-10 * time.Second),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.ErrorIs(err, errInvalidWorkflowStartDelaySeconds)
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

func (s *workflowHandlerSuite) TestGetHistory() {
	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("test-namespace")
	firstEventID := int64(100)
	nextEventID := int64(102)
	branchToken := []byte{1}
	we := commonpb.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      2,
		NextPageToken: []byte{},
		ShardID:       shardID,
	}
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(100),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			{
				EventId:   int64(101),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"CustomKeywordField":    payload.EncodeString("random-keyword"),
								"TemporalChangeVersion": payload.EncodeString("random-data"),
							},
						},
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(namespaceName).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	history, token, err := wh.getHistory(
		context.Background(),
		metrics.NoopMetricsHandler,
		namespaceID,
		namespaceName,
		we,
		firstEventID,
		nextEventID,
		2,
		[]byte{},
		nil,
		branchToken,
	)
	s.NoError(err)
	s.NotNil(history)
	s.Equal([]byte{}, token)

	s.EqualValues("Keyword", history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["AliasForCustomKeywordField"].GetMetadata()["type"])
	s.EqualValues(`"random-data"`, history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["TemporalChangeVersion"].GetData())
}

func (s *workflowHandlerSuite) TestGetWorkflowExecutionHistory() {
	namespaceID := namespace.ID(uuid.New())
	namespace := namespace.Name("namespace")
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}
	newRunID := uuid.New()

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              namespace.String(),
		Execution:              &we,
		MaximumPageSize:        10,
		NextPageToken:          nil,
		WaitNewEvent:           true,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		SkipArchival:           true,
	}

	// set up mocks to simulate a failed workflow with a retry policy. the failure event is id 5.
	branchToken := []byte{1, 2, 3}
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(namespace).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().PollMutableState(gomock.Any(), &historyservice.PollMutableStateRequest{
		NamespaceId:         namespaceID.String(),
		Execution:           &we,
		ExpectedNextEventId: common.EndEventID,
		CurrentBranchToken:  nil,
	}).Return(&historyservice.PollMutableStateResponse{
		Execution:           &we,
		WorkflowType:        &commonpb.WorkflowType{Name: "mytype"},
		NextEventId:         6,
		LastFirstEventId:    5,
		CurrentBranchToken:  branchToken,
		VersionHistories:    nil,
		WorkflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		WorkflowStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		LastFirstEventTxnId: 100,
	}, nil).Times(2)

	// GetWorkflowExecutionHistory will request the last event
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    5,
		MaxEventID:    6,
		PageSize:      10,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
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
		NextPageToken: []byte{},
		Size:          1,
	}, nil).Times(2)

	s.mockExecutionManager.EXPECT().TrimHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

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

func (s *workflowHandlerSuite) TestGetWorkflowExecutionHistory_RawHistoryWithTransientDecision() {
	namespaceID := namespace.ID(uuid.New())
	namespace := namespace.Name("namespace")
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}

	config := s.newConfig()
	config.SendRawWorkflowHistory = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)

	branchToken := []byte{1, 2, 3}
	persistenceToken := []byte("some random persistence token")
	nextPageToken, err := serializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            we.GetRunId(),
		FirstEventId:     common.FirstEventID,
		NextEventId:      5,
		PersistenceToken: persistenceToken,
		TransientWorkflowTask: &historyspb.TransientWorkflowTaskInfo{
			HistorySuffix: []*historypb.HistoryEvent{
				{
					EventId:   5,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   6,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
		},
		BranchToken: branchToken,
	})
	s.NoError(err)
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              namespace.String(),
		Execution:              &we,
		MaximumPageSize:        10,
		NextPageToken:          nextPageToken,
		WaitNewEvent:           false,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		SkipArchival:           true,
	}

	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(namespace).Return(namespaceID, nil).AnyTimes()

	historyBlob1, err := wh.payloadSerializer.SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	historyBlob2, err := wh.payloadSerializer.SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(4),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	s.mockExecutionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    1,
		MaxEventID:    5,
		PageSize:      10,
		NextPageToken: persistenceToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{historyBlob1, historyBlob2},
		NextPageToken:     []byte{},
		Size:              1,
	}, nil).Times(1)

	resp, err := wh.GetWorkflowExecutionHistory(context.Background(), req)
	s.NoError(err)
	s.False(resp.Archived)
	s.Empty(resp.History.Events)
	s.Len(resp.RawHistory, 4)
	event, err := wh.payloadSerializer.DeserializeEvent(resp.RawHistory[2])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
	event, err = wh.payloadSerializer.DeserializeEvent(resp.RawHistory[3])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, event.EventType)
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
	now := timestamp.TimePtr(time.Now())

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
	now := timestamp.TimePtr(time.Now())

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
	wh := s.getWorkflowHandler(s.newConfig())

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
		err := wh.verifyHistoryIsComplete(tc.events, tc.firstEventID, tc.lastEventID, tc.isFirstPage, tc.isLastPage, tc.pageSize)
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
	s.mockVisibilityMgr.EXPECT().ListOpenWorkflowExecutionsByType(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(
		func(
			_ context.Context,
			request *manager.ListWorkflowExecutionsByTypeRequest,
		) (*manager.ListWorkflowExecutionsResponse, error) {
			s.Equal(testNamespace, request.Namespace)
			s.Equal(batcher.BatchWFTypeName, request.WorkflowTypeName)
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
	now := timestamp.TimePtr(time.Now())
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
	now := timestamp.TimePtr(time.Now())
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
	now := timestamp.TimePtr(time.Now())
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
	now := timestamp.TimePtr(time.Now())
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
	return NewConfig(dc.NewCollection(dc.NewNoopClient(), s.mockResource.GetLogger()), numHistoryShards, true, false)
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
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(10 * time.Hour * 24),
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
