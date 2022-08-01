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
	"testing"
	"time"

	"go.temporal.io/server/common/clock"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/health"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
)

const (
	numHistoryShards = 10

	testWorkflowID            = "test-workflow-id"
	testRunID                 = "test-run-id"
	testHistoryArchivalURI    = "testScheme://history/URI"
	testVisibilityArchivalURI = "testScheme://visibility/URI"
)

type (
	workflowHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller                   *gomock.Controller
		mockResource                 *resource.Test
		mockNamespaceCache           *namespace.MockRegistry
		mockHistoryClient            *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata          *cluster.MockMetadata
		mockSearchAttributesProvider *searchattribute.MockProvider
		mockSearchAttributesMapper   *searchattribute.MockMapper
		mockMatchingClient           *matchingservicemock.MockMatchingServiceClient

		mockProducer           *persistence.MockNamespaceReplicationQueue
		mockMetadataMgr        *persistence.MockMetadataManager
		mockExecutionManager   *persistence.MockExecutionManager
		mockVisibilityMgr      *manager.MockVisibilityManager
		mockArchivalMetadata   *archiver.MockArchivalMetadata
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
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockSearchAttributesProvider = s.mockResource.SearchAttributesProvider
	s.mockSearchAttributesMapper = s.mockResource.SearchAttributesMapper
	s.mockMetadataMgr = s.mockResource.MetadataMgr
	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)
	s.mockArchivalMetadata = s.mockResource.ArchivalMetadata
	s.mockArchiverProvider = s.mockResource.ArchiverProvider
	s.mockMatchingClient = s.mockResource.MatchingClient

	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockHistoryArchiver = archiver.NewMockHistoryArchiver(s.controller)
	s.mockVisibilityArchiver = archiver.NewMockVisibilityArchiver(s.controller)

	s.tokenSerializer = common.NewProtoTaskTokenSerializer()

	mockMonitor := s.mockResource.MembershipMonitor
	mockMonitor.EXPECT().GetMemberCount(common.FrontendServiceName).Return(5, nil).AnyTimes()
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	return NewWorkflowHandler(
		config,
		s.mockProducer,
		s.mockVisibilityMgr,
		s.mockResource.Logger,
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetMatchingClient(),
		s.mockResource.GetArchiverProvider(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesMapper(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		health.NewServer(),
		clock.NewRealTimeSource(),
	)
}

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	testNamespace := namespace.Name("test-namespace")
	namespaceID := namespace.ID(uuid.New())
	config := s.newConfig()
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByNamespace(true)

	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()

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
		&historypb.HistoryEvent{EventId: 1},
		&historypb.HistoryEvent{EventId: 2},
	}

	// Needed to execute test but not relevant
	s.mockSearchAttributesProvider.EXPECT().
		GetSearchAttributes(cfg.ESIndexName, false).
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
			name: "Legacy",
			taskInfo: historyspb.TransientWorkflowTaskInfo{
				ScheduledEvent: &historypb.HistoryEvent{EventId: 3},
				StartedEvent:   &historypb.HistoryEvent{EventId: 4},
			},
			transientCount: 2,
		},
		{
			name: "HistorySuffix",
			taskInfo: historyspb.TransientWorkflowTaskInfo{
				HistorySuffix: []*historypb.HistoryEvent{
					&historypb.HistoryEvent{EventId: 3},
					&historypb.HistoryEvent{EventId: 4},
					&historypb.HistoryEvent{EventId: 5},
					&historypb.HistoryEvent{EventId: 6},
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
	wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
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
	wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random history URI"},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random visibility URI"},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
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
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
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
	namespace := namespace.Name("namespace")
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
	s.mockSearchAttributesMapper.EXPECT().GetAlias("CustomKeywordField", namespace.String()).Return("AliasOfCustomKeyword", nil)

	wh := s.getWorkflowHandler(s.newConfig())

	history, token, err := wh.getHistory(
		context.Background(),
		metrics.NoopScope,
		namespaceID,
		namespace,
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

	s.EqualValues("Keyword", history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["AliasOfCustomKeyword"].GetMetadata()["type"])
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
			ScheduledEvent: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			StartedEvent: &historypb.HistoryEvent{
				EventId:   6,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
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

func (s *workflowHandlerSuite) TestListWorkflowExecutions() {
	config := s.newConfig()
	config.EnableReadVisibilityFromES = dc.GetBoolPropertyFnFilteredByNamespace(true)

	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.ListWorkflowExecutionsResponse{}, nil)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
		PageSize:  int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	listRequest.Query = query
	_, err := wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	listRequest.PageSize = int32(config.ESIndexMaxResultWindow() + 1)
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestScanWorkflowExecutions() {
	config := s.newConfig()
	config.EnableReadVisibilityFromES = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().ScanWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.ListWorkflowExecutionsResponse{}, nil)

	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
		PageSize:  int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	scanRequest.Query = query
	_, err := wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, scanRequest.GetQuery())

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.testNamespace.String(),
		PageSize:  int32(config.ESIndexMaxResultWindow() + 1),
		Query:     query,
	}
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestCountWorkflowExecutions() {
	config := s.newConfig()
	config.EnableReadVisibilityFromES = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)

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
	config := s.newConfig()
	config.EnableReadVisibilityFromES = dc.GetBoolPropertyFnFilteredByNamespace(true)

	wh := s.getWorkflowHandler(config)

	resp, err := wh.GetSystemInfo(context.Background(), &workflowservice.GetSystemInfoRequest{})
	s.NoError(err)
	s.Equal(headers.ServerVersion, resp.ServerVersion)
	s.True(resp.Capabilities.SignalAndQueryHeader)
	s.True(resp.Capabilities.InternalErrorDifferentiation)
	s.True(resp.Capabilities.ActivityFailureIncludeHeartbeat)
	s.True(resp.Capabilities.SupportsSchedules)
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNoopClient(), s.mockResource.GetLogger()), numHistoryShards, "", false)
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

func persistenceGetNamespaceResponse(historyArchivalState, visibilityArchivalState *namespace.ArchivalState) *persistence.GetNamespaceResponse {
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

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*500)
	assert.True(t, contextNearDeadline(ctx, longPollTailRoom))
	assert.False(t, contextNearDeadline(ctx, time.Millisecond))

	assert.False(t, common.IsServiceTransientError(errContextNearDeadline))
}
