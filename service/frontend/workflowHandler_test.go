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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
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

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockNamespaceCache  *cache.MockNamespaceCache
		mockHistoryClient   *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata *cluster.MockMetadata
		mockMatchingClient  *matchingservicemock.MockMatchingServiceClient

		mockProducer           *persistence.MockNamespaceReplicationQueue
		mockMetadataMgr        *persistence.MockMetadataManager
		mockHistoryMgr         *persistence.MockHistoryManager
		mockVisibilityMgr      *persistence.MockVisibilityManager
		mockArchivalMetadata   *archiver.MockArchivalMetadata
		mockArchiverProvider   *provider.MockArchiverProvider
		mockHistoryArchiver    *archiver.MockHistoryArchiver
		mockVisibilityArchiver *archiver.MockVisibilityArchiver

		tokenSerializer common.TaskTokenSerializer

		testNamespace   string
		testNamespaceID string
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
	s.mockMetadataMgr = s.mockResource.MetadataMgr
	s.mockHistoryMgr = s.mockResource.HistoryMgr
	s.mockVisibilityMgr = s.mockResource.VisibilityMgr
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
	return NewWorkflowHandler(s.mockResource, config, s.mockProducer).(*WorkflowHandler)
}

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	testNamespace := "test-namespace"
	namespaceID := uuid.New()
	config := s.newConfig()
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByNamespace(true)

	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceID, nil).AnyTimes()

	// test list open by wid
	listRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace: testNamespace,
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
	s.Equal(errNoPermission, err)

	// test list open by workflow type
	listRequest.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list close by wid
	listRequest2 := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace: testNamespace,
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
	s.Equal(errNoPermission, err)

	// test list close by workflow type
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list close by workflow status
	failedStatus := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_StatusFilter{StatusFilter: &filterpb.StatusFilter{Status: failedStatus}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errNoPermission, err)
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

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
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
	s.Equal(errNamespaceNotSet, err)
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any()).Return(&persistence.CreateNamespaceResponse{
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any()).Return(&persistence.CreateNamespaceResponse{
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any()).Return(&persistence.CreateNamespaceResponse{
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any()).Return(&persistence.CreateNamespaceResponse{
		ID: testNamespaceID,
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerNamespaceRequest(enumspb.ARCHIVAL_STATE_UNSPECIFIED, "", enumspb.ARCHIVAL_STATE_UNSPECIFIED, "")
	_, err := wh.RegisterNamespace(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestDescribeNamespace_Success_ArchivalDisabled() {
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)

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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)

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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any()).Return(nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random history URI"},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: "some random visibility URI"},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any()).Return(nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testHistoryArchivalURI},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_ENABLED, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
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
	s.mockMetadataMgr.EXPECT().GetMetadata().Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getNamespaceResp := persistenceGetNamespaceResponse(
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
		&namespace.ArchivalState{State: enumspb.ARCHIVAL_STATE_DISABLED, URI: ""},
	)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any()).Return(getNamespaceResp, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any()).Return(nil)
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
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:      "",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"",
		nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidURI() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      "uri without scheme",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"",
		nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testNamespaceID)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      testHistoryArchivalURI,
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		"",
		nil)
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
	namespaceID := uuid.New()
	firstEventID := int64(100)
	nextEventID := int64(101)
	branchToken := []byte{1}
	we := commonpb.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, we.WorkflowId, numHistoryShards)
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: []byte{},
		ShardID:       shardID,
	}
	s.mockHistoryMgr.EXPECT().ReadHistoryBranch(req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId: int64(100),
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	history, token, err := wh.getHistory(
		metrics.NoopScope(metrics.Frontend),
		namespaceID,
		we,
		firstEventID,
		nextEventID,
		1,
		[]byte{},
		nil,
		branchToken,
	)
	s.NoError(err)
	s.NotNil(history)
	s.Equal([]byte{}, token)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidRequest() {
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
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(cache.NewLocalNamespaceCacheEntryForTest(
		nil,
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
		},
		"",
		nil,
	), nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidURI() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(cache.NewLocalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "uri without scheme",
		},
		"",
		nil,
	), nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Success() {
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(cache.NewLocalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   testVisibilityArchivalURI,
		},
		"",
		nil,
	), nil).AnyTimes()
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI")).Times(2)
	s.mockVisibilityArchiver.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&archiver.QueryVisibilityResponse{}, nil)
	s.mockArchiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.NotNil(resp)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestGetSearchAttributes() {
	wh := s.getWorkflowHandler(s.newConfig())

	ctx := context.Background()
	resp, err := wh.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *workflowHandlerSuite) TestListWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().ListWorkflowExecutions(gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.testNamespace,
		PageSize:  int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	listRequest.Query = query
	_, err := wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	query = "InvalidKey = 'a'"
	listRequest.Query = query
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)

	listRequest.PageSize = int32(config.ESIndexMaxResultWindow() + 1)
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestScanWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().ScanWorkflowExecutions(gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil)

	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.testNamespace,
		PageSize:  int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	scanRequest.Query = query
	_, err := wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, scanRequest.GetQuery())

	query = "InvalidKey = 'a'"
	scanRequest.Query = query
	_, err = wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.Error(err)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.testNamespace,
		PageSize:  int32(config.ESIndexMaxResultWindow() + 1),
		Query:     query,
	}
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestCountWorkflowExecutions() {
	wh := s.getWorkflowHandler(s.newConfig())

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().CountWorkflowExecutions(gomock.Any()).Return(&persistence.CountWorkflowExecutionsResponse{}, nil)

	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.testNamespace,
	}
	ctx := context.Background()

	query := "WorkflowId = 'wid'"
	countRequest.Query = query
	_, err := wh.CountWorkflowExecutions(ctx, countRequest)
	s.NoError(err)
	s.Equal(query, countRequest.GetQuery())

	query = "InvalidKey = 'a'"
	countRequest.Query = query
	_, err = wh.CountWorkflowExecutions(ctx, countRequest)
	s.NotNil(err)
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

func (s *workflowHandlerSuite) TestTokenNamespaceEnforcementDisabled() {
	s.executeTokenTestCases("wrong-namespace", false, false, false)
}

func (s *workflowHandlerSuite) TestTokenNamespaceEnforcementEnabledMismatch() {
	s.executeTokenTestCases("wrong-namespace", true, true, true)
}

func (s *workflowHandlerSuite) TestTokenNamespaceEnforcementEnabledMatch() {
	s.executeTokenTestCases(s.testNamespace, true, false, false)
}

func (s *workflowHandlerSuite) executeTokenTestCases(tokenNamespace string, enforceNamespaceMatch bool,
	isErrorExpected bool, isNilExpected bool) {
	ctx := context.Background()
	wh := s.setupTokenNamespaceTest(tokenNamespace, enforceNamespaceMatch)

	req1 := s.newRespondActivityTaskCompletedRequest(uuid.New())
	resp1, err := wh.RespondActivityTaskCompleted(ctx, req1)
	s.checkResponse(err, resp1, isErrorExpected, isNilExpected)

	req2 := s.newRespondActivityTaskFailedRequest(uuid.New())
	resp2, err := wh.RespondActivityTaskFailed(ctx, req2)
	s.checkResponse(err, resp2, isErrorExpected, isNilExpected)

	req3 := s.newRespondActivityTaskCanceledRequest(uuid.New())
	resp3, err := wh.RespondActivityTaskCanceled(ctx, req3)
	s.checkResponse(err, resp3, isErrorExpected, isNilExpected)

	req4 := s.newRespondWorkflowTaskCompletedRequest(uuid.New())
	resp4, err := wh.RespondWorkflowTaskCompleted(ctx, req4)
	s.checkResponse(err, resp4, isErrorExpected, isNilExpected)

	req5 := s.newRespondWorkflowTaskFailedRequest(uuid.New())
	resp5, err := wh.RespondWorkflowTaskFailed(ctx, req5)
	s.checkResponse(err, resp5, isErrorExpected, isNilExpected)

	req6 := s.newRespondQueryTaskCompletedRequest(uuid.New())
	resp6, err := wh.RespondQueryTaskCompleted(ctx, req6)
	s.checkResponse(err, resp6, isErrorExpected, isNilExpected)
}

func (s *workflowHandlerSuite) checkResponse(err error, response interface{},
	isErrorExpected bool, isNilExpected bool) {
	if isErrorExpected {
		s.Error(err)
	} else {
		s.NoError(err)
	}
	if isNilExpected {
		s.Nil(response)
	} else {
		s.NotNil(response)
	}
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNoopClient(), s.mockResource.GetLogger()), numHistoryShards, false)
}

func (s *workflowHandlerSuite) newRespondActivityTaskCompletedRequest(tokenNamespaceId string) *workflowservice.RespondActivityTaskCompletedRequest {
	return &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newRespondActivityTaskFailedRequest(tokenNamespaceId string) *workflowservice.RespondActivityTaskFailedRequest {
	return &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newRespondActivityTaskCanceledRequest(tokenNamespaceId string) *workflowservice.RespondActivityTaskCanceledRequest {
	return &workflowservice.RespondActivityTaskCanceledRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newRespondWorkflowTaskCompletedRequest(tokenNamespaceId string) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newRespondWorkflowTaskFailedRequest(tokenNamespaceId string) *workflowservice.RespondWorkflowTaskFailedRequest {
	return &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newRespondQueryTaskCompletedRequest(tokenNamespaceId string) *workflowservice.RespondQueryTaskCompletedRequest {
	return &workflowservice.RespondQueryTaskCompletedRequest{
		Namespace: s.testNamespace,
		TaskToken: s.newSerializedQueryTaskToken(tokenNamespaceId),
	}
}

func (s *workflowHandlerSuite) newSerializedToken(namespaceId string) []byte {
	token, _ := s.tokenSerializer.Serialize(&tokenspb.Task{
		NamespaceId: namespaceId,
	})
	return token
}

func (s *workflowHandlerSuite) newSerializedQueryTaskToken(namespaceId string) []byte {
	token, _ := s.tokenSerializer.SerializeQueryTaskToken(&tokenspb.QueryTask{
		NamespaceId: namespaceId,
		TaskQueue:   "some-task-queue",
		TaskId:      "some-task-id",
	})
	return token
}

func newNamespaceCacheEntry(namespaceName string) *cache.NamespaceCacheEntry {
	info := &persistencespb.NamespaceInfo{
		Name: namespaceName,
	}
	return cache.NewLocalNamespaceCacheEntryForTest(info, nil, "", nil)
}

func (s *workflowHandlerSuite) setupTokenNamespaceTest(tokenNamespace string, enforce bool) *WorkflowHandler {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(newNamespaceCacheEntry(tokenNamespace), nil).AnyTimes()
	ctx := context.Background()
	s.mockHistoryClient.EXPECT().RespondActivityTaskCompleted(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().RespondActivityTaskFailed(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().RespondActivityTaskCanceled(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().RespondWorkflowTaskCompleted(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().RespondWorkflowTaskFailed(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().RespondQueryTaskCompleted(ctx, gomock.Any()).Return(nil, nil).AnyTimes()
	cfg := s.newConfig()
	cfg.EnableTokenNamespaceEnforcement = dc.GetBoolPropertyFn(enforce)
	return s.getWorkflowHandler(cfg)
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
		SecurityToken:           "token",
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
