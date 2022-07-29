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

package visibility

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
)

type VisibilityManagerSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	controller *gomock.Controller

	visibilityManager manager.VisibilityManager
	visibilityStore   *store.MockVisibilityStore
	metricClient      *metrics.MockClient
}

var (
	testNamespaceUUID     = namespace.ID("fb15e4b5-356f-466d-8c6d-a29223e5c536")
	testNamespace         = namespace.Name("test-namespace")
	testWorkflowExecution = commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	testWorkflowTypeName = "visibility-workflow"
)

func TestVisibilityManagerSuite(t *testing.T) {
	suite.Run(t, new(VisibilityManagerSuite))
}

func (s *VisibilityManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.controller = gomock.NewController(s.T())
	s.visibilityStore = store.NewMockVisibilityStore(s.controller)
	s.metricClient = metrics.NewMockClient(s.controller)
	s.visibilityManager = newVisibilityManager(
		s.visibilityStore,
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetIntPropertyFn(1),
		s.metricClient,
		metrics.StandardVisibilityTypeTag(),
		log.NewNoopLogger())
}

func (s *VisibilityManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *VisibilityManagerSuite) TestRecordWorkflowExecutionStarted() {
	request := &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Namespace:        testNamespace,
			Execution:        testWorkflowExecution,
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        time.Now().UTC(),
		},
	}
	s.visibilityStore.EXPECT().RecordWorkflowExecutionStarted(gomock.Any(), gomock.Any()).Return(nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	s.NoError(s.visibilityManager.RecordWorkflowExecutionStarted(context.Background(), request))

	// no remaining tokens
	err := s.visibilityManager.RecordWorkflowExecutionStarted(context.Background(), request)
	s.Error(err)
	s.ErrorIs(err, persistence.ErrPersistenceLimitExceeded)
}

func (s *VisibilityManagerSuite) TestRecordWorkflowExecutionClosed() {
	request := &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Namespace:        testNamespace,
			Execution:        testWorkflowExecution,
			WorkflowTypeName: testWorkflowTypeName,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}

	s.visibilityStore.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).Return(nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	s.NoError(s.visibilityManager.RecordWorkflowExecutionClosed(context.Background(), request))

	err := s.visibilityManager.RecordWorkflowExecutionClosed(context.Background(), request)
	s.Error(err)
	s.ErrorIs(err, persistence.ErrPersistenceLimitExceeded)
}

func (s *VisibilityManagerSuite) TestListOpenWorkflowExecutions() {
	request := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	s.visibilityStore.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListOpenWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListOpenWorkflowExecutions(context.Background(), request)
	s.Error(err)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListClosedWorkflowExecutions() {
	request := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	s.visibilityStore.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListClosedWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListClosedWorkflowExecutions(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListOpenWorkflowExecutionsByType() {
	req := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &manager.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.visibilityStore.EXPECT().ListOpenWorkflowExecutionsByType(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListOpenWorkflowExecutionsByType(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListOpenWorkflowExecutionsByType(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListClosedWorkflowExecutionsByType() {
	req := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &manager.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.visibilityStore.EXPECT().ListClosedWorkflowExecutionsByType(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListClosedWorkflowExecutionsByType(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListClosedWorkflowExecutionsByType(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	req := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &manager.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowId(),
	}
	s.visibilityStore.EXPECT().ListOpenWorkflowExecutionsByWorkflowID(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListOpenWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListOpenWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	req := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &manager.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowId(),
	}
	s.visibilityStore.EXPECT().ListClosedWorkflowExecutionsByWorkflowID(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListClosedWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListClosedWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}

func (s *VisibilityManagerSuite) TestListClosedWorkflowExecutionsByStatus() {
	req := &manager.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &manager.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: req,
		Status:                        enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}
	s.visibilityStore.EXPECT().ListClosedWorkflowExecutionsByStatus(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricClient.EXPECT().Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope, metrics.StandardVisibilityTypeTag()).Return(metrics.NoopScope).Times(2)
	_, err := s.visibilityManager.ListClosedWorkflowExecutionsByStatus(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.ListClosedWorkflowExecutionsByStatus(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}
