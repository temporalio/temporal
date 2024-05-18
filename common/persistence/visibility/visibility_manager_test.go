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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
)

type VisibilityManagerSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	controller *gomock.Controller

	visibilityManager manager.VisibilityManager
	visibilityStore   *store.MockVisibilityStore
	metricsHandler    *metrics.MockHandler
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
	s.visibilityStore.EXPECT().GetName().Return(mysql.PluginName).AnyTimes()
	s.visibilityStore.EXPECT().GetIndexName().Return("test-index-name").AnyTimes()
	s.metricsHandler = metrics.NewMockHandler(s.controller)
	s.visibilityManager = newVisibilityManager(
		s.visibilityStore,
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetFloatPropertyFn(0.2),
		s.metricsHandler,
		metrics.VisibilityPluginNameTag(s.visibilityStore.GetName()),
		log.NewNoopLogger())
}

func (s *VisibilityManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *VisibilityManagerSuite) TestRecordWorkflowExecutionStarted() {
	startTime := time.Now().UTC()
	executionTime := startTime.Add(1 * time.Minute)
	request := &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Namespace:        testNamespace,
			Execution:        &testWorkflowExecution,
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	memoBlob, err := serializeMemo(request.Memo)
	s.NoError(err)

	s.visibilityStore.EXPECT().RecordWorkflowExecutionStarted(
		gomock.Any(),
		&store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      request.NamespaceID.String(),
				WorkflowID:       request.Execution.GetWorkflowId(),
				RunID:            request.Execution.GetRunId(),
				WorkflowTypeName: request.WorkflowTypeName,
				StartTime:        request.StartTime,
				ExecutionTime:    request.ExecutionTime,
				Status:           request.Status,
				Memo:             memoBlob,
			},
		},
	).Return(nil)
	s.metricsHandler.EXPECT().
		WithTags(
			metrics.OperationTag(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope),
			metrics.VisibilityPluginNameTag(s.visibilityStore.GetName()),
		).
		Return(metrics.NoopMetricsHandler).Times(2)
	s.NoError(s.visibilityManager.RecordWorkflowExecutionStarted(context.Background(), request))

	// no remaining tokens
	err = s.visibilityManager.RecordWorkflowExecutionStarted(context.Background(), request)
	s.Error(err)
	s.ErrorIs(err, persistence.ErrPersistenceLimitExceeded)
}

func (s *VisibilityManagerSuite) TestRecordWorkflowExecutionClosed() {
	startTime := time.Now().UTC()
	executionTime := startTime.Add(1 * time.Minute)
	closeTime := startTime.Add(2 * time.Minute)
	request := &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Namespace:        testNamespace,
			Execution:        &testWorkflowExecution,
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime:         closeTime,
		ExecutionDuration: closeTime.Sub(executionTime),
	}

	memoBlob, err := serializeMemo(request.Memo)
	s.NoError(err)

	s.visibilityStore.EXPECT().RecordWorkflowExecutionClosed(
		gomock.Any(),
		&store.InternalRecordWorkflowExecutionClosedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      request.NamespaceID.String(),
				WorkflowID:       request.Execution.GetWorkflowId(),
				RunID:            request.Execution.GetRunId(),
				WorkflowTypeName: request.WorkflowTypeName,
				StartTime:        request.StartTime,
				ExecutionTime:    request.ExecutionTime,
				Status:           request.Status,
				Memo:             memoBlob,
			},
			CloseTime:         request.CloseTime,
			ExecutionDuration: request.ExecutionDuration,
		},
	).Return(nil)
	s.metricsHandler.EXPECT().
		WithTags(metrics.OperationTag(
			metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope),
			metrics.VisibilityPluginNameTag(s.visibilityStore.GetName()),
		).
		Return(metrics.NoopMetricsHandler).Times(2)
	s.NoError(s.visibilityManager.RecordWorkflowExecutionClosed(context.Background(), request))

	err = s.visibilityManager.RecordWorkflowExecutionClosed(context.Background(), request)
	s.Error(err)
	s.ErrorIs(err, persistence.ErrPersistenceLimitExceeded)
}

func (s *VisibilityManagerSuite) TestGetWorkflowExecution() {
	request := &manager.GetWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
		RunID:       testWorkflowExecution.RunId,
		WorkflowID:  testWorkflowExecution.WorkflowId,
	}
	s.visibilityStore.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&store.InternalGetWorkflowExecutionResponse{},
		nil,
	)
	s.metricsHandler.EXPECT().
		WithTags(
			metrics.OperationTag(metrics.VisibilityPersistenceGetWorkflowExecutionScope),
			metrics.VisibilityPluginNameTag(s.visibilityStore.GetName()),
		).
		Return(metrics.NoopMetricsHandler).Times(2)
	_, err := s.visibilityManager.GetWorkflowExecution(context.Background(), request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.visibilityManager.GetWorkflowExecution(context.Background(), request)
	s.Equal(persistence.ErrPersistenceLimitExceeded, err)
}
