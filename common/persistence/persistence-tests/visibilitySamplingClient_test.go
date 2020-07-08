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

package persistencetests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	mmocks "go.temporal.io/server/common/metrics/mocks"
	"go.temporal.io/server/common/mocks"
	p "go.temporal.io/server/common/persistence"
	c "go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type VisibilitySamplingSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	client       p.VisibilityManager
	persistence  *mocks.VisibilityManager
	metricClient *mmocks.Client
}

var (
	testNamespaceUUID     = "fb15e4b5-356f-466d-8c6d-a29223e5c536"
	testNamespace         = "test-namespace"
	testWorkflowExecution = commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	testWorkflowTypeName = "visibility-workflow"

	listErrMsg = "Persistence Max QPS Reached for List Operations."
)

func TestVisibilitySamplingSuite(t *testing.T) {
	suite.Run(t, new(VisibilitySamplingSuite))
}

func (s *VisibilitySamplingSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.persistence = &mocks.VisibilityManager{}
	config := &c.VisibilityConfig{
		VisibilityOpenMaxQPS:   dynamicconfig.GetIntPropertyFilteredByNamespace(1),
		VisibilityClosedMaxQPS: dynamicconfig.GetIntPropertyFilteredByNamespace(10),
		VisibilityListMaxQPS:   dynamicconfig.GetIntPropertyFilteredByNamespace(1),
	}
	s.metricClient = &mmocks.Client{}
	s.client = p.NewVisibilitySamplingClient(s.persistence, config, s.metricClient, loggerimpl.NewNopLogger())
}

func (s *VisibilitySamplingSuite) TearDownTest() {
	s.persistence.AssertExpectations(s.T())
	s.metricClient.AssertExpectations(s.T())
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionStarted() {
	request := &p.RecordWorkflowExecutionStartedRequest{
		NamespaceID:      testNamespaceUUID,
		Namespace:        testNamespace,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		StartTimestamp:   time.Now().UnixNano(),
	}
	s.persistence.On("RecordWorkflowExecutionStarted", request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(request))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(request))
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionClosed() {
	request := &p.RecordWorkflowExecutionClosedRequest{
		NamespaceID:      testNamespaceUUID,
		Namespace:        testNamespace,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}
	request2 := &p.RecordWorkflowExecutionClosedRequest{
		NamespaceID:      testNamespaceUUID,
		Namespace:        testNamespace,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	s.persistence.On("RecordWorkflowExecutionClosed", request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request))
	s.persistence.On("RecordWorkflowExecutionClosed", request2).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request2))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request))
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request2))
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutions() {
	request := &p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	s.persistence.On("ListOpenWorkflowExecutions", request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutions(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutions(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutions() {
	request := &p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	s.persistence.On("ListClosedWorkflowExecutions", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutions(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutions(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByType() {
	req := p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.persistence.On("ListOpenWorkflowExecutionsByType", request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutionsByType(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutionsByType(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByType() {
	req := p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.persistence.On("ListClosedWorkflowExecutionsByType", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByType(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByType(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	req := p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowId(),
	}
	s.persistence.On("ListOpenWorkflowExecutionsByWorkflowID", request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	req := p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowId(),
	}
	s.persistence.On("ListClosedWorkflowExecutionsByWorkflowID", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByStatus() {
	req := p.ListWorkflowExecutionsRequest{
		NamespaceID: testNamespaceUUID,
		Namespace:   testNamespace,
	}
	request := &p.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: req,
		Status:                        enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}
	s.persistence.On("ListClosedWorkflowExecutionsByStatus", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByStatus(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByStatus(request)
	s.Error(err)
	errDetail, ok := err.(*serviceerror.ResourceExhausted)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}
