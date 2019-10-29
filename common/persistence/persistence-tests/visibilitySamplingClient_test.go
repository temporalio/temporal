// Copyright (c) 2017 Uber Technologies, Inc.
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

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	c "github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type VisibilitySamplingSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	client       p.VisibilityManager
	persistence  *mocks.VisibilityManager
	metricClient *mmocks.Client
}

var (
	testDomainUUID        = "fb15e4b5-356f-466d-8c6d-a29223e5c536"
	testDomain            = "test-domain-name"
	testWorkflowExecution = gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
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
		VisibilityOpenMaxQPS:   dynamicconfig.GetIntPropertyFilteredByDomain(1),
		VisibilityClosedMaxQPS: dynamicconfig.GetIntPropertyFilteredByDomain(10),
		VisibilityListMaxQPS:   dynamicconfig.GetIntPropertyFilteredByDomain(1),
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
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
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
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           gen.WorkflowExecutionCloseStatusCompleted,
	}
	request2 := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           gen.WorkflowExecutionCloseStatusFailed,
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
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	s.persistence.On("ListOpenWorkflowExecutions", request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutions(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutions(request)
	s.Error(err)
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutions() {
	request := &p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	s.persistence.On("ListClosedWorkflowExecutions", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutions(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutions(request)
	s.Error(err)
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByType() {
	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
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
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByType() {
	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
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
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
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
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
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
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByStatus() {
	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: req,
		Status:                        gen.WorkflowExecutionCloseStatusFailed,
	}
	s.persistence.On("ListClosedWorkflowExecutionsByStatus", request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByStatus(request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByStatus(request)
	s.Error(err)
	errDetail, ok := err.(*gen.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}
