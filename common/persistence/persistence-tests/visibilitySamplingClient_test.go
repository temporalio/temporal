package persistencetests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	mmocks "github.com/temporalio/temporal/common/metrics/mocks"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
	c "github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
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
	testWorkflowExecution = executionpb.WorkflowExecution{
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
		Status:           executionpb.WorkflowExecutionStatus_Completed,
	}
	request2 := &p.RecordWorkflowExecutionClosedRequest{
		NamespaceID:      testNamespaceUUID,
		Namespace:        testNamespace,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           executionpb.WorkflowExecutionStatus_Failed,
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
		Status:                        executionpb.WorkflowExecutionStatus_Failed,
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
