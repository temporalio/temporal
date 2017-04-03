package persistence

import (
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	visibilityPersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestVisibilityPersistenceSuite(t *testing.T) {
	s := new(visibilityPersistenceSuite)
	suite.Run(t, s)
}

func (s *visibilityPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()
}

func (s *visibilityPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *visibilityPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *visibilityPersistenceSuite) TestBasicVisibility() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}

	startTime := time.Now().Add(time.Second * -5)
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTime:        startTime,
	})
	s.Nil(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		PageSize:   1,
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTime:        startTime,
		CloseTime:        time.Now(),
	})
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		PageSize:   1,
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		PageSize:   1,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
}

func (s *visibilityPersistenceSuite) TestVisibilityPagination() {
	testDomainUUID := uuid.New()
	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTime:        time.Now(),
	})
	s.Nil(err0)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test2"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTime:        time.Now(),
	})
	s.Nil(err1)

	// Get the first one
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		PageSize:   1,
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Use token to get the second one
	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:    testDomainUUID,
		PageSize:      1,
		NextPageToken: resp.NextPageToken,
	})
	s.Nil(err3)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution2.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Now should get empty result by using token
	resp, err4 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:    testDomainUUID,
		PageSize:      1,
		NextPageToken: resp.NextPageToken,
	})
	s.Nil(err4)
	s.Equal(0, len(resp.Executions))
}
