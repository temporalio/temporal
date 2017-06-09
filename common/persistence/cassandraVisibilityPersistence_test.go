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

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
}

func (s *visibilityPersistenceSuite) TestVisibilityPagination() {
	testDomainUUID := uuid.New()

	// Create 2 executions
	startTime1 := time.Now()
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime1.UnixNano(),
	})
	s.Nil(err0)

	startTime2 := startTime1.Add(time.Second)
	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test2"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime2.UnixNano(),
	})
	s.Nil(err1)

	// Get the first one
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime1.UnixNano(),
		LatestStartTime:   startTime2.UnixNano(),
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution2.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Use token to get the second one
	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime1.UnixNano(),
		LatestStartTime:   startTime2.UnixNano(),
		NextPageToken:     resp.NextPageToken,
	})
	s.Nil(err3)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Now should get empty result by using token
	resp, err4 := s.VisibilityMgr.ListOpenWorkflowExecutions(&ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime1.UnixNano(),
		LatestStartTime:   startTime2.UnixNano(),
		NextPageToken:     resp.NextPageToken,
	})
	s.Nil(err4)
	s.Equal(0, len(resp.Executions))
}

func (s *visibilityPersistenceSuite) TestFilteringByType() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow-1",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test2"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow-2",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByType(&ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowTypeName: "visibility-workflow-1",
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Close both executions
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow-1",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err3)

	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow-2",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByType(&ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowTypeName: "visibility-workflow-2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution2.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())
}

func (s *visibilityPersistenceSuite) TestFilteringByWorkflowID() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test2"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(&ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowID: "visibility-filtering-test1",
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())

	// Close both executions
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err3)

	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(&ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowID: "visibility-filtering-test2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution2.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())
}

func (s *visibilityPersistenceSuite) TestFilteringByCloseStatus() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test2"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// Close both executions with different status
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		Status:           gen.WorkflowExecutionCloseStatus_COMPLETED,
	})
	s.Nil(err2)

	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		Status:           gen.WorkflowExecutionCloseStatus_FAILED,
	})
	s.Nil(err3)

	// List closed with filtering
	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutionsByStatus(&ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		Status: gen.WorkflowExecutionCloseStatus_FAILED,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution2.GetWorkflowId(), resp.Executions[0].Execution.GetWorkflowId())
}

func (s *visibilityPersistenceSuite) TestGetClosedExecution() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("a3dbc7bf-deb1-4946-b57c-cf0615ea553f"),
	}

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	_, err1 := s.VisibilityMgr.GetClosedWorkflowExecution(&GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.NotNil(err1)

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	})
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.GetClosedWorkflowExecution(&GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Nil(err3)
	s.Equal(workflowExecution.GetWorkflowId(), resp.Execution.GetExecution().GetWorkflowId())
	s.Equal(int64(3), resp.Execution.GetHistoryLength())
}
