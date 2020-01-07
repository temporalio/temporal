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
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/definition"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// VisibilityPersistenceSuite tests visibility persistence
	VisibilityPersistenceSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *VisibilityPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *VisibilityPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *VisibilityPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestBasicVisibility test
func (s *VisibilityPersistenceSuite) TestBasicVisibility() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	startReq := &p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq)
	s.Nil(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq, resp.Executions[0])

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    5,
	}
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestBasicVisibilityTimeSkew test
func (s *VisibilityPersistenceSuite) TestBasicVisibilityTimeSkew() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test-time-skew"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution.WorkflowId, resp.Executions[0].Execution.WorkflowId)

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   startTime - (10 * time.Second).Nanoseconds(),
	})
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
}

// TestVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestVisibilityPagination() {
	testDomainUUID := uuid.New()

	// Create 2 executions
	startTime1 := time.Now()
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}

	startReq1 := &p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime1.UnixNano(),
	}

	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq1)
	s.Nil(err0)

	startTime2 := startTime1.Add(time.Second)
	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-pagination-test2"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}

	startReq2 := &p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime2.UnixNano(),
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq2)
	s.Nil(err1)

	// Get the first one
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime1.UnixNano(),
		LatestStartTime:   startTime2.UnixNano(),
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq2, resp.Executions[0])

	// Use token to get the second one
	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		PageSize:          1,
		EarliestStartTime: startTime1.UnixNano(),
		LatestStartTime:   startTime2.UnixNano(),
		NextPageToken:     resp.NextPageToken,
	})
	s.Nil(err3)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq1, resp.Executions[0])

	// It is possible to not return non empty token which is going to return empty result
	if len(resp.NextPageToken) != 0 {
		// Now should get empty result by using token
		resp, err4 := s.VisibilityMgr.ListOpenWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          1,
			EarliestStartTime: startTime1.UnixNano(),
			LatestStartTime:   startTime2.UnixNano(),
			NextPageToken:     resp.NextPageToken,
		})
		s.Nil(err4)
		s.Equal(0, len(resp.Executions))
	}
}

// TestFilteringByType test
func (s *VisibilityPersistenceSuite) TestFilteringByType() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
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
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow-2",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByType(&p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowTypeName: "visibility-workflow-1",
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.WorkflowId, resp.Executions[0].Execution.WorkflowId)

	// Close both executions
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow-1",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err3)

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow-2",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}
	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByType(&p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowTypeName: "visibility-workflow-2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestFilteringByWorkflowID test
func (s *VisibilityPersistenceSuite) TestFilteringByWorkflowID() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
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
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(&p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowID: "visibility-filtering-test1",
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution1.WorkflowId, resp.Executions[0].Execution.WorkflowId)

	// Close both executions
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
	})
	s.Nil(err3)

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}
	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(&p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		WorkflowID: "visibility-filtering-test2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestFilteringByCloseStatus test
func (s *VisibilityPersistenceSuite) TestFilteringByCloseStatus() {
	testDomainUUID := uuid.New()
	startTime := time.Now().UnixNano()

	// Create 2 executions
	workflowExecution1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-filtering-test1"),
		RunId:      common.StringPtr("fb15e4b5-356f-466d-8c6d-a29223e5c536"),
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
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
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err1)

	// Close both executions with different status
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution1,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		CloseTimestamp:   time.Now().UnixNano(),
		Status:           gen.WorkflowExecutionCloseStatusCompleted,
	})
	s.Nil(err2)

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution2,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		Status:           gen.WorkflowExecutionCloseStatusFailed,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err3)

	// List closed with filtering
	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutionsByStatus(&p.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   startTime,
		},
		Status: gen.WorkflowExecutionCloseStatusFailed,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestGetClosedExecution test
func (s *VisibilityPersistenceSuite) TestGetClosedExecution() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("a3dbc7bf-deb1-4946-b57c-cf0615ea553f"),
	}

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
	})
	s.Nil(err0)

	closedResp, err1 := s.VisibilityMgr.GetClosedWorkflowExecution(&p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Error(err1)
	_, ok := err1.(*gen.EntityNotExistsError)
	s.True(ok, "EntityNotExistsError")
	s.Nil(closedResp)

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		Status:           gen.WorkflowExecutionCloseStatusFailed,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.GetClosedWorkflowExecution(&p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Nil(err3)
	s.assertClosedExecutionEquals(closeReq, resp.Execution)
}

// TestClosedWithoutStarted test
func (s *VisibilityPersistenceSuite) TestClosedWithoutStarted() {
	testDomainUUID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("1bdb0122-e8c9-4b35-b6f8-d692ab259b09"),
	}

	closedResp, err0 := s.VisibilityMgr.GetClosedWorkflowExecution(&p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Error(err0)
	_, ok := err0.(*gen.EntityNotExistsError)
	s.True(ok, "EntityNotExistsError")
	s.Nil(closedResp)

	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   time.Now().Add(time.Second * -5).UnixNano(),
		Status:           gen.WorkflowExecutionCloseStatusFailed,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err1)

	resp, err2 := s.VisibilityMgr.GetClosedWorkflowExecution(&p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Nil(err2)
	s.assertClosedExecutionEquals(closeReq, resp.Execution)
}

// TestMultipleUpserts test
func (s *VisibilityPersistenceSuite) TestMultipleUpserts() {
	testDomainUUID := uuid.New()

	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("a3dbc7bf-deb1-4946-b57c-cf0615ea553f"),
	}

	startTime := time.Now().Add(time.Second * -5).UnixNano()
	closeReq := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Execution:        workflowExecution,
		WorkflowTypeName: "visibility-workflow",
		StartTimestamp:   startTime,
		Status:           gen.WorkflowExecutionCloseStatusFailed,
		CloseTimestamp:   time.Now().UnixNano(),
		HistoryLength:    3,
	}

	count := 3
	for i := 0; i < count; i++ {
		err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
			DomainUUID:       testDomainUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTimestamp:   startTime,
		})
		s.Nil(err0)
		if i < count-1 {
			err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
			s.Nil(err1)
		}
	}

	resp, err3 := s.VisibilityMgr.GetClosedWorkflowExecution(&p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainUUID,
		Execution:  workflowExecution,
	})
	s.Nil(err3)
	s.assertClosedExecutionEquals(closeReq, resp.Execution)

}

// TestDelete test
func (s *VisibilityPersistenceSuite) TestDelete() {
	if s.VisibilityMgr.GetName() == "cassandra" {
		s.T().Skip("this test is not applicable for cassandra")
	}
	nRows := 5
	testDomainUUID := uuid.New()
	startTime := time.Now().Add(time.Second * -5).UnixNano()
	for i := 0; i < nRows; i++ {
		workflowExecution := gen.WorkflowExecution{
			WorkflowId: common.StringPtr(uuid.New()),
			RunId:      common.StringPtr(uuid.New()),
		}
		err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&p.RecordWorkflowExecutionStartedRequest{
			DomainUUID:       testDomainUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTimestamp:   startTime,
		})
		s.Nil(err0)
		closeReq := &p.RecordWorkflowExecutionClosedRequest{
			DomainUUID:       testDomainUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTimestamp:   startTime,
			Status:           gen.WorkflowExecutionCloseStatusFailed,
			CloseTimestamp:   time.Now().UnixNano(),
			HistoryLength:    3,
		}
		err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
		s.Nil(err1)
	}

	resp, err3 := s.VisibilityMgr.ListClosedWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainUUID,
		EarliestStartTime: startTime,
		LatestStartTime:   time.Now().UnixNano(),
		PageSize:          10,
	})
	s.Nil(err3)
	s.Equal(nRows, len(resp.Executions))

	remaining := nRows
	for _, row := range resp.Executions {
		err4 := s.VisibilityMgr.DeleteWorkflowExecution(&p.VisibilityDeleteWorkflowExecutionRequest{
			DomainID: testDomainUUID,
			RunID:    row.GetExecution().GetRunId(),
		})
		s.Nil(err4)
		remaining--
		resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutions(&p.ListWorkflowExecutionsRequest{
			DomainUUID:        testDomainUUID,
			EarliestStartTime: startTime,
			LatestStartTime:   time.Now().UnixNano(),
			PageSize:          10,
		})
		s.Nil(err5)
		s.Equal(remaining, len(resp.Executions))
	}
}

// TestUpsertWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestUpsertWorkflowExecution() {
	tests := []struct {
		request  *p.UpsertWorkflowExecutionRequest
		expected error
	}{
		{
			request: &p.UpsertWorkflowExecutionRequest{
				DomainUUID:         "",
				Domain:             "",
				Execution:          gen.WorkflowExecution{},
				WorkflowTypeName:   "",
				StartTimestamp:     0,
				ExecutionTimestamp: 0,
				WorkflowTimeout:    0,
				TaskID:             0,
				Memo:               nil,
				SearchAttributes: map[string][]byte{
					definition.CadenceChangeVersion: []byte("dummy"),
				},
			},
			expected: nil,
		},
		{
			request: &p.UpsertWorkflowExecutionRequest{
				DomainUUID:         "",
				Domain:             "",
				Execution:          gen.WorkflowExecution{},
				WorkflowTypeName:   "",
				StartTimestamp:     0,
				ExecutionTimestamp: 0,
				WorkflowTimeout:    0,
				TaskID:             0,
				Memo:               nil,
				SearchAttributes:   nil,
			},
			expected: p.NewOperationNotSupportErrorForVis(),
		},
	}

	for _, test := range tests {
		s.Equal(test.expected, s.VisibilityMgr.UpsertWorkflowExecution(test.request))
	}
}

func (s *VisibilityPersistenceSuite) assertClosedExecutionEquals(
	req *p.RecordWorkflowExecutionClosedRequest, resp *gen.WorkflowExecutionInfo) {
	s.Equal(req.Execution.RunId, resp.Execution.RunId)
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(s.nanosToMillis(req.StartTimestamp), s.nanosToMillis(resp.GetStartTime()))
	s.Equal(s.nanosToMillis(req.CloseTimestamp), s.nanosToMillis(resp.GetCloseTime()))
	s.Equal(req.Status, resp.GetCloseStatus())
	s.Equal(req.HistoryLength, *resp.HistoryLength)
}

func (s *VisibilityPersistenceSuite) assertOpenExecutionEquals(
	req *p.RecordWorkflowExecutionStartedRequest, resp *gen.WorkflowExecutionInfo) {
	s.Equal(req.Execution.GetRunId(), resp.Execution.GetRunId())
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(s.nanosToMillis(req.StartTimestamp), s.nanosToMillis(resp.GetStartTime()))
	s.Nil(resp.CloseTime)
	s.Nil(resp.CloseStatus)
	s.Nil(resp.HistoryLength)
}

func (s *VisibilityPersistenceSuite) nanosToMillis(nanos int64) int64 {
	return nanos / int64(time.Millisecond)
}
