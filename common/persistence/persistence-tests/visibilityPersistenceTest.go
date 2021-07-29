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
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/visibility"

	"go.temporal.io/server/common/payload"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
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
	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}

	startTime := time.Now().UTC().Add(time.Second * -5)
	startReq := &visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq)
	s.Nil(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq, resp.Executions[0])

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime:     time.Now(),
		HistoryLength: 5,
	}
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   time.Now(),
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestBasicVisibilityTimeSkew test
func (s *VisibilityPersistenceSuite) TestBasicVisibilityTimeSkew() {
	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test-time-skew",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}

	startTime := time.Now().UTC()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.NoError(err0)

	resp, err1 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.NoError(err1)
	s.Equal(1, len(resp.Executions))
	s.Equal(workflowExecution.WorkflowId, resp.Executions[0].Execution.WorkflowId)

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime: startTime.Add(-10 * time.Millisecond),
	})
	s.NoError(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.NoError(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime.Add(-10 * time.Millisecond), // This is actually close_time
		LatestStartTime:   startTime.Add(-10 * time.Millisecond),
	})
	s.NoError(err4)
	s.Equal(1, len(resp.Executions))
}

func (s *VisibilityPersistenceSuite) TestBasicVisibilityShortWorkflow() {
	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test-short-workflow",
		RunId:      "3c095198-0c33-4136-939a-c29fbbb6a80b",
	}

	startTime := time.Now().UTC()
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.NoError(err0)

	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime: startTime.Add(10 * time.Millisecond),
	})
	s.NoError(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.NoError(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime.Add(10 * time.Millisecond), // This is actually close_time
		LatestStartTime:   startTime.Add(10 * time.Millisecond),
	})
	s.NoError(err4)
	s.Equal(1, len(resp.Executions))
}

func (s *VisibilityPersistenceSuite) TestVisibilityRetention() {
	if _, ok := s.VisibilityTestCluster.(*cassandra.TestCluster); !ok {
		return
	}

	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test-visibility-retention",
		RunId:      "3c095198-0c33-4136-939a-c29fbbb6a802",
	}

	startTime := time.Now().UTC().Add(-1 * time.Hour)
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.NoError(err0)

	retention := 1 * time.Second
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime: startTime.Add(1 * time.Minute),
		Retention: &retention,
	})
	s.NoError(err2)

	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime,
		LatestStartTime:   startTime,
	})
	s.NoError(err3)
	s.Equal(0, len(resp.Executions))

	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime.Add(1 * time.Minute), // This is actually close_time
		LatestStartTime:   startTime.Add(1 * time.Minute),
	})
	s.NoError(err4)
	s.Equal(1, len(resp.Executions))

	// Sleep for retention to fire.
	time.Sleep(retention)
	resp2, err5 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime.Add(1 * time.Minute), // This is actually close_time
		LatestStartTime:   startTime.Add(1 * time.Minute),
	})
	s.NoError(err5)
	s.Equal(0, len(resp2.Executions))
}

// TestVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestVisibilityPagination() {
	testNamespaceUUID := uuid.New()

	// Create 2 executions
	startTime1 := time.Now().UTC()
	workflowExecution1 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-pagination-test1",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}

	startReq1 := &visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime1,
		},
	}

	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq1)
	s.Nil(err0)

	startTime2 := startTime1.Add(time.Second)
	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-pagination-test2",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}

	startReq2 := &visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime2,
		},
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(startReq2)
	s.Nil(err1)

	// Get the first one
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime1,
		LatestStartTime:   startTime2,
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq2, resp.Executions[0])

	// Use token to get the second one
	resp, err3 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		PageSize:          1,
		EarliestStartTime: startTime1,
		LatestStartTime:   startTime2,
		NextPageToken:     resp.NextPageToken,
	})
	s.Nil(err3)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq1, resp.Executions[0])

	// It is possible to not return non empty token which is going to return empty result
	if len(resp.NextPageToken) != 0 {
		// Now should get empty result by using token
		resp, err4 := s.VisibilityMgr.ListOpenWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
			PageSize:          1,
			EarliestStartTime: startTime1,
			LatestStartTime:   startTime2,
			NextPageToken:     resp.NextPageToken,
		})
		s.Nil(err4)
		s.Equal(0, len(resp.Executions))
	}
}

// TestFilteringByType test
func (s *VisibilityPersistenceSuite) TestFilteringByType() {
	testNamespaceUUID := uuid.New()
	startTime := time.Now()

	// Create 2 executions
	workflowExecution1 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test1",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow-1",
			StartTime:        startTime,
		},
	})
	s.Nil(err0)

	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test2",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow-2",
			StartTime:        startTime,
		},
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByType(&visibility.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
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
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow-1",
			StartTime:        startTime,
		},
		CloseTime: time.Now(),
	})
	s.Nil(err3)

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow-2",
			StartTime:        startTime,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}
	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByType(&visibility.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   time.Now(),
		},
		WorkflowTypeName: "visibility-workflow-2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestFilteringByWorkflowID test
func (s *VisibilityPersistenceSuite) TestFilteringByWorkflowID() {
	testNamespaceUUID := uuid.New()
	startTime := time.Now()

	// Create 2 executions
	workflowExecution1 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test1",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.Nil(err0)

	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test2",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.Nil(err1)

	// List open with filtering
	resp, err2 := s.VisibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(&visibility.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
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
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime: time.Now(),
	})
	s.Nil(err3)

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}
	err4 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err4)

	// List closed with filtering
	resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(&visibility.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   time.Now(),
		},
		WorkflowID: "visibility-filtering-test2",
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestFilteringByStatus test
func (s *VisibilityPersistenceSuite) TestFilteringByStatus() {
	testNamespaceUUID := uuid.New()
	startTime := time.Now()

	// Create 2 executions
	workflowExecution1 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test1",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.Nil(err0)

	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test2",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.Nil(err1)

	// Close both executions with different status
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(&visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime: time.Now(),
	})
	s.Nil(err2)

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err3)

	// List closed with filtering
	resp, err4 := s.VisibilityMgr.ListClosedWorkflowExecutionsByStatus(&visibility.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
			PageSize:          2,
			EarliestStartTime: startTime,
			LatestStartTime:   time.Now(),
		},
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestGetClosedExecution test
func (s *VisibilityPersistenceSuite) TestGetClosedExecution() {
	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "a3dbc7bf-deb1-4946-b57c-cf0615ea553f",
	}

	startTime := time.Now().UTC().Add(time.Second * -5)
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
		},
	})
	s.Nil(err0)

	closedResp, err1 := s.VisibilityMgr.GetClosedWorkflowExecution(&visibility.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Execution:   workflowExecution,
	})
	s.Error(err1)
	_, ok := err1.(*serviceerror.NotFound)
	s.True(ok, "EntityNotExistsError")
	s.Nil(closedResp)

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err2)

	resp, err3 := s.VisibilityMgr.GetClosedWorkflowExecution(&visibility.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Execution:   workflowExecution,
	})
	s.Nil(err3)
	s.assertClosedExecutionEquals(closeReq, resp.Execution)
}

// TestClosedWithoutStarted test
func (s *VisibilityPersistenceSuite) TestClosedWithoutStarted() {
	testNamespaceUUID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "1bdb0122-e8c9-4b35-b6f8-d692ab259b09",
	}

	closedResp, err0 := s.VisibilityMgr.GetClosedWorkflowExecution(&visibility.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Execution:   workflowExecution,
	})
	s.Error(err0)
	_, ok := err0.(*serviceerror.NotFound)
	s.True(ok, "EntityNotExistsError")
	s.Nil(closedResp)

	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        time.Now().UTC().Add(time.Second * -5),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
	s.Nil(err1)

	resp, err2 := s.VisibilityMgr.GetClosedWorkflowExecution(&visibility.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Execution:   workflowExecution,
	})
	s.Nil(err2)
	s.assertClosedExecutionEquals(closeReq, resp.Execution)
}

// TestMultipleUpserts test
func (s *VisibilityPersistenceSuite) TestMultipleUpserts() {
	testNamespaceUUID := uuid.New()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "visibility-workflow-test",
		RunId:      "a3dbc7bf-deb1-4946-b57c-cf0615ea553f",
	}

	startTime := time.Now().UTC().Add(time.Second * -5)
	closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        workflowExecution,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		CloseTime:     time.Now(),
		HistoryLength: 3,
	}

	count := 3
	for i := 0; i < count; i++ {
		err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
			VisibilityRequestBase: &visibility.VisibilityRequestBase{
				NamespaceID:      testNamespaceUUID,
				Execution:        workflowExecution,
				WorkflowTypeName: "visibility-workflow",
				StartTime:        startTime,
			},
		})
		s.Nil(err0)
		if i < count-1 {
			err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
			s.Nil(err1)
		}
	}

	resp, err3 := s.VisibilityMgr.GetClosedWorkflowExecution(&visibility.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceUUID,
		Execution:   workflowExecution,
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
	testNamespaceUUID := uuid.New()
	startTime := time.Now().UTC().Add(time.Second * -5)
	for i := 0; i < nRows; i++ {
		workflowExecution := commonpb.WorkflowExecution{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		}
		err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(&visibility.RecordWorkflowExecutionStartedRequest{
			VisibilityRequestBase: &visibility.VisibilityRequestBase{
				NamespaceID:      testNamespaceUUID,
				Execution:        workflowExecution,
				WorkflowTypeName: "visibility-workflow",
				StartTime:        startTime,
			},
		})
		s.Nil(err0)
		closeReq := &visibility.RecordWorkflowExecutionClosedRequest{
			VisibilityRequestBase: &visibility.VisibilityRequestBase{
				NamespaceID:      testNamespaceUUID,
				Execution:        workflowExecution,
				WorkflowTypeName: "visibility-workflow",
				StartTime:        startTime,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			CloseTime:     time.Now(),
			HistoryLength: 3,
		}
		err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(closeReq)
		s.Nil(err1)
	}

	resp, err3 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceUUID,
		EarliestStartTime: startTime,
		LatestStartTime:   time.Now(),
		PageSize:          10,
	})
	s.Nil(err3)
	s.Equal(nRows, len(resp.Executions))

	remaining := nRows
	for _, row := range resp.Executions {
		err4 := s.VisibilityMgr.DeleteWorkflowExecution(&visibility.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: testNamespaceUUID,
			RunID:       row.GetExecution().GetRunId(),
		})
		s.Nil(err4)
		remaining--
		resp, err5 := s.VisibilityMgr.ListClosedWorkflowExecutions(&visibility.ListWorkflowExecutionsRequest{
			NamespaceID:       testNamespaceUUID,
			EarliestStartTime: startTime,
			LatestStartTime:   time.Now(),
			PageSize:          10,
		})
		s.Nil(err5)
		s.Equal(remaining, len(resp.Executions))
	}
}

// TestUpsertWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestUpsertWorkflowExecution() {
	tests := []struct {
		request  *visibility.UpsertWorkflowExecutionRequest
		expected error
	}{
		{
			request: &visibility.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &visibility.VisibilityRequestBase{
					NamespaceID:      "",
					Namespace:        "",
					Execution:        commonpb.WorkflowExecution{},
					WorkflowTypeName: "",
					StartTime:        time.Time{},
					ExecutionTime:    time.Time{},
					TaskID:           0,
					Memo:             nil,
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							searchattribute.TemporalChangeVersion: payload.EncodeBytes([]byte("dummy")),
						},
					},
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			expected: nil,
		},
		{
			request: &visibility.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &visibility.VisibilityRequestBase{
					NamespaceID:      "",
					Namespace:        "",
					Execution:        commonpb.WorkflowExecution{},
					WorkflowTypeName: "",
					StartTime:        time.Time{},
					ExecutionTime:    time.Time{},
					TaskID:           0,
					Memo:             nil,
					SearchAttributes: nil,
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			// To avoid blocking the task queue processors on non-ElasticSearch visibility stores
			// we simply treat any attempts to perform Upserts as "no-ops"
			// Attempts to Scan, Count or List will still fail for non-ES stores.
			expected: nil,
		},
	}

	for _, test := range tests {
		s.Equal(test.expected, s.VisibilityMgr.UpsertWorkflowExecution(test.request))
	}
}

func (s *VisibilityPersistenceSuite) assertClosedExecutionEquals(
	req *visibility.RecordWorkflowExecutionClosedRequest, resp *workflowpb.WorkflowExecutionInfo) {
	s.Equal(req.Execution.RunId, resp.Execution.RunId)
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(p.UnixMilliseconds(req.StartTime), p.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	s.Equal(p.UnixMilliseconds(req.CloseTime), p.UnixMilliseconds(timestamp.TimeValue(resp.GetCloseTime())))
	s.Equal(req.Status, resp.GetStatus())
	s.Equal(req.HistoryLength, resp.HistoryLength)
}

func (s *VisibilityPersistenceSuite) assertOpenExecutionEquals(
	req *visibility.RecordWorkflowExecutionStartedRequest, resp *workflowpb.WorkflowExecutionInfo) {
	s.Equal(req.Execution.GetRunId(), resp.Execution.GetRunId())
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(p.UnixMilliseconds(req.StartTime), p.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	s.Nil(resp.CloseTime)
	s.Equal(resp.Status, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	s.Zero(resp.HistoryLength)
}
