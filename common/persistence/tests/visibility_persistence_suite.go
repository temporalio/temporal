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

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// VisibilityPersistenceSuite tests visibility persistence
	VisibilityPersistenceSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		controller *gomock.Controller

		*persistencetests.TestBase
		VisibilityMgr                  manager.VisibilityManager
		SearchAttributesProvider       searchattribute.Provider
		SearchAttributesMapperProvider searchattribute.MapperProvider
		CustomVisibilityStoreFactory   visibility.VisibilityStoreFactory

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation
func (s *VisibilityPersistenceSuite) SetupSuite() {
	s.DefaultTestCluster.SetupTestDatabase()
	cfg := s.DefaultTestCluster.Config()

	var err error
	s.controller = gomock.NewController(s.T())
	s.SearchAttributesProvider = searchattribute.NewTestProvider()
	s.SearchAttributesMapperProvider = searchattribute.NewTestMapperProvider(nil)
	s.VisibilityMgr, err = visibility.NewManager(
		cfg,
		resolver.NewNoopResolver(),
		s.CustomVisibilityStoreFactory,
		nil,
		nil,
		s.SearchAttributesProvider,
		s.SearchAttributesMapperProvider,
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetFloatPropertyFn(0.2),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		metrics.NoopMetricsHandler,
		s.Logger,
	)

	if err != nil {
		// s.NoError doesn't work here.
		s.Logger.Fatal("Unable to create visibility manager", tag.Error(err))
	}
}

// SetupTest implementation
func (s *VisibilityPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

func (s *VisibilityPersistenceSuite) TearDownTest() {
	s.cancel()
}

// TearDownSuite implementation
func (s *VisibilityPersistenceSuite) TearDownSuite() {
	s.VisibilityMgr.Close()
	s.DefaultTestCluster.TearDownTestDatabase()
}

// TestBasicVisibility test
func (s *VisibilityPersistenceSuite) TestBasicVisibility() {
	testNamespaceUUID := namespace.ID(uuid.New())
	startTime := time.Now().UTC().Add(time.Second * -5)
	startReq := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test",
		"visibility-workflow",
		startTime,
		"test-queue",
	)

	// ListOpenWorkflowExecutions
	resp, err1 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err1)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(startReq, resp.Executions[0])

	closeReq := s.createClosedWorkflowRecord(startReq, time.Now())

	// ListOpenWorkflowExecutions
	resp, err3 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err3)
	s.Equal(0, len(resp.Executions))

	// ListClosedWorkflowExecutions
	resp, err4 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			time.Now().Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestBasicVisibilityTimeSkew test
func (s *VisibilityPersistenceSuite) TestBasicVisibilityTimeSkew() {
	testNamespaceUUID := namespace.ID(uuid.New())

	startTime := time.Now()
	openRecord := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test-time-skew",
		"visibility-workflow",
		startTime,
		"test-queue",
	)

	// ListOpenWorkflowExecutions
	resp, err1 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err1)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord, resp.Executions[0])

	closedRecord := s.createClosedWorkflowRecord(openRecord, startTime.Add(-10*time.Millisecond))

	// ListOpenWorkflowExecutions
	resp, err3 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err3)
	s.Equal(0, len(resp.Executions))

	// ListClosedWorkflowExecutions
	resp, err4 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
			searchattribute.CloseTime,
			startTime.Add(-10*time.Millisecond).Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			startTime.Add(-10*time.Millisecond).Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord, resp.Executions[0])
}

func (s *VisibilityPersistenceSuite) TestBasicVisibilityShortWorkflow() {
	testNamespaceUUID := namespace.ID(uuid.New())

	startTime := time.Now().UTC()
	openRecord := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test-short-workflow",
		"visibility-workflow",
		startTime,
		"test-queue",
	)
	closedRecord := s.createClosedWorkflowRecord(openRecord, startTime.Add(10*time.Millisecond))

	// ListOpenWorkflowExecutions
	resp, err3 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err3)
	s.Equal(0, len(resp.Executions))

	// ListClosedWorkflowExecutions
	resp, err4 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
			searchattribute.CloseTime,
			startTime.Add(10*time.Millisecond).Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			startTime.Add(10*time.Millisecond).Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord, resp.Executions[0])
}

// TestVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestVisibilityPagination() {
	testNamespaceUUID := namespace.ID(uuid.New())

	// Create 2 executions
	startTime1 := time.Now().UTC()
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-pagination-test1",
		"visibility-workflow",
		startTime1,
		"test-queue",
	)

	startTime2 := startTime1.Add(time.Second)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-pagination-test2",
		"visibility-workflow",
		startTime2,
		"test-queue",
	)

	// Get the first one
	resp, err2 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime1.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime2.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord2, resp.Executions[0])

	// Use token to get the second one
	resp, err3 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    1,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime1.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime2.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
		NextPageToken: resp.NextPageToken,
	})
	s.Nil(err3)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])

	// It is possible to not return non empty token which is going to return empty result
	if len(resp.NextPageToken) != 0 {
		// Now should get empty result by using token
		resp, err4 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				searchattribute.StartTime,
				startTime1.Format(time.RFC3339Nano),
				searchattribute.StartTime,
				startTime2.Format(time.RFC3339Nano),
				searchattribute.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
			NextPageToken: resp.NextPageToken,
		})
		s.Nil(err4)
		s.Equal(0, len(resp.Executions))
	}
}

// TestFilteringByStartTime test
func (s *VisibilityPersistenceSuite) TestFilteringByStartTime() {
	testNamespaceUUID := namespace.ID(uuid.New())
	startTime := time.Now()

	// Create 2 open workflows, one started 2hrs ago, the other started just now.
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow-1",
		startTime.Add(-2*time.Hour),
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow-2",
		startTime,
		"test-queue",
	)

	// List open workflows with start time filter
	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.StartTime,
			time.Now().Add(-time.Hour).Format(time.RFC3339Nano),
			searchattribute.StartTime,
			time.Now().Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.NoError(err)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord2, resp.Executions[0])

	// List with WorkflowType filter in query string
	queryStr := fmt.Sprintf(`StartTime BETWEEN "%v" AND "%v"`, time.Now().Add(-time.Hour).Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano))
	resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       queryStr,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord2, resp.Executions[0])

	queryStr = fmt.Sprintf(`StartTime BETWEEN "%v" AND "%v"`, time.Now().Add(-3*time.Hour).Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano))
	resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       queryStr,
	})
	s.Nil(err)
	s.Equal(2, len(resp.Executions))

	resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       queryStr + ` AND WorkflowType = "visibility-workflow-1"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])
}

// TestFilteringByType test
func (s *VisibilityPersistenceSuite) TestFilteringByType() {
	testNamespaceUUID := namespace.ID(uuid.New())
	startTime := time.Now()

	// Create 2 executions
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow-1",
		startTime,
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow-2",
		startTime,
		"test-queue",
	)

	// List open with filtering: ListOpenWorkflowExecutionsByType
	resp, err2 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			searchattribute.WorkflowType,
			"visibility-workflow-1",
		),
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])

	// List with WorkflowType filter in query string
	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       `WorkflowType = "visibility-workflow-1"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])

	// Close both executions
	s.createClosedWorkflowRecord(openRecord1, time.Now())
	closedRecord2 := s.createClosedWorkflowRecord(openRecord2, time.Now())

	// List closed with filtering: ListClosedWorkflowExecutionsByType
	resp, err5 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s' AND %s = '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			time.Now().Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			searchattribute.WorkflowType,
			"visibility-workflow-2",
		),
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord2, resp.Executions[0])

	// List with WorkflowType filter in query string
	resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       `WorkflowType = "visibility-workflow-2"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord2, resp.Executions[0])
}

// TestFilteringByWorkflowID test
func (s *VisibilityPersistenceSuite) TestFilteringByWorkflowID() {
	testNamespaceUUID := namespace.ID(uuid.New())
	startTime := time.Now()

	// Create 2 executions
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow",
		startTime,
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow",
		startTime,
		"test-queue",
	)

	// List open with filtering: ListOpenWorkflowExecutionsByWorkflowID
	resp, err2 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s' AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			searchattribute.WorkflowID,
			"visibility-filtering-test1",
		),
	})
	s.Nil(err2)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])

	// List workflow with workflowID filter in query string
	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       `WorkflowId = "visibility-filtering-test1"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertOpenExecutionEquals(openRecord1, resp.Executions[0])

	// Close both executions
	s.createClosedWorkflowRecord(openRecord1, time.Now())
	closedRecord2 := s.createClosedWorkflowRecord(openRecord2, time.Now())

	// List closed with filtering: ListClosedWorkflowExecutionsByWorkflowID
	resp, err5 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s' AND %s = '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			time.Now().Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			searchattribute.WorkflowID,
			"visibility-filtering-test2",
		),
	})
	s.Nil(err5)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord2, resp.Executions[0])

	// List workflow with workflowID filter in query string
	resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query:       `WorkflowId = "visibility-filtering-test2"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closedRecord2, resp.Executions[0])
}

// TestFilteringByStatus test
func (s *VisibilityPersistenceSuite) TestFilteringByStatus() {
	testNamespaceUUID := namespace.ID(uuid.New())
	startTime := time.Now()
	executionTime := startTime

	// Create 2 executions
	workflowExecution1 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test1",
		RunId:      "fb15e4b5-356f-466d-8c6d-a29223e5c536",
	}
	err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(s.ctx, &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        &workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	})
	s.Nil(err0)

	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "visibility-filtering-test2",
		RunId:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	err1 := s.VisibilityMgr.RecordWorkflowExecutionStarted(s.ctx, &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        &workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	})
	s.Nil(err1)

	// Close both executions with different status
	closeTime := time.Now().UTC()
	err2 := s.VisibilityMgr.RecordWorkflowExecutionClosed(s.ctx, &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        &workflowExecution1,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime:         closeTime,
		ExecutionDuration: closeTime.Sub(executionTime),
	})
	s.Nil(err2)

	closeReq := &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      testNamespaceUUID,
			Execution:        &workflowExecution2,
			WorkflowTypeName: "visibility-workflow",
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		CloseTime:         closeTime,
		ExecutionDuration: closeTime.Sub(executionTime),
		HistoryLength:     3,
	}
	err3 := s.VisibilityMgr.RecordWorkflowExecutionClosed(s.ctx, closeReq)
	s.Nil(err3)

	// List closed with filtering: ListClosedWorkflowExecutionsByStatus
	resp, err4 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    2,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			time.Now().Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		),
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])

	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    5,
		Query:       `ExecutionStatus = "Failed"`,
	})
	s.Nil(err)
	s.Equal(1, len(resp.Executions))
	s.assertClosedExecutionEquals(closeReq, resp.Executions[0])
}

// TestDelete test
func (s *VisibilityPersistenceSuite) TestDeleteWorkflow() {
	openRows := 10
	closedRows := 5
	testNamespaceUUID := namespace.ID(uuid.New())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)
	executionTime := closeTime.Add(-4 * time.Second)
	var pendingExecutions []*commonpb.WorkflowExecution
	for i := 0; i < openRows; i++ {
		workflowExecution := commonpb.WorkflowExecution{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		}
		pendingExecutions = append(pendingExecutions, &workflowExecution)
		err0 := s.VisibilityMgr.RecordWorkflowExecutionStarted(
			s.ctx,
			&manager.RecordWorkflowExecutionStartedRequest{
				VisibilityRequestBase: &manager.VisibilityRequestBase{
					NamespaceID:      testNamespaceUUID,
					Execution:        &workflowExecution,
					WorkflowTypeName: "visibility-workflow",
					StartTime:        startTime,
					ExecutionTime:    executionTime,
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
		)
		s.Nil(err0)
	}

	for i := 0; i < closedRows; i++ {
		closeReq := &manager.RecordWorkflowExecutionClosedRequest{
			VisibilityRequestBase: &manager.VisibilityRequestBase{
				NamespaceID:      testNamespaceUUID,
				Execution:        pendingExecutions[i],
				WorkflowTypeName: "visibility-workflow",
				StartTime:        startTime,
				ExecutionTime:    executionTime,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			CloseTime:         closeTime,
			ExecutionDuration: closeTime.Sub(executionTime),
			HistoryLength:     3,
		}
		err1 := s.VisibilityMgr.RecordWorkflowExecutionClosed(s.ctx, closeReq)
		s.Nil(err1)
	}

	// ListClosedWorkflowExecutions
	resp, err3 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    10,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			closeTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err3)
	s.Equal(closedRows, len(resp.Executions))

	// Delete closed workflow
	for _, row := range resp.Executions {
		err4 := s.VisibilityMgr.DeleteWorkflowExecution(s.ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: testNamespaceUUID,
			WorkflowID:  row.GetExecution().GetWorkflowId(),
			RunID:       row.GetExecution().GetRunId(),
		})
		s.Nil(err4)
	}

	// ListClosedWorkflowExecutions
	resp, err5 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		PageSize:    10,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
			searchattribute.CloseTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.CloseTime,
			closeTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	})
	s.Nil(err5)
	s.Equal(0, len(resp.Executions))

	// ListOpenWorkflowExecutions
	resp, err6 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s'AND %s = '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			closeTime.Format(time.RFC3339Nano),
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
		PageSize: 10,
	})
	s.Nil(err6)
	s.Equal(openRows-closedRows, len(resp.Executions))
	// Delete open workflow
	for _, row := range resp.Executions {
		err7 := s.VisibilityMgr.DeleteWorkflowExecution(s.ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: testNamespaceUUID,
			WorkflowID:  row.GetExecution().GetWorkflowId(),
			RunID:       row.GetExecution().GetRunId(),
		})
		s.Nil(err7)
	}
	resp, err8 := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceUUID,
		Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s'",
			searchattribute.StartTime,
			startTime.Format(time.RFC3339Nano),
			searchattribute.StartTime,
			closeTime.Format(time.RFC3339Nano),
		),
		PageSize: 10,
	})
	s.Nil(err8)
	s.Equal(0, len(resp.Executions))
}

// TestUpsertWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestUpsertWorkflowExecution() {
	temporalChangeVersionPayload, _ := payload.Encode([]string{"dummy"})
	tests := []struct {
		request  *manager.UpsertWorkflowExecutionRequest
		expected error
	}{
		{
			request: &manager.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &manager.VisibilityRequestBase{
					NamespaceID:      "",
					Namespace:        "",
					Execution:        &commonpb.WorkflowExecution{},
					WorkflowTypeName: "",
					StartTime:        time.Time{},
					ExecutionTime:    time.Time{},
					TaskID:           0,
					Memo:             nil,
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							searchattribute.TemporalChangeVersion: temporalChangeVersionPayload,
						},
					},
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			expected: nil,
		},
		{
			request: &manager.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &manager.VisibilityRequestBase{
					NamespaceID:      "",
					Namespace:        "",
					Execution:        &commonpb.WorkflowExecution{},
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
		s.Equal(test.expected, s.VisibilityMgr.UpsertWorkflowExecution(s.ctx, test.request))
	}
}

// TestGetWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestGetWorkflowExecution() {
	testNamespaceUUID := namespace.ID(uuid.New())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	var startRequests []*manager.RecordWorkflowExecutionStartedRequest
	for i := 0; i < 5; i++ {
		startRequests = append(
			startRequests,
			s.createOpenWorkflowRecord(
				testNamespaceUUID,
				"visibility-workflow-test",
				"visibility-workflow",
				startTime,
				"test-queue",
			),
		)
	}
	for _, req := range startRequests {
		resp, err := s.VisibilityMgr.GetWorkflowExecution(
			s.ctx,
			&manager.GetWorkflowExecutionRequest{
				NamespaceID: testNamespaceUUID,
				RunID:       req.Execution.RunId,
			},
		)
		s.NoError(err)
		s.assertOpenExecutionEquals(req, resp.Execution)
	}

	var closeRequests []*manager.RecordWorkflowExecutionClosedRequest
	for _, startReq := range startRequests {
		closeRequests = append(closeRequests, s.createClosedWorkflowRecord(startReq, closeTime))
	}
	for _, req := range closeRequests {
		resp, err := s.VisibilityMgr.GetWorkflowExecution(
			s.ctx,
			&manager.GetWorkflowExecutionRequest{
				NamespaceID: testNamespaceUUID,
				RunID:       req.Execution.RunId,
			},
		)
		s.NoError(err)
		s.assertClosedExecutionEquals(req, resp.Execution)
	}
}

// TestAdvancedVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestAdvancedVisibilityPagination() {
	testNamespaceUUID := namespace.ID(uuid.New())

	// Generate 5 workflow records, keep 2 open and 3 closed.
	var startReqs []*manager.RecordWorkflowExecutionStartedRequest
	var closeReqs []*manager.RecordWorkflowExecutionClosedRequest
	for i := 0; i < 5; i++ {
		startReq := s.createOpenWorkflowRecord(
			testNamespaceUUID,
			fmt.Sprintf("advanced-visibility-%v", i),
			"visibility-workflow",
			time.Now(),
			"test-queue",
		)
		if i <= 1 {
			startReqs = append([]*manager.RecordWorkflowExecutionStartedRequest{startReq}, startReqs...)
		} else {
			closeReq := s.createClosedWorkflowRecord(startReq, time.Now())
			closeReqs = append([]*manager.RecordWorkflowExecutionClosedRequest{closeReq}, closeReqs...)
		}
	}

	for pageSize := 1; pageSize <= 5; pageSize++ {
		executions := make(map[string]*workflowpb.WorkflowExecutionInfo)
		for _, e := range s.listWithPagination(testNamespaceUUID, 5) {
			executions[e.GetExecution().GetWorkflowId()] = e
		}

		// there is no order guarantee from the list method, so we have to find the right execution
		for _, r := range startReqs {
			id := r.Execution.GetWorkflowId()
			e, ok := executions[id]
			s.True(ok)
			s.assertOpenExecutionEquals(r, e)
			delete(executions, id)
		}
		for _, r := range closeReqs {
			id := r.Execution.GetWorkflowId()
			e, ok := executions[id]
			s.True(ok)
			s.assertClosedExecutionEquals(r, e)
			delete(executions, id)
		}
		s.Empty(executions, "Unexpected executions returned from list method")
	}
}

func (s *VisibilityPersistenceSuite) TestCountWorkflowExecutions() {
	testNamespaceUUID := namespace.ID(uuid.New())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	for i := 0; i < 5; i++ {
		s.createOpenWorkflowRecord(
			testNamespaceUUID,
			"visibility-workflow-test",
			"visibility-workflow",
			startTime,
			"test-queue",
		)
	}

	resp, err := s.VisibilityMgr.CountWorkflowExecutions(
		s.ctx,
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "",
		},
	)
	s.NoError(err)
	s.Equal(int64(5), resp.Count)
	s.Nil(resp.Groups)
}

func (s *VisibilityPersistenceSuite) TestCountGroupByWorkflowExecutions() {
	testNamespaceUUID := namespace.ID(uuid.New())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	var startRequests []*manager.RecordWorkflowExecutionStartedRequest
	for i := 0; i < 5; i++ {
		startRequests = append(
			startRequests,
			s.createOpenWorkflowRecord(
				testNamespaceUUID,
				"visibility-workflow-test",
				"visibility-workflow",
				startTime,
				"test-queue",
			),
		)
	}

	runningStatusPayload, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	resp, err := s.VisibilityMgr.CountWorkflowExecutions(
		s.ctx,
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "GROUP BY ExecutionStatus",
		},
	)
	s.NoError(err)
	s.Equal(int64(5), resp.Count)
	s.Equal(
		[]*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			{
				GroupValues: []*commonpb.Payload{runningStatusPayload},
				Count:       int64(5),
			},
		},
		resp.Groups,
	)

	for i := 0; i < 2; i++ {
		s.createClosedWorkflowRecord(startRequests[i], closeTime)
	}

	resp, err = s.VisibilityMgr.CountWorkflowExecutions(
		s.ctx,
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "GROUP BY ExecutionStatus",
		},
	)
	s.NoError(err)
	s.Equal(int64(5), resp.Count)
}

func (s *VisibilityPersistenceSuite) listWithPagination(namespaceID namespace.ID, pageSize int) []*workflowpb.WorkflowExecutionInfo {
	var executions []*workflowpb.WorkflowExecutionInfo
	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespaceID,
		PageSize:    pageSize,
		Query:       "",
	})
	s.Nil(err)
	executions = append(executions, resp.Executions...)

	for len(resp.NextPageToken) > 0 {
		resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceID,
			PageSize:      pageSize,
			Query:         "",
			NextPageToken: resp.NextPageToken,
		})
		s.Nil(err)
		executions = append(executions, resp.Executions...)
	}

	return executions
}

func (s *VisibilityPersistenceSuite) createClosedWorkflowRecord(
	startReq *manager.RecordWorkflowExecutionStartedRequest,
	closeTime time.Time,
) *manager.RecordWorkflowExecutionClosedRequest {
	closeReq := &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      startReq.NamespaceID,
			Execution:        startReq.Execution,
			WorkflowTypeName: startReq.WorkflowTypeName,
			StartTime:        startReq.StartTime,
			ExecutionTime:    startReq.ExecutionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime:         closeTime,
		ExecutionDuration: closeTime.Sub(startReq.ExecutionTime),
		HistoryLength:     5,
	}
	err := s.VisibilityMgr.RecordWorkflowExecutionClosed(s.ctx, closeReq)
	s.Nil(err)
	return closeReq
}

func (s *VisibilityPersistenceSuite) createOpenWorkflowRecord(
	namespaceID namespace.ID,
	workflowID string,
	workflowType string,
	startTime time.Time,
	taskQueue string,
) *manager.RecordWorkflowExecutionStartedRequest {
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	startReq := &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      namespaceID,
			Execution:        &workflowExecution,
			WorkflowTypeName: workflowType,
			StartTime:        startTime,
			ExecutionTime:    startTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        taskQueue,
		},
	}
	err := s.VisibilityMgr.RecordWorkflowExecutionStarted(s.ctx, startReq)
	s.Nil(err)
	return startReq
}

func (s *VisibilityPersistenceSuite) assertClosedExecutionEquals(
	req *manager.RecordWorkflowExecutionClosedRequest, resp *workflowpb.WorkflowExecutionInfo) {
	s.Equal(req.Execution.RunId, resp.Execution.RunId)
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(persistence.UnixMilliseconds(req.StartTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	s.Equal(persistence.UnixMilliseconds(req.CloseTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetCloseTime())))
	s.Equal(req.Status, resp.GetStatus())
	s.Equal(req.HistoryLength, resp.HistoryLength)
}

func (s *VisibilityPersistenceSuite) assertOpenExecutionEquals(
	req *manager.RecordWorkflowExecutionStartedRequest, resp *workflowpb.WorkflowExecutionInfo) {
	s.Equal(req.Execution.GetRunId(), resp.Execution.GetRunId())
	s.Equal(req.Execution.WorkflowId, resp.Execution.WorkflowId)
	s.Equal(req.WorkflowTypeName, resp.GetType().GetName())
	s.Equal(persistence.UnixMilliseconds(req.StartTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	s.Nil(resp.CloseTime)
	s.Equal(resp.Status, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	s.Zero(resp.HistoryLength)
}
