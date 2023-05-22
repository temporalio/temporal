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

package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
)

func TestForceReplicationWorkflow_Query_Deprecated(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
				LastStartTime: startTime,
				LastCloseTime: closeTime,
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(totalPageCount)

	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(totalPageCount)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "random query",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	_ = envValue.Get(&status)
	assert.Equal(t, 0, status.ContinuedAsNewCount)
	assert.Equal(t, startTime, status.LastStartTime)
	assert.Equal(t, closeTime, status.LastCloseTime)
}

func TestForceReplicationWorkflow_Queries_Compatibility(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	namespace := "test-ns"
	query := "deprecated query"
	queries := []string{"random query"}

	env.OnActivity(a.ListWorkflows, mock.Anything, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     namespace,
		PageSize:      int32(1),
		NextPageToken: nil,
		Query:         query,
	}).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(1)
	env.OnActivity(a.ListWorkflows, mock.Anything, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     namespace,
		PageSize:      1,
		NextPageToken: nil,
		Query:         queries[0],
	}).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(1)
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(2)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               namespace,
		Query:                   query,
		Queries:                 queries,
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	_ = envValue.Get(&status)
	assert.Equal(t, 0, status.ContinuedAsNewCount)
	assert.Equal(t, startTime, status.LastStartTime)
	assert.Equal(t, closeTime, status.LastCloseTime)
}

func TestForceReplicationWorkflow_Queries(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	namespace := "test-ns"
	query := ""
	queries := []string{"random query 1", "random query 2"}

	env.OnActivity(a.ListWorkflows, mock.Anything, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     namespace,
		PageSize:      1,
		NextPageToken: nil,
		Query:         queries[0],
	}).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(1)
	env.OnActivity(a.ListWorkflows, mock.Anything, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     namespace,
		PageSize:      1,
		NextPageToken: nil,
		Query:         queries[1],
	}).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(1)
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(2)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               namespace,
		Query:                   query,
		Queries:                 queries,
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	_ = envValue.Get(&status)
	assert.Equal(t, 0, status.ContinuedAsNewCount)
	assert.Equal(t, startTime, status.LastStartTime)
	assert.Equal(t, closeTime, status.LastCloseTime)
}

func TestForceReplicationWorkflow_ContinueAsNew(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	maxPageCountPerExecution := 2
	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
				LastStartTime: startTime,
				LastCloseTime: closeTime,
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	}).Times(maxPageCountPerExecution)

	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(maxPageCountPerExecution)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		Queries:                 []string{"random query"},
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   maxPageCountPerExecution,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "continue as new")
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	_ = envValue.Get(&status)
	assert.Equal(t, 1, status.ContinuedAsNewCount)
	assert.Equal(t, startTime, status.LastStartTime)
	assert.Equal(t, closeTime, status.LastCloseTime)
}

func TestForceReplicationWorkflow_ListWorkflowsError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	maxPageCountPerExecution := 2
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(nil, errors.New("mock listWorkflows error"))

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		Queries:                 []string{"random query"},
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   maxPageCountPerExecution,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock listWorkflows error")
	env.AssertExpectations(t)
}

func TestForceReplicationWorkflow_GenerateReplicationTaskError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	}).Times(totalPageCount)

	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(errors.New("mock generate replication tasks error"))

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		Queries:                 []string{"random query"},
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock generate replication tasks error")
	env.AssertExpectations(t)
}
