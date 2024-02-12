// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package history

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type callbackQueueActiveTaskExecutorSuite struct {
	taskExecutorSuite
}

func TestCallbackQueueActiveTaskExecutor(t *testing.T) {
	s := new(callbackQueueActiveTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *callbackQueueActiveTaskExecutorSuite) SetupTest() {
	s.taskExecutorSuite.SetupTest()
}

func (s *callbackQueueActiveTaskExecutorSuite) TearDownTest() {
	s.taskExecutorSuite.TearDownTest()
}

func (s *callbackQueueActiveTaskExecutorSuite) TestProcessCallbackTask_NamespaceInHandoverState() {
	handoverNamespace := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "failover-nsid", Name: "failover-ns"},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(1),
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "test:///visibility/archival",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
			State: enumspb.REPLICATION_STATE_HANDOVER,
		},
		s.version,
	)
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(handoverNamespace.ID()).Return(handoverNamespace, nil).AnyTimes()
	task := &tasks.CallbackTask{
		WorkflowKey: definition.NewWorkflowKey(string(handoverNamespace.ID()), "", ""),
	}
	resp := s.execute(task, fakeSuccessfulCaller)
	s.ErrorIs(resp.ExecutionErr, consts.ErrNamespaceHandover)
}

func (s *callbackQueueActiveTaskExecutorSuite) TestProcessCallbackTask_Outcomes() {
	type testcase struct {
		name          string
		caller        HTTPCaller
		assertOutcome func(*persistencespb.CallbackInfo)
	}
	cases := []testcase{
		{
			name:   "success",
			caller: fakeSuccessfulCaller,
			assertOutcome: func(ci *persistencespb.CallbackInfo) {
				s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, ci.PublicInfo.State)
			},
		},
		{
			name: "failed",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			assertOutcome: func(ci *persistencespb.CallbackInfo) {
				s.Equal(enumspb.CALLBACK_STATE_BACKING_OFF, ci.PublicInfo.State)
			},
		},
		{
			name: "retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500}, nil
			},
			assertOutcome: func(ci *persistencespb.CallbackInfo) {
				s.Equal(enumspb.CALLBACK_STATE_BACKING_OFF, ci.PublicInfo.State)
			},
		},
		{
			name: "non-retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500}, nil
			},
			assertOutcome: func(ci *persistencespb.CallbackInfo) {
				s.Equal(enumspb.CALLBACK_STATE_BACKING_OFF, ci.PublicInfo.State)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			mutableState := s.prepareMutableStateWithCompletedWFT()

			s.mockShard.Resource.ExecutionMgr.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.SetWorkflowExecutionRequest) (*persistence.SetWorkflowExecutionResponse, error) {
				callbacks := request.SetWorkflowSnapshot.ExecutionInfo.Callbacks
				s.Equal(1, len(callbacks))
				for _, cb := range callbacks {
					tc.assertOutcome(cb)
				}
				return &persistence.SetWorkflowExecutionResponse{}, nil
			})
			event, err := mutableState.AddCompletedWorkflowEvent(mutableState.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
			s.NoError(err)
			s.sealMutableState(mutableState, event, 1)

			task := mutableState.PopTasks()[tasks.CategoryCallback][0]
			resp := s.execute(task, tc.caller)
			s.NoError(resp.ExecutionErr)
		})
	}
}

func (s *callbackQueueActiveTaskExecutorSuite) TestProcessCallbackTask_Completions() {
	type testcase struct {
		name        string
		caller      HTTPCaller
		mutateState func(workflow.MutableState) (*historypb.HistoryEvent, error)
	}
	cases := []testcase{
		{
			name: "success",
			mutateState: func(mutableState workflow.MutableState) (*historypb.HistoryEvent, error) {
				return mutableState.AddCompletedWorkflowEvent(mutableState.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Metadata: map[string][]byte{"encoding": []byte("json/plain")},
								Data:     []byte("3"),
							},
						},
					},
				}, "")
			},
			caller: func(r *http.Request) (*http.Response, error) {
				s.Equal(string(nexus.OperationStateSucceeded), r.Header.Get("nexus-operation-state"))
				s.Equal("application/json", r.Header.Get("content-type"))
				s.Equal("1", r.Header.Get("content-length"))
				defer r.Body.Close()
				buf, err := io.ReadAll(r.Body)
				s.NoError(err)
				s.Equal([]byte("3"), buf)
				return &http.Response{StatusCode: 200}, nil
			},
		},
		{
			name: "failure",
			mutateState: func(mutableState workflow.MutableState) (*historypb.HistoryEvent, error) {
				return mutableState.AddFailWorkflowEvent(mutableState.GetNextEventID(), enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: &failure.Failure{
						Message: "workflow failed",
					},
				}, "")
			},
			caller: func(r *http.Request) (*http.Response, error) {
				s.Equal(string(nexus.OperationStateFailed), r.Header.Get("nexus-operation-state"))
				s.Equal("application/json", r.Header.Get("content-type"))
				defer r.Body.Close()
				buf, err := io.ReadAll(r.Body)
				s.NoError(err)
				var failure nexus.Failure
				err = json.Unmarshal(buf, &failure)
				s.NoError(err)
				s.Equal("workflow failed", failure.Message)
				return &http.Response{StatusCode: 200}, nil
			},
		},
		{
			name: "termination",
			mutateState: func(mutableState workflow.MutableState) (*historypb.HistoryEvent, error) {
				return mutableState.AddWorkflowExecutionTerminatedEvent(mutableState.GetNextEventID(), "dont care", nil, "identity", false)
			},
			caller: func(r *http.Request) (*http.Response, error) {
				s.Equal(string(nexus.OperationStateFailed), r.Header.Get("nexus-operation-state"))
				s.Equal("application/json", r.Header.Get("content-type"))
				defer r.Body.Close()
				buf, err := io.ReadAll(r.Body)
				s.NoError(err)
				var failure nexus.Failure
				err = json.Unmarshal(buf, &failure)
				s.NoError(err)
				s.Equal("operation terminated", failure.Message)
				return &http.Response{StatusCode: 200}, nil
			},
		},
		{
			name: "cancelation",
			mutateState: func(mutableState workflow.MutableState) (*historypb.HistoryEvent, error) {
				return mutableState.AddWorkflowExecutionCanceledEvent(mutableState.GetNextEventID(), &commandpb.CancelWorkflowExecutionCommandAttributes{})
			},
			caller: func(r *http.Request) (*http.Response, error) {
				s.Equal(string(nexus.OperationStateCanceled), r.Header.Get("nexus-operation-state"))
				s.Equal("application/json", r.Header.Get("content-type"))
				defer r.Body.Close()
				buf, err := io.ReadAll(r.Body)
				s.NoError(err)
				var failure nexus.Failure
				err = json.Unmarshal(buf, &failure)
				s.NoError(err)
				s.Equal("operation canceled", failure.Message)
				return &http.Response{StatusCode: 200}, nil
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			mutableState := s.prepareMutableStateWithCompletedWFT()
			event, err := tc.mutateState(mutableState)
			s.NoError(err)
			s.sealMutableState(mutableState, event, 1)
			s.mockShard.Resource.ExecutionMgr.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.SetWorkflowExecutionResponse{}, nil)
			task := mutableState.PopTasks()[tasks.CategoryCallback][0]
			resp := s.execute(task, tc.caller)
			s.NoError(resp.ExecutionErr)
		})
	}
}

func (s *callbackQueueActiveTaskExecutorSuite) execute(
	task tasks.Task,
	caller HTTPCaller,
) queues.ExecuteResponse {
	executor := s.newExecutor(caller)
	executable := queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		executor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		s.mockShard.GetTimeSource(),
		s.mockShard.Resource.NamespaceCache,
		s.mockShard.Resource.ClusterMetadata,
		nil,
		metrics.NoopMetricsHandler,
	)
	return executor.Execute(context.Background(), executable)
}

func (s *callbackQueueActiveTaskExecutorSuite) newExecutor(caller HTTPCaller) *callbackQueueActiveTaskExecutor {
	return newCallbackQueueActiveTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetConfig(),
		func(namespaceID, destination string) HTTPCaller {
			return caller
		},
	)
}

func fakeSuccessfulCaller(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200}, nil
}
