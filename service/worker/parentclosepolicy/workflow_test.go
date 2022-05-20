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

package parentclosepolicy

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/testsuite"

	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tests"
)

type parentClosePolicyWorkflowSuite struct {
	*require.Assertions
	suite.Suite
	testsuite.WorkflowTestSuite

	controller        *gomock.Controller
	mockClientBean    *client.MockBean
	mockHistoryClient *historyservicemock.MockHistoryServiceClient
	mockRemoteClient  *workflowservicemock.MockWorkflowServiceClient

	processor *Processor
}

func TestParentClosePolicyWorkflowSuite(t *testing.T) {
	s := new(parentClosePolicyWorkflowSuite)
	suite.Run(t, s)
}

func (s *parentClosePolicyWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockClientBean = client.NewMockBean(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockRemoteClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)

	s.mockClientBean.EXPECT().GetHistoryClient().Return(s.mockHistoryClient).AnyTimes()
	s.mockClientBean.EXPECT().GetRemoteFrontendClient(gomock.Any()).Return(s.mockRemoteClient, nil).AnyTimes()

	s.processor = &Processor{
		metricsClient: metrics.NoopClient,
		logger:        log.NewNoopLogger(),
		cfg: Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1000),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1000),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(4),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(4),
			NumParentClosePolicySystemWorkflows:    dynamicconfig.GetIntPropertyFn(10),
		},
		clientBean: s.mockClientBean,
	}
}

func (s *parentClosePolicyWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_SameCluster() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enums.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 3",
				RunID:       "childworkflow runID 3",
				Policy:      enums.PARENT_CLOSE_POLICY_ABANDON,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("")).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_RemoteCluster() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enums.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 1"}).Times(1)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 2"}).Times(1)
	s.mockRemoteClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *workflowservice.SignalWithStartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
			s.Equal(common.SystemLocalNamespace, request.Namespace)
			s.Equal(processorWFTypeName, request.WorkflowType.Name)
			s.Equal(processorTaskQueueName, request.TaskQueue.Name)
			s.Equal(workflowIDReusePolicy, request.WorkflowIdReusePolicy)
			s.Equal(processorChannelName, request.SignalName)
			return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
		},
	).Times(2)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}
