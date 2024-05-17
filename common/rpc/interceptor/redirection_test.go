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

package interceptor

import (
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/temporalapi"
)

type (
	redirectionInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		namespaceCache  *namespace.MockRegistry
		clientBean      *client.MockBean
		clusterMetadata *cluster.MockMetadata

		redirector *Redirection
	}
)

func TestRedirectionInterceptorSuite(t *testing.T) {
	s := new(redirectionInterceptorSuite)
	suite.Run(t, s)
}

func (s *redirectionInterceptorSuite) SetupSuite() {
}

func (s *redirectionInterceptorSuite) TearDownSuite() {
}

func (s *redirectionInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.redirector = NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		s.namespaceCache,
		config.DCRedirectionPolicy{
			Policy: DCRedirectionPolicyAllAPIsForwarding,
		},
		log.NewNoopLogger(),
		s.clientBean,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)
}

func (s *redirectionInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *redirectionInterceptorSuite) TestLocalAPI() {
	apis := make(map[string]struct{})
	for api := range localAPIResponses {
		apis[api] = struct{}{}
	}
	s.Equal(map[string]struct{}{
		"DeprecateNamespace": {},
		"DescribeNamespace":  {},
		"ListNamespaces":     {},
		"RegisterNamespace":  {},
		"UpdateNamespace":    {},

		"GetSearchAttributes": {},
		"GetClusterInfo":      {},
		"GetSystemInfo":       {},
	}, apis)
}

func (s *redirectionInterceptorSuite) TestGlobalAPI() {
	apis := make(map[string]struct{})
	for api := range globalAPIResponses {
		apis[api] = struct{}{}
	}
	s.Equal(map[string]struct{}{
		"DescribeTaskQueue":                  {},
		"DescribeWorkflowExecution":          {},
		"GetWorkflowExecutionHistory":        {},
		"GetWorkflowExecutionHistoryReverse": {},
		"ListArchivedWorkflowExecutions":     {},
		"ListClosedWorkflowExecutions":       {},
		"ListOpenWorkflowExecutions":         {},
		"ListWorkflowExecutions":             {},
		"ScanWorkflowExecutions":             {},
		"CountWorkflowExecutions":            {},
		"PollActivityTaskQueue":              {},
		"PollWorkflowTaskQueue":              {},
		"PollNexusTaskQueue":                 {},
		"QueryWorkflow":                      {},
		"RecordActivityTaskHeartbeat":        {},
		"RecordActivityTaskHeartbeatById":    {},
		"RequestCancelWorkflowExecution":     {},
		"ResetStickyTaskQueue":               {},
		"ResetWorkflowExecution":             {},
		"RespondActivityTaskCanceled":        {},
		"RespondActivityTaskCanceledById":    {},
		"RespondActivityTaskCompleted":       {},
		"RespondActivityTaskCompletedById":   {},
		"RespondActivityTaskFailed":          {},
		"RespondActivityTaskFailedById":      {},
		"RespondWorkflowTaskCompleted":       {},
		"RespondWorkflowTaskFailed":          {},
		"RespondQueryTaskCompleted":          {},
		"RespondNexusTaskCompleted":          {},
		"RespondNexusTaskFailed":             {},
		"SignalWithStartWorkflowExecution":   {},
		"SignalWorkflowExecution":            {},
		"StartWorkflowExecution":             {},
		"ExecuteMultiOperation":              {},
		"UpdateWorkflowExecution":            {},
		"PollWorkflowExecutionUpdate":        {},
		"TerminateWorkflowExecution":         {},
		"DeleteWorkflowExecution":            {},
		"ListTaskQueuePartitions":            {},

		"CreateSchedule":                   {},
		"DescribeSchedule":                 {},
		"UpdateSchedule":                   {},
		"PatchSchedule":                    {},
		"DeleteSchedule":                   {},
		"ListSchedules":                    {},
		"ListScheduleMatchingTimes":        {},
		"UpdateWorkerBuildIdCompatibility": {},
		"GetWorkerBuildIdCompatibility":    {},
		"UpdateWorkerVersioningRules":      {},
		"GetWorkerVersioningRules":         {},
		"GetWorkerTaskReachability":        {},

		"StartBatchOperation":    {},
		"StopBatchOperation":     {},
		"DescribeBatchOperation": {},
		"ListBatchOperations":    {},
	}, apis)
}

func (s *redirectionInterceptorSuite) TestAPIResultMapping() {
	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	expectedAPIs := make(map[string]interface{}, t.NumMethod())
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		expectedAPIs[m.Name] = m.Type.Out(0)
	})

	actualAPIs := make(map[string]interface{})
	for api, respAllocFn := range localAPIResponses {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}
	for api, respAllocFn := range globalAPIResponses {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}
	s.Equal(expectedAPIs, actualAPIs)
}

func (s *redirectionInterceptorSuite) TestHandleLocalAPIInvocation() {
	ctx := context.Background()
	req := &workflowservice.RegisterNamespaceRequest{}
	functionInvoked := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		functionInvoked = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	}
	methodName := "RegisterNamespace"

	resp, err := s.redirector.handleLocalAPIInvocation(
		ctx,
		req,
		handler,
		methodName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.RegisterNamespaceResponse{}, resp)
	s.True(functionInvoked)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_Local() {
	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{}
	functionInvoked := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		functionInvoked = true
		return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
	}
	namespaceName := namespace.Name("(╯°Д°)╯ ┻━┻")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		handler,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, resp)
	s.True(functionInvoked)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_Redirect() {
	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
	}
	namespaceName := namespace.Name("(╯°Д°)╯ ┻━┻")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	grpcConn := &mockClientConnInterface{
		Suite:          &s.Suite,
		targetMethod:   info.FullMethod,
		targetResponse: &workflowservice.SignalWithStartWorkflowExecutionResponse{},
	}
	s.clientBean.EXPECT().GetRemoteFrontendClient(cluster.TestAlternativeClusterName).Return(grpcConn, nil, nil).Times(1)

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		nil,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, resp)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_NamespaceNotFound() {
	ctx := context.Background()
	req := &workflowservice.PollWorkflowTaskQueueRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
	}

	namespaceName := namespace.Name("unknown_namespace")
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(nil, &serviceerror.NamespaceNotFound{}).AnyTimes()
	methodName := "PollWorkflowTaskQueue"

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		nil,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.Nil(resp)
	s.IsType(&serviceerror.NamespaceNotFound{}, err)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_Empty() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{}))
	allowed := s.redirector.redirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_Error() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		dcRedirectionContextHeaderName: "?",
	}))
	allowed := s.redirector.redirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_True() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		dcRedirectionContextHeaderName: "t",
	}))
	allowed := s.redirector.redirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_False() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		dcRedirectionContextHeaderName: "f",
	}))
	allowed := s.redirector.redirectionAllowed(ctx)
	s.False(allowed)
}

type (
	mockClientConnInterface struct {
		*suite.Suite
		targetMethod   string
		targetResponse interface{}
	}
)

var _ grpc.ClientConnInterface = (*mockClientConnInterface)(nil)

func (s *mockClientConnInterface) Invoke(
	_ context.Context,
	method string,
	_ interface{},
	reply interface{},
	_ ...grpc.CallOption,
) error {
	s.Equal(s.targetMethod, method)
	s.Equal(s.targetResponse, reply)
	return nil
}

func (s *mockClientConnInterface) NewStream(
	_ context.Context,
	_ *grpc.StreamDesc,
	_ string,
	_ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	panic("implement me")
}
