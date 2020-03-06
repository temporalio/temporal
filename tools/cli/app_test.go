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

package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"github.com/golang/mock/gomock"
	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal-proto/workflowservicemock"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
)

type cliAppSuite struct {
	suite.Suite
	app               *cli.App
	mockCtrl          *gomock.Controller
	frontendClient    *workflowservicemock.MockWorkflowServiceClient
	serverAdminClient *adminservicemock.MockAdminServiceClient
}

type clientFactoryMock struct {
	frontendClient    workflowservice.WorkflowServiceClient
	serverAdminClient adminservice.AdminServiceClient
}

func (m *clientFactoryMock) FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	return m.frontendClient
}

func (m *clientFactoryMock) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	return m.serverAdminClient
}

var commands = []string{
	"domain", "d",
	"workflow", "wf",
	"tasklist", "tl",
}

var domainName = "cli-test-domain"

func TestCLIAppSuite(t *testing.T) {
	s := new(cliAppSuite)
	suite.Run(t, s)
}

func (s *cliAppSuite) SetupSuite() {
	s.app = NewCliApp()
}

func (s *cliAppSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())

	s.frontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.mockCtrl)
	s.serverAdminClient = adminservicemock.NewMockAdminServiceClient(s.mockCtrl)
	SetFactory(&clientFactoryMock{
		frontendClient:    s.frontendClient,
		serverAdminClient: s.serverAdminClient,
	})
}

func (s *cliAppSuite) TearDownTest() {
	s.mockCtrl.Finish() // assert mock’s expectations
}

func (s *cliAppSuite) RunErrorExitCode(arguments []string) int {
	oldOsExit := osExit
	defer func() { osExit = oldOsExit }()
	var errorCode int
	osExit = func(code int) {
		errorCode = code
	}
	s.NoError(s.app.Run(arguments))
	return errorCode
}

func (s *cliAppSuite) TestAppCommands() {
	for _, test := range commands {
		cmd := s.app.Command(test)
		s.NotNil(cmd)
	}
}

func (s *cliAppSuite) TestDomainRegister_LocalDomain() {
	s.frontendClient.EXPECT().RegisterDomain(gomock.Any(), gomock.Any()).Return(nil, nil)
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "register", "--global_domain", "false"})
	s.Equal(0, errorCode)
}

func (s *cliAppSuite) TestDomainRegister_GlobalDomain() {
	s.frontendClient.EXPECT().RegisterDomain(gomock.Any(), gomock.Any()).Return(nil, nil)
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "register", "--global_domain", "true"})
	s.Equal(0, errorCode)
}

func (s *cliAppSuite) TestDomainRegister_DomainExist() {
	s.frontendClient.EXPECT().RegisterDomain(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.AlreadyExists, "").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "register", "--global_domain", "true"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestDomainRegister_Failed() {
	s.frontendClient.EXPECT().RegisterDomain(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "register", "--global_domain", "true"})
	s.Equal(1, errorCode)
}

var describeDomainResponseServer = &workflowservice.DescribeDomainResponse{
	DomainInfo: &commonproto.DomainInfo{
		Name:        "test-domain",
		Description: "a test domain",
		OwnerEmail:  "test@uber.com",
	},
	Configuration: &commonproto.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: 3,
		EmitMetric:                             &types.BoolValue{Value: true},
	},
	ReplicationConfiguration: &commonproto.DomainReplicationConfiguration{
		ActiveClusterName: "active",
		Clusters: []*commonproto.ClusterReplicationConfiguration{
			{
				ClusterName: "active",
			},
			{
				ClusterName: "standby",
			},
		},
	},
}

func (s *cliAppSuite) TestDomainUpdate() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	s.frontendClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "domain", "update"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "domain", "update", "--desc", "another desc", "--oe", "another@uber.com", "--rd", "1"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainUpdate_DomainNotExist() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, nil)
	s.frontendClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.NotFound, "").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "update"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestDomainUpdate_ActiveClusterFlagNotSet_DomainNotExist() {
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.NotFound, "").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "update"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestDomainUpdate_Failed() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, nil)
	s.frontendClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "update"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestDomainDescribe() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "domain", "describe"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainDescribe_DomainNotExist() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, status.New(codes.NotFound, "").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "describe"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestDomainDescribe_Failed() {
	resp := describeDomainResponseServer
	s.frontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(resp, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "domain", "describe"})
	s.Equal(1, errorCode)
}

var (
	eventType = enums.EventTypeWorkflowExecutionStarted

	getWorkflowExecutionHistoryResponse = &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &commonproto.History{
			Events: []*commonproto.HistoryEvent{
				{
					EventType: eventType,
					Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
						WorkflowType:                        &commonproto.WorkflowType{Name: "TestWorkflow"},
						TaskList:                            &commonproto.TaskList{Name: "taskList"},
						ExecutionStartToCloseTimeoutSeconds: 60,
						TaskStartToCloseTimeoutSeconds:      10,
						Identity:                            "tester",
					}},
				},
			},
		},
		NextPageToken: nil,
	}
)

func (s *cliAppSuite) TestShowHistory() {
	resp := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistoryWithID() {
	resp := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "showid", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistory_PrintRawTime() {
	resp := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid", "-prt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistory_PrintDateTime() {
	resp := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid", "-pdt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestStartWorkflow() {
	resp := &workflowservice.StartWorkflowExecutionResponse{RunId: uuid.New()}
	s.frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	// start with wid
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "start", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid", "wrp", "2"})
	s.Nil(err)
	// start without wid
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "start", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "wrp", "2"})
	s.Nil(err)
}

func (s *cliAppSuite) TestStartWorkflow_Failed() {
	resp := &workflowservice.StartWorkflowExecutionResponse{RunId: uuid.New()}
	s.frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, status.New(codes.InvalidArgument, "faked error").Err())
	// start with wid
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "start", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestRunWorkflow() {
	resp := &workflowservice.StartWorkflowExecutionResponse{RunId: uuid.New()}
	history := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(history, nil).Times(2)
	// start with wid
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "run", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid", "wrp", "2"})
	s.Nil(err)
	// start without wid
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "run", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "wrp", "2"})
	s.Nil(err)
}

func (s *cliAppSuite) TestRunWorkflow_Failed() {
	resp := &workflowservice.StartWorkflowExecutionResponse{RunId: uuid.New()}
	history := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, status.New(codes.InvalidArgument, "faked error").Err())
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(history, nil)
	// start with wid
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "run", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestTerminateWorkflow() {
	s.frontendClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "terminate", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestTerminateWorkflow_Failed() {
	s.frontendClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "terminate", "-w", "wid"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestCancelWorkflow() {
	s.frontendClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "cancel", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestCancelWorkflow_Failed() {
	s.frontendClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "cancel", "-w", "wid"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestSignalWorkflow() {
	s.frontendClient.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "signal", "-w", "wid", "-n", "signal-name"})
	s.Nil(err)
}

func (s *cliAppSuite) TestSignalWorkflow_Failed() {
	s.frontendClient.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "signal", "-w", "wid", "-n", "signal-name"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestQueryWorkflow() {
	resp := &workflowservice.QueryWorkflowResponse{
		QueryResult: []byte("query-result"),
	}
	s.frontendClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "query", "-w", "wid", "-qt", "query-type-test"})
	s.Nil(err)
}

func (s *cliAppSuite) TestQueryWorkflowUsingStackTrace() {
	resp := &workflowservice.QueryWorkflowResponse{
		QueryResult: []byte("query-result"),
	}
	s.frontendClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "stack", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestQueryWorkflow_Failed() {
	resp := &workflowservice.QueryWorkflowResponse{
		QueryResult: []byte("query-result"),
	}
	s.frontendClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "workflow", "query", "-w", "wid", "-qt", "query-type-test"})
	s.Equal(1, errorCode)
}

var (
	closeStatus = enums.WorkflowExecutionCloseStatusCompleted

	listClosedWorkflowExecutionsResponse = &workflowservice.ListClosedWorkflowExecutionsResponse{
		Executions: []*commonproto.WorkflowExecutionInfo{
			{
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: "test-list-workflow-id",
					RunId:      uuid.New(),
				},
				Type: &commonproto.WorkflowType{
					Name: "test-list-workflow-type",
				},
				StartTime:     time.Now().UnixNano(),
				CloseTime:     time.Now().Add(time.Hour).UnixNano(),
				CloseStatus:   closeStatus,
				HistoryLength: 12,
			},
		},
	}

	listOpenWorkflowExecutionsResponse = &workflowservice.ListOpenWorkflowExecutionsResponse{
		Executions: []*commonproto.WorkflowExecutionInfo{
			{
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: "test-list-open-workflow-id",
					RunId:      uuid.New(),
				},
				Type: &commonproto.WorkflowType{
					Name: "test-list-open-workflow-type",
				},
				StartTime:     time.Now().UnixNano(),
				CloseTime:     time.Now().Add(time.Hour).UnixNano(),
				HistoryLength: 12,
			},
		},
	}
)

func (s *cliAppSuite) TestListWorkflow() {
	resp := listClosedWorkflowExecutionsResponse
	s.frontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_WithWorkflowID() {
	resp := &workflowservice.ListClosedWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-wid", "nothing"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_WithWorkflowType() {
	resp := &workflowservice.ListClosedWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-wt", "no-type"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_PrintDateTime() {
	resp := listClosedWorkflowExecutionsResponse
	s.frontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-pdt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_PrintRawTime() {
	resp := listClosedWorkflowExecutionsResponse
	s.frontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-prt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open() {
	resp := listOpenWorkflowExecutionsResponse
	s.frontendClient.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open_WithWorkflowID() {
	resp := &workflowservice.ListOpenWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op", "-wid", "nothing"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open_WithWorkflowType() {
	resp := &workflowservice.ListOpenWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op", "-wt", "no-type"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListArchivedWorkflow() {
	resp := &workflowservice.ListArchivedWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().ListArchivedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "listarchived", "-q", "some query string", "--ps", "200", "--all"})
	s.Nil(err)
}

func (s *cliAppSuite) TestCountWorkflow() {
	resp := &workflowservice.CountWorkflowExecutionsResponse{}
	s.frontendClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "count"})
	s.Nil(err)

	s.frontendClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(resp, nil)
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "count", "-q", "'CloseTime = missing'"})
	s.Nil(err)
}

var describeTaskListResponse = &workflowservice.DescribeTaskListResponse{
	Pollers: []*commonproto.PollerInfo{
		{
			LastAccessTime: time.Now().UnixNano(),
			Identity:       "tester",
		},
	},
}

func (s *cliAppSuite) TestAdminDescribeWorkflow() {
	resp := &adminservice.DescribeWorkflowExecutionResponse{
		ShardId:                "test-shard-id",
		HistoryAddr:            "ip:port",
		MutableStateInDatabase: `{"ExecutionInfo":{"BranchToken":"ChBNWvyipehOuYvioA1u+suwEhDyawZ9XsdN6Liiof+Novu5"}}`,
	}

	s.serverAdminClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "admin", "wf", "describe", "-w", "test-wf-id"})
	s.Nil(err)
}

func (s *cliAppSuite) TestAdminDescribeWorkflow_Failed() {
	s.serverAdminClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, status.New(codes.InvalidArgument, "faked error").Err())
	errorCode := s.RunErrorExitCode([]string{"", "--do", domainName, "admin", "wf", "describe", "-w", "test-wf-id"})
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestAdminAddSearchAttribute() {
	err := s.app.Run([]string{"", "--do", domainName, "admin", "cl", "asa", "--search_attr_key", "testKey", "--search_attr_type", "1"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDescribeTaskList() {
	resp := describeTaskListResponse
	s.frontendClient.EXPECT().DescribeTaskList(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "tasklist", "describe", "-tl", "test-taskList"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDescribeTaskList_Activity() {
	resp := describeTaskListResponse
	s.frontendClient.EXPECT().DescribeTaskList(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "tasklist", "describe", "-tl", "test-taskList", "-tlt", "activity"})
	s.Nil(err)
}

func (s *cliAppSuite) TestObserveWorkflow() {
	history := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(history, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "observe", "-w", "wid"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "observe", "-w", "wid", "-sd"})
	s.Nil(err)
}

func (s *cliAppSuite) TestObserveWorkflowWithID() {
	history := getWorkflowExecutionHistoryResponse
	s.frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).Return(history, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "observeid", "wid"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "observeid", "wid", "-sd"})
	s.Nil(err)
}

// TestParseTime tests the parsing of date argument in UTC and UnixNano formats
func (s *cliAppSuite) TestParseTime() {
	s.Equal(int64(100), parseTime("", 100, time.Now()))
	s.Equal(int64(1528383845000000000), parseTime("2018-06-07T15:04:05+00:00", 0, time.Now()))
	s.Equal(int64(1528383845000000000), parseTime("1528383845000000000", 0, time.Now()))
}

// TestParseTimeDateRange tests the parsing of date argument in time range format, N<duration>
// where N is the integral multiplier, and duration can be second/minute/hour/day/week/month/year
func (s *cliAppSuite) TestParseTimeDateRange() {
	now := time.Now()
	tests := []struct {
		timeStr  string // input
		defVal   int64  // input
		expected int64  // expected unix nano (approx)
	}{
		{
			timeStr:  "1s",
			defVal:   int64(0),
			expected: now.Add(-time.Second).UnixNano(),
		},
		{
			timeStr:  "100second",
			defVal:   int64(0),
			expected: now.Add(-100 * time.Second).UnixNano(),
		},
		{
			timeStr:  "2m",
			defVal:   int64(0),
			expected: now.Add(-2 * time.Minute).UnixNano(),
		},
		{
			timeStr:  "200minute",
			defVal:   int64(0),
			expected: now.Add(-200 * time.Minute).UnixNano(),
		},
		{
			timeStr:  "3h",
			defVal:   int64(0),
			expected: now.Add(-3 * time.Hour).UnixNano(),
		},
		{
			timeStr:  "1000hour",
			defVal:   int64(0),
			expected: now.Add(-1000 * time.Hour).UnixNano(),
		},
		{
			timeStr:  "5d",
			defVal:   int64(0),
			expected: now.Add(-5 * day).UnixNano(),
		},
		{
			timeStr:  "25day",
			defVal:   int64(0),
			expected: now.Add(-25 * day).UnixNano(),
		},
		{
			timeStr:  "5w",
			defVal:   int64(0),
			expected: now.Add(-5 * week).UnixNano(),
		},
		{
			timeStr:  "52week",
			defVal:   int64(0),
			expected: now.Add(-52 * week).UnixNano(),
		},
		{
			timeStr:  "3M",
			defVal:   int64(0),
			expected: now.Add(-3 * month).UnixNano(),
		},
		{
			timeStr:  "6month",
			defVal:   int64(0),
			expected: now.Add(-6 * month).UnixNano(),
		},
		{
			timeStr:  "1y",
			defVal:   int64(0),
			expected: now.Add(-year).UnixNano(),
		},
		{
			timeStr:  "7year",
			defVal:   int64(0),
			expected: now.Add(-7 * year).UnixNano(),
		},
		{
			timeStr:  "100y", // epoch time will be returned as that's the minimum unix timestamp possible
			defVal:   int64(0),
			expected: time.Unix(0, 0).UnixNano(),
		},
	}
	const delta = int64(5 * time.Millisecond)
	for _, te := range tests {
		parsedTime := parseTime(te.timeStr, te.defVal, now)
		s.True(te.expected <= parsedTime, "Case: %s. %d must be less or equal than parsed %d", te.timeStr, te.expected, parsedTime)
		s.True(te.expected+delta >= parsedTime, "Case: %s. %d must be greater or equal than parsed %d", te.timeStr, te.expected, parsedTime)
	}
}

func (s *cliAppSuite) TestBreakLongWords() {
	s.Equal("111 222 333 4", breakLongWords("1112223334", 3))
	s.Equal("111 2 223", breakLongWords("1112 223", 3))
	s.Equal("11 122 23", breakLongWords("11 12223", 3))
	s.Equal("111", breakLongWords("111", 3))
	s.Equal("", breakLongWords("", 3))
	s.Equal("111  222", breakLongWords("111 222", 3))
}

func (s *cliAppSuite) TestAnyToString() {
	arg := strings.Repeat("LongText", 80)
	event := &commonproto.HistoryEvent{
		EventId:   1,
		EventType: eventType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &commonproto.WorkflowType{Name: "code.uber.internal/devexp/cadence-samples.git/cmd/samples/recipes/helloworld.Workflow"},
			TaskList:                            &commonproto.TaskList{Name: "taskList"},
			ExecutionStartToCloseTimeoutSeconds: 60,
			TaskStartToCloseTimeoutSeconds:      10,
			Identity:                            "tester",
			Input:                               []byte(arg),
		}},
	}
	res := anyToString(event, false, defaultMaxFieldLength)
	ss, l := tablewriter.WrapString(res, 10)
	s.Equal(6, len(ss))
	s.Equal(131, l)
}

func (s *cliAppSuite) TestAnyToString_DecodeMapValues() {
	fields := map[string][]byte{
		"TestKey": []byte("testValue"),
	}
	execution := &commonproto.WorkflowExecutionInfo{
		Memo: &commonproto.Memo{Fields: fields},
	}
	s.Equal("{StartTime:0, CloseTime:0, CloseStatus:WorkflowExecutionCloseStatusRunning, HistoryLength:0, ExecutionTime:0, Memo:{Fields:map{TestKey:testValue}}}", anyToString(execution, true, 0))

	fields["TestKey2"] = []byte(`anotherTestValue`)
	execution.Memo = &commonproto.Memo{Fields: fields}
	got := anyToString(execution, true, 0)
	expected := got == "{StartTime:0, CloseTime:0, CloseStatus:WorkflowExecutionCloseStatusRunning, HistoryLength:0, ExecutionTime:0, Memo:{Fields:map{TestKey2:anotherTestValue, TestKey:testValue}}}" ||
		got == "{StartTime:0, CloseTime:0, CloseStatus:WorkflowExecutionCloseStatusRunning, HistoryLength:0, ExecutionTime:0, Memo:{Fields:map{TestKey:testValue, TestKey2:anotherTestValue}}}"
	s.True(expected)
}

func (s *cliAppSuite) TestIsAttributeName() {
	s.True(isAttributeName("WorkflowExecutionStartedEventAttributes"))
	s.False(isAttributeName("workflowExecutionStartedEventAttributes"))
}

func (s *cliAppSuite) TestGetWorkflowIdReusePolicy() {
	res := getWorkflowIDReusePolicy(2)
	s.Equal(res.String(), enums.WorkflowIdReusePolicyRejectDuplicate.String())
}

func (s *cliAppSuite) TestGetWorkflowIdReusePolicy_Failed_ExceedRange() {
	oldOsExit := osExit
	defer func() { osExit = oldOsExit }()
	var errorCode int
	osExit = func(code int) {
		errorCode = code
	}
	getWorkflowIDReusePolicy(2147483647)
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestGetWorkflowIdReusePolicy_Failed_Negative() {
	oldOsExit := osExit
	defer func() { osExit = oldOsExit }()
	var errorCode int
	osExit = func(code int) {
		errorCode = code
	}
	getWorkflowIDReusePolicy(-1)
	s.Equal(1, errorCode)
}

func (s *cliAppSuite) TestGetSearchAttributes() {
	resp := &workflowservice.GetSearchAttributesResponse{}
	s.frontendClient.EXPECT().GetSearchAttributes(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	err := s.app.Run([]string{"", "cluster", "get-search-attr"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "cluster", "get-search-attr"})
	s.Nil(err)
}

func (s *cliAppSuite) TestParseBool() {
	res, err := parseBool("true")
	s.NoError(err)
	s.True(res)

	res, err = parseBool("false")
	s.NoError(err)
	s.False(res)

	for _, v := range []string{"True, TRUE, False, FALSE, T, F"} {
		res, err = parseBool(v)
		s.Error(err)
		s.False(res)
	}
}

func (s *cliAppSuite) TestConvertStringToRealType() {
	var res interface{}

	// int
	res = convertStringToRealType("1")
	s.Equal(int64(1), res)

	// bool
	res = convertStringToRealType("true")
	s.Equal(true, res)
	res = convertStringToRealType("false")
	s.Equal(false, res)

	// double
	res = convertStringToRealType("1.0")
	s.Equal(float64(1.0), res)

	// datetime
	res = convertStringToRealType("2019-01-01T01:01:01Z")
	s.Equal(time.Date(2019, 1, 1, 1, 1, 1, 0, time.UTC), res)

	// array
	res = convertStringToRealType(`["a", "b", "c"]`)
	s.Equal([]interface{}{"a", "b", "c"}, res)

	// string
	res = convertStringToRealType("test string")
	s.Equal("test string", res)
}

func (s *cliAppSuite) TestConvertArray() {
	t1, _ := time.Parse(defaultDateTimeFormat, "2019-06-07T16:16:34-08:00")
	t2, _ := time.Parse(defaultDateTimeFormat, "2019-06-07T17:16:34-08:00")
	testCases := []struct {
		name     string
		input    string
		expected interface{}
	}{
		{
			name:     "string",
			input:    `["a", "b", "c"]`,
			expected: []interface{}{"a", "b", "c"},
		},
		{
			name:     "int",
			input:    `[1, 2, 3]`,
			expected: []interface{}{"1", "2", "3"},
		},
		{
			name:     "double",
			input:    `[1.1, 2.2, 3.3]`,
			expected: []interface{}{"1.1", "2.2", "3.3"},
		},
		{
			name:     "bool",
			input:    `["true", "false"]`,
			expected: []interface{}{"true", "false"},
		},
		{
			name:     "datetime",
			input:    `["2019-06-07T16:16:34-08:00", "2019-06-07T17:16:34-08:00"]`,
			expected: []interface{}{t1, t2},
		},
	}
	for _, testCase := range testCases {
		res, err := parseArray(testCase.input)
		s.Nil(err)
		s.Equal(testCase.expected, res)
	}

	testCases2 := []struct {
		name     string
		input    string
		expected error
	}{
		{
			name:  "not array",
			input: "normal string",
		},
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "not json array",
			input: "[a, b, c]",
		},
	}
	for _, testCase := range testCases2 {
		res, err := parseArray(testCase.input)
		s.NotNil(err)
		s.Nil(res)
	}
}
