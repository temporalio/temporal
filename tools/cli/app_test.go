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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"

	"go.uber.org/cadence/.gen/go/admin"
	"go.uber.org/cadence/.gen/go/admin/adminserviceclient"
	"go.uber.org/cadence/.gen/go/admin/adminservicetest"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/cadence/workflowservicetest"
	"go.uber.org/cadence/.gen/go/shared"
)

type cliAppSuite struct {
	suite.Suite
	app          *cli.App
	mockCtrl     *gomock.Controller
	service      *workflowservicetest.MockClient
	adminService *adminservicetest.MockClient
}

type workflowClientBuilderMock struct {
	service      workflowserviceclient.Interface
	adminService adminserviceclient.Interface
}

func (mock *workflowClientBuilderMock) BuildServiceClient(c *cli.Context) (workflowserviceclient.Interface, error) {
	return mock.service, nil
}

func (mock *workflowClientBuilderMock) BuildAdminServiceClient(c *cli.Context) (adminserviceclient.Interface, error) {
	return mock.adminService, nil
}

// this is the mock for yarpcCallOptions, make sure length are the same
var callOptions = []interface{}{gomock.Any(), gomock.Any(), gomock.Any()}

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
	s.service = workflowservicetest.NewMockClient(s.mockCtrl)
	s.adminService = adminservicetest.NewMockClient(s.mockCtrl)
	SetBuilder(&workflowClientBuilderMock{service: s.service, adminService: s.adminService})
}

func (s *cliAppSuite) TearDownTest() {
	s.mockCtrl.Finish() // assert mock’s expectations
}

func (s *cliAppSuite) TestAppCommands() {
	for _, test := range commands {
		cmd := s.app.Command(test)
		s.NotNil(cmd)
	}
}

func (s *cliAppSuite) TestDomainRegister() {
	s.service.EXPECT().RegisterDomain(gomock.Any(), gomock.Any(), callOptions...).Return(nil)
	err := s.app.Run([]string{"", "--do", domainName, "domain", "register"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainRegister_DomainExist() {
	s.service.EXPECT().RegisterDomain(gomock.Any(), gomock.Any(), callOptions...).Return(&shared.DomainAlreadyExistsError{})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "register"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainRegister_Failed() {
	s.service.EXPECT().RegisterDomain(gomock.Any(), gomock.Any(), callOptions...).Return(&shared.BadRequestError{"fake error"})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "register"})
	s.Nil(err)
}

var describeDomainResponse = &shared.DescribeDomainResponse{
	DomainInfo: &shared.DomainInfo{
		Name:        common.StringPtr("test-domain"),
		Description: common.StringPtr("a test domain"),
		OwnerEmail:  common.StringPtr("test@uber.com"),
	},
	Configuration: &shared.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(3),
		EmitMetric:                             common.BoolPtr(true),
	},
	ReplicationConfiguration: &shared.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr("active"),
		Clusters: []*shared.ClusterReplicationConfiguration{
			&shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr("active"),
			},
			&shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr("standby"),
			},
		},
	},
}

func (s *cliAppSuite) TestDomainUpdate() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil).Times(2)
	s.service.EXPECT().UpdateDomain(gomock.Any(), gomock.Any(), callOptions...).Return(nil, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "domain", "update"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "domain", "update", "--desc", "another desc", "--oe", "another@uber.com", "--rd", "1", "--em", "f"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainUpdate_DomainNotExist() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	s.service.EXPECT().UpdateDomain(gomock.Any(), gomock.Any(), callOptions...).Return(nil, &shared.EntityNotExistsError{})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "update"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainUpdate_ActiveClusterFlagNotSet_DomainNotExist() {
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(nil, &shared.EntityNotExistsError{})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "update"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainUpdate_Failed() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	s.service.EXPECT().UpdateDomain(gomock.Any(), gomock.Any(), callOptions...).Return(nil, &shared.BadRequestError{"faked error"})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "update"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainDescribe() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "domain", "describe"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainDescribe_DomainNotExist() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, &shared.EntityNotExistsError{})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "describe"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDomainDescribe_Failed() {
	resp := describeDomainResponse
	s.service.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), callOptions...).Return(resp, &shared.BadRequestError{"faked error"})
	err := s.app.Run([]string{"", "--do", domainName, "domain", "describe"})
	s.Nil(err)
}

var (
	eventType = shared.EventTypeWorkflowExecutionStarted

	getWorkflowExecutionHistoryResponse = &shared.GetWorkflowExecutionHistoryResponse{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventType: &eventType,
					WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
						WorkflowType: &shared.WorkflowType{Name: common.StringPtr("TestWorkflow")},
						TaskList:     &shared.TaskList{Name: common.StringPtr("taskList")},
						ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(60),
						TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
						Identity:                            common.StringPtr("tester"),
					},
				},
			},
		},
		NextPageToken: nil,
	}
)

func (s *cliAppSuite) TestShowHistory() {
	resp := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistoryWithID() {
	resp := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "showid", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistory_PrintRawTime() {
	resp := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid", "-prt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestShowHistory_PrintDateTime() {
	resp := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "show", "-w", "wid", "-pdt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestStartWorkflow() {
	resp := &shared.StartWorkflowExecutionResponse{RunId: common.StringPtr(uuid.New())}
	s.service.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	// start with wid
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "start", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid"})
	s.Nil(err)
	// start without wid
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "start", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60"})
	s.Nil(err)
}

func (s *cliAppSuite) TestRunWorkflow() {
	resp := &shared.StartWorkflowExecutionResponse{RunId: common.StringPtr(uuid.New())}
	history := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(history, nil).Times(2)
	// start with wid
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "run", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60", "-w", "wid"})
	s.Nil(err)
	// start without wid
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "run", "-tl", "testTaskList", "-wt", "testWorkflowType", "-et", "60"})
	s.Nil(err)
}

func (s *cliAppSuite) TestTerminateWorkflow() {
	s.service.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any(), callOptions...).Return(nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "terminate", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestCancelWorkflow() {
	s.service.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any(), callOptions...).Return(nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "cancel", "-w", "wid"})
	s.Nil(err)
}

func (s *cliAppSuite) TestSignalWorkflow() {
	s.service.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "signal", "-w", "wid", "-n", "signal-name"})
	s.Nil(err)
}

func (s *cliAppSuite) TestQueryWorkflow() {
	resp := &shared.QueryWorkflowResponse{
		QueryResult: []byte("query-result"),
	}
	s.service.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "query", "-w", "wid", "-qt", "query-type-test"})
	s.Nil(err)
}

func (s *cliAppSuite) TestQueryWorkflowUsingStackTrace() {
	resp := &shared.QueryWorkflowResponse{
		QueryResult: []byte("query-result"),
	}
	s.service.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "stack", "-w", "wid"})
	s.Nil(err)
}

var (
	closeStatus = shared.WorkflowExecutionCloseStatusCompleted

	listClosedWorkflowExecutionsResponse = &shared.ListClosedWorkflowExecutionsResponse{
		Executions: []*shared.WorkflowExecutionInfo{
			&shared.WorkflowExecutionInfo{
				Execution: &shared.WorkflowExecution{
					WorkflowId: common.StringPtr("test-list-workflow-id"),
					RunId:      common.StringPtr(uuid.New()),
				},
				Type: &shared.WorkflowType{
					Name: common.StringPtr("test-list-workflow-type"),
				},
				StartTime:     common.Int64Ptr(time.Now().UnixNano()),
				CloseTime:     common.Int64Ptr(time.Now().Add(time.Hour).UnixNano()),
				CloseStatus:   &closeStatus,
				HistoryLength: common.Int64Ptr(12),
			},
		},
	}

	listOpenWorkflowExecutionsResponse = &shared.ListOpenWorkflowExecutionsResponse{
		Executions: []*shared.WorkflowExecutionInfo{
			&shared.WorkflowExecutionInfo{
				Execution: &shared.WorkflowExecution{
					WorkflowId: common.StringPtr("test-list-open-workflow-id"),
					RunId:      common.StringPtr(uuid.New()),
				},
				Type: &shared.WorkflowType{
					Name: common.StringPtr("test-list-open-workflow-type"),
				},
				StartTime:     common.Int64Ptr(time.Now().UnixNano()),
				CloseTime:     common.Int64Ptr(time.Now().Add(time.Hour).UnixNano()),
				HistoryLength: common.Int64Ptr(12),
			},
		},
	}
)

func (s *cliAppSuite) TestListWorkflow() {
	resp := listClosedWorkflowExecutionsResponse
	s.service.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_WithWorkflowID() {
	resp := &shared.ListClosedWorkflowExecutionsResponse{}
	s.service.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-wid", "nothing"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_WithWorkflowType() {
	resp := &shared.ListClosedWorkflowExecutionsResponse{}
	s.service.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-wt", "no-type"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_PrintDateTime() {
	resp := listClosedWorkflowExecutionsResponse
	s.service.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-pdt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_PrintRawTime() {
	resp := listClosedWorkflowExecutionsResponse
	s.service.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-prt"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open() {
	resp := listOpenWorkflowExecutionsResponse
	s.service.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open_WithWorkflowID() {
	resp := &shared.ListOpenWorkflowExecutionsResponse{}
	s.service.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op", "-wid", "nothing"})
	s.Nil(err)
}

func (s *cliAppSuite) TestListWorkflow_Open_WithWorkflowType() {
	resp := &shared.ListOpenWorkflowExecutionsResponse{}
	s.service.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "list", "-op", "-wt", "no-type"})
	s.Nil(err)
}

var describeTaskListResponse = &shared.DescribeTaskListResponse{
	Pollers: []*shared.PollerInfo{
		&shared.PollerInfo{
			LastAccessTime: common.Int64Ptr(time.Now().UnixNano()),
			Identity:       common.StringPtr("tester"),
		},
	},
}

func (s *cliAppSuite) TestAdminDescribeWorkflow() {
	resp := &admin.DescribeWorkflowExecutionResponse{
		ShardId:     common.StringPtr("test-shard-id"),
		HistoryAddr: common.StringPtr("ip:port"),
	}

	s.adminService.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "admin", "wf", "describe", "-w", "test-wf-id"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDescribeTaskList() {
	resp := describeTaskListResponse
	s.service.EXPECT().DescribeTaskList(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "tasklist", "describe", "-tl", "test-taskList"})
	s.Nil(err)
}

func (s *cliAppSuite) TestDescribeTaskList_Activity() {
	resp := describeTaskListResponse
	s.service.EXPECT().DescribeTaskList(gomock.Any(), gomock.Any(), callOptions...).Return(resp, nil)
	err := s.app.Run([]string{"", "--do", domainName, "tasklist", "describe", "-tl", "test-taskList", "-tlt", "activity"})
	s.Nil(err)
}

func (s *cliAppSuite) TestObserveWorkflow() {
	history := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(history, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "observe", "-w", "wid"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "observe", "-w", "wid", "-sd"})
	s.Nil(err)
}

func (s *cliAppSuite) TestObserveWorkflowWithID() {
	history := getWorkflowExecutionHistoryResponse
	s.service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), callOptions...).Return(history, nil).Times(2)
	err := s.app.Run([]string{"", "--do", domainName, "workflow", "observeid", "wid"})
	s.Nil(err)
	err = s.app.Run([]string{"", "--do", domainName, "workflow", "observeid", "wid", "-sd"})
	s.Nil(err)
}

func (s *cliAppSuite) TestParseTime() {
	s.Equal(int64(100), parseTime("", 100))
	s.Equal(int64(1528383845000000000), parseTime("2018-06-07T15:04:05+00:00", 0))
	s.Equal(int64(1528383845000000000), parseTime("1528383845000000000", 0))
}
