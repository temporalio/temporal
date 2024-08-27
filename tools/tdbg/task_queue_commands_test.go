package tdbg

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"testing"
)

type (
	testClient struct {
		adminservice.AdminServiceClient
		describeTaskQueuePartitionFn func(request *adminservice.DescribeTaskQueuePartitionRequest) (*adminservice.DescribeTaskQueuePartitionResponse, error)
	}
)

func (t *testClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t
}

func (t *testClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("unimplemented")
}

func (t *testClient) DescribeTaskQueuePartition(_ context.Context, request *adminservice.DescribeTaskQueuePartitionRequest, opts ...grpc.CallOption) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
	return t.describeTaskQueuePartitionFn(request)
}

func (s *taskQueueCommandTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	// injecting a test admin client
	client := &testClient{
		describeTaskQueuePartitionFn: func(request *adminservice.DescribeTaskQueuePartitionRequest) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
			return &adminservice.DescribeTaskQueuePartitionResponse{}, nil
		},
	}
	s.app = NewCliApp(func(params *Params) {
		params.ClientFactory = client
	})
	s.app.ExitErrHandler = func(context *cli.Context, err error) {}
}
func TestTaskQueueCommandSuite(t *testing.T) {
	suite.Run(t, new(taskQueueCommandTestSuite))
}

type taskQueueCommandTestSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
	app        *cli.App
}

// TestDescribeTaskQueuePartitionWithArgs tests that the cli accepts the various arguments for
// --describe-task-queue-partition
func (s *taskQueueCommandTestSuite) TestDescribeTaskQueuePartition() {
	type describeTQPartitionTest struct {
		Name       string
		inputFlags []string
		err        error
	}

	// test cases with different input flags
	testCases := []describeTQPartitionTest{
		{
			Name:       "task queue type: workflow",
			inputFlags: []string{"--task-queue-type", "TASK_QUEUE_TYPE_WORKFLOW"},
			err:        nil,
		},
		{
			Name:       "task queue type: activity",
			inputFlags: []string{"--task-queue-type", "TASK_QUEUE_TYPE_ACTIVITY"},
			err:        nil,
		},
		{
			Name:       "task queue type: invalid",
			inputFlags: []string{"--task-queue-type", "false"},
			err:        errors.New("invalid task queue type"),
		},
		{
			Name:       "task queue type: unspecified",
			inputFlags: []string{"--task-queue-type", "TASK_QUEUE_TYPE_UNSPECIFIED"},
			err:        errors.New("invalid task queue type"),
		},
		{
			Name:       "task queue partition ID",
			inputFlags: []string{"--partition-id", "1"},
			err:        nil,
		},
		{
			Name:       "sticky name",
			inputFlags: []string{"--sticky-name", "random"},
			err:        nil,
		},
		{
			Name:       "multiple buildId's",
			inputFlags: []string{"--build-ids", "['1', '2']"},
			err:        nil,
		},
		{
			Name:       "unversioned: false",
			inputFlags: []string{"--unversioned", "false"},
			err:        nil,
		},
		{
			Name:       "allActive: false",
			inputFlags: []string{"--all-active", "false"},
			err:        nil,
		},
	}
	baseCommand := []string{"tdbg", "taskqueue", "describe-task-queue-partition",
		"--task-queue", "test"}

	for _, test := range testCases {
		cliCommand := append(baseCommand, test.inputFlags...)
		resp := s.app.Run(cliCommand)
		if resp != nil {
			s.ErrorContainsf(resp, test.err.Error(), "error present")
		}
	}
}
