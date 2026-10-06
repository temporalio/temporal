package tdbg

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/codec"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type (
	testClient struct {
		adminservice.AdminServiceClient
		describeTaskQueuePartitionFn    func(request *adminservice.DescribeTaskQueuePartitionRequest) (*adminservice.DescribeTaskQueuePartitionResponse, error)
		forceUnloadTaskQueuePartitionFn func(request *adminservice.ForceUnloadTaskQueuePartitionRequest) (*adminservice.ForceUnloadTaskQueuePartitionResponse, error)
		getTaskQueueUserDataFn          func(request *adminservice.GetTaskQueueUserDataRequest) (*adminservice.GetTaskQueueUserDataResponse, error)
		updateTaskQueueUserDataFn       func(request *adminservice.UpdateTaskQueueUserDataRequest) (*adminservice.UpdateTaskQueueUserDataResponse, error)
	}
)

type AdminTests struct {
	Name       string
	inputFlags []string
	err        error
}

// common test cases
var testCases = []AdminTests{
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
		Name:       "task queue type: nexus",
		inputFlags: []string{"--task-queue-type", "TASK_QUEUE_TYPE_NEXUS"},
		err:        nil,
	},
	{
		Name:       "task queue type: invalid",
		inputFlags: []string{"--task-queue-type", "false"},
		err:        errors.New("invalid task queue type"), // nolint
	},
	{
		Name:       "task queue type: unspecified",
		inputFlags: []string{"--task-queue-type", "TASK_QUEUE_TYPE_UNSPECIFIED"},
		err:        errors.New("invalid task queue type"), // nolint
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
	}}

func (t *testClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t
}

func (t *testClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("unimplemented")
}

func (t *testClient) DescribeTaskQueuePartition(_ context.Context, request *adminservice.DescribeTaskQueuePartitionRequest, opts ...grpc.CallOption) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
	return t.describeTaskQueuePartitionFn(request)
}

func (t *testClient) ForceUnloadTaskQueuePartition(_ context.Context, request *adminservice.ForceUnloadTaskQueuePartitionRequest, opts ...grpc.CallOption) (*adminservice.ForceUnloadTaskQueuePartitionResponse, error) {
	return t.forceUnloadTaskQueuePartitionFn(request)
}

func (t *testClient) GetTaskQueueUserData(_ context.Context, request *adminservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*adminservice.GetTaskQueueUserDataResponse, error) {
	return t.getTaskQueueUserDataFn(request)
}

func (t *testClient) UpdateTaskQueueUserData(_ context.Context, request *adminservice.UpdateTaskQueueUserDataRequest, opts ...grpc.CallOption) (*adminservice.UpdateTaskQueueUserDataResponse, error) {
	return t.updateTaskQueueUserDataFn(request)
}

func (s *taskQueueCommandTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	// injecting a test admin client
	client := &testClient{
		describeTaskQueuePartitionFn: func(request *adminservice.DescribeTaskQueuePartitionRequest) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
			return &adminservice.DescribeTaskQueuePartitionResponse{}, nil
		},
		forceUnloadTaskQueuePartitionFn: func(request *adminservice.ForceUnloadTaskQueuePartitionRequest) (*adminservice.ForceUnloadTaskQueuePartitionResponse, error) {
			return &adminservice.ForceUnloadTaskQueuePartitionResponse{}, nil
		},
		getTaskQueueUserDataFn: func(request *adminservice.GetTaskQueueUserDataRequest) (*adminservice.GetTaskQueueUserDataResponse, error) {
			return &adminservice.GetTaskQueueUserDataResponse{}, nil
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
	describeTQPartitionTests := make([]AdminTests, len(testCases))
	copy(describeTQPartitionTests, testCases) // creating local copy before appending new test cases

	// describe-tq-partition specific test cases
	additionalTestCases := []AdminTests{
		{
			Name:       "multiple buildId's",
			inputFlags: []string{"--select-build-id", "['1', '2']"},
			err:        nil,
		},
		{
			Name:       "unversioned: false",
			inputFlags: []string{"--select-unversioned", "false"},
			err:        nil,
		},
		{
			Name:       "allActive: false",
			inputFlags: []string{"--select-all-active", "false"},
			err:        nil,
		},
	}
	describeTQPartitionTests = append(describeTQPartitionTests, additionalTestCases...)

	baseCommand := []string{"tdbg", "taskqueue", "describe-task-queue-partition",
		"--task-queue", "test"}

	for _, test := range describeTQPartitionTests {
		cliCommand := append(baseCommand, test.inputFlags...)
		resp := s.app.Run(cliCommand)
		if resp != nil {
			s.ErrorContainsf(resp, test.err.Error(), "error present")
		}
	}
}

// TestForceUnloadTaskQueuePartitionWithArgs tests that the cli accepts the various arguments for
// --force-unload-task-queue-partition
func (s *taskQueueCommandTestSuite) TestForceUnloadTaskQueuePartition() {
	baseCommand := []string{"tdbg", "taskqueue", "force-unload-task-queue-partition",
		"--task-queue", "test"}

	for _, test := range testCases {
		cliCommand := append(baseCommand, test.inputFlags...)
		resp := s.app.Run(cliCommand)
		if resp != nil {
			s.ErrorContainsf(resp, test.err.Error(), "error present")
		}
	}
}

// TestGetTaskQueueUserData tests that the cli accepts the various arguments for get-user-data.
func (s *taskQueueCommandTestSuite) TestGetTaskQueueUserData() {
	baseCommand := []string{"tdbg", "taskqueue", "get-user-data",
		"--namespace", "default", "--task-queue", "test"}

	// Run shared test cases, skipping sticky-name (not a registered flag on this command)
	// and unspecified type (this command defaults to workflow instead of erroring).
	for _, test := range testCases {
		if len(test.inputFlags) > 0 && (test.inputFlags[0] == "--sticky-name" ||
			test.inputFlags[1] == "TASK_QUEUE_TYPE_UNSPECIFIED") {
			continue
		}
		cliCommand := append(baseCommand, test.inputFlags...)
		resp := s.app.Run(cliCommand)
		if test.err != nil {
			s.ErrorContainsf(resp, test.err.Error(), "error present")
		} else {
			s.NoError(resp)
		}
	}

	// TASK_QUEUE_TYPE_UNSPECIFIED defaults to WORKFLOW (no error).
	s.NoError(s.app.Run([]string{"tdbg", "taskqueue", "get-user-data",
		"--namespace", "default", "--task-queue", "test",
		"--task-queue-type", "TASK_QUEUE_TYPE_UNSPECIFIED"}))

	// Missing --task-queue is enforced by cli/v2 (Required: true) before the action runs.
	s.Error(s.app.Run([]string{"tdbg", "taskqueue", "get-user-data", "--namespace", "default"}))

	// Missing --namespace is enforced by cli/v2 (Required: true) before the action runs.
	s.Error(s.app.Run([]string{"tdbg", "taskqueue", "get-user-data", "--task-queue", "test"}))

	// No --task-queue-type or --partition-id: both use their defaults and succeed.
	s.NoError(s.app.Run([]string{"tdbg", "taskqueue", "get-user-data",
		"--namespace", "default", "--task-queue", "test"}))

	// Matching client returns an error: CLI wraps and returns it.
	errorClient := &testClient{
		getTaskQueueUserDataFn: func(request *adminservice.GetTaskQueueUserDataRequest) (*adminservice.GetTaskQueueUserDataResponse, error) {
			return nil, errors.New("matching unavailable")
		},
	}
	errorApp := NewCliApp(func(params *Params) { params.ClientFactory = errorClient })
	errorApp.ExitErrHandler = func(context *cli.Context, err error) {}
	resp := errorApp.Run([]string{"tdbg", "taskqueue", "get-user-data",
		"--namespace", "default", "--task-queue", "test"})
	s.ErrorContains(resp, "unable to get Task Queue User Data")
}

func TestUpdateTaskQueueUserData(t *testing.T) {
	dir := t.TempDir()
	validFile := filepath.Join(dir, "valid.json")
	require.NoError(t, os.WriteFile(validFile, []byte(`{"config": {"queueRateLimit": {"rateLimit": {"requestsPerSecond": 10}}}, "fairnessState": "FAIRNESS_STATE_V2"}`), 0o600))
	invalidFile := filepath.Join(dir, "invalid.json")
	require.NoError(t, os.WriteFile(invalidFile, []byte(`{"config": `), 0o600))

	baseArgs := func(extra ...string) []string {
		return append([]string{"tdbg", "--yes", "taskqueue", "update-user-data", "--namespace", "default", "--task-queue", "test"}, extra...)
	}

	tests := []struct {
		name      string
		args      []string
		rpcErr    error
		expectErr string
		verify    func(t *testing.T, req *adminservice.UpdateTaskQueueUserDataRequest)
	}{
		{
			name: "success with default type",
			args: baseArgs("--input-filename", validFile, "--known-version", "7"),
			verify: func(t *testing.T, req *adminservice.UpdateTaskQueueUserDataRequest) {
				require.Equal(t, "default", req.GetNamespace())
				require.Equal(t, "test", req.GetTaskQueue())
				require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, req.GetTaskQueueType())
				require.Equal(t, int64(7), req.GetKnownVersion())
				require.InDelta(t, 10, req.GetUserData().GetConfig().GetQueueRateLimit().GetRateLimit().GetRequestsPerSecond(), 0.001)
				require.Equal(t, enumsspb.FAIRNESS_STATE_V2, req.GetUserData().GetFairnessState())
			},
		},
		{
			name: "success with activity type",
			args: baseArgs("--input-filename", validFile, "--known-version", "7", "--task-queue-type", "TASK_QUEUE_TYPE_ACTIVITY"),
			verify: func(t *testing.T, req *adminservice.UpdateTaskQueueUserDataRequest) {
				require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, req.GetTaskQueueType())
			},
		},
		{
			name:      "invalid task queue type",
			args:      baseArgs("--input-filename", validFile, "--known-version", "7", "--task-queue-type", "bogus"),
			expectErr: "invalid task queue type",
		},
		{
			name:      "missing input file flag",
			args:      baseArgs("--known-version", "7"),
			expectErr: FlagInputFilename,
		},
		{
			name:      "missing known version flag",
			args:      baseArgs("--input-filename", validFile),
			expectErr: FlagKnownVersion,
		},
		{
			name:      "non-positive known version",
			args:      baseArgs("--input-filename", validFile, "--known-version", "0"),
			expectErr: "must be a positive version",
		},
		{
			name:      "unreadable input file",
			args:      baseArgs("--input-filename", filepath.Join(dir, "missing.json"), "--known-version", "7"),
			expectErr: "unable to read input file",
		},
		{
			name:      "invalid json",
			args:      baseArgs("--input-filename", invalidFile, "--known-version", "7"),
			expectErr: "unable to parse user data",
		},
		{
			name:      "rpc error",
			args:      baseArgs("--input-filename", validFile, "--known-version", "7"),
			rpcErr:    errors.New("user data version mismatch"),
			expectErr: "unable to update Task Queue User Data",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotReq *adminservice.UpdateTaskQueueUserDataRequest
			client := &testClient{
				updateTaskQueueUserDataFn: func(request *adminservice.UpdateTaskQueueUserDataRequest) (*adminservice.UpdateTaskQueueUserDataResponse, error) {
					gotReq = request
					if tc.rpcErr != nil {
						return nil, tc.rpcErr
					}
					return &adminservice.UpdateTaskQueueUserDataResponse{Version: 8}, nil
				},
			}
			var stdout bytes.Buffer
			app := NewCliApp(func(params *Params) {
				params.ClientFactory = client
				params.Writer = &stdout
			})
			app.ExitErrHandler = func(context *cli.Context, err error) {}

			err := app.Run(tc.args)
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, gotReq)
			tc.verify(t, gotReq)
			var resp adminservice.UpdateTaskQueueUserDataResponse
			require.NoError(t, codec.NewJSONPBEncoder().Decode(stdout.Bytes(), &resp))
			require.Equal(t, int64(8), resp.GetVersion())
		})
	}
}
